package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/xuri/excelize/v2"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

const batchStream = "fc:batch-outbox"

var batchEnqueue = redis.NewScript(`
for i=1,2 do local t=redis.call('TYPE',KEYS[i]).ok; local wanted=({'set','stream'})[i]; if t~='none' and t~=wanted then return redis.error_reply('WRONGTYPE batch key') end end
if redis.call('SISMEMBER',KEYS[1],ARGV[1])==1 then return 0 end
redis.call('SADD',KEYS[1],ARGV[1]); redis.call('XADD',KEYS[2],'*','batch',ARGV[2],'user',ARGV[1]); return 1`)

type BatchResult struct {
	ID        string `json:"id"`
	State     string `json:"state"`
	Total     int64  `json:"total"`
	Accepted  int64  `json:"accepted"`
	Rejected  int64  `json:"rejected"`
	Processed int64  `json:"processed"`
	Error     string `json:"error,omitempty"`
	Issued    int64  `json:"issued"`
	Failed    int64  `json:"failed"`
}

func (s *Service) Import(ctx context.Context, m int64, tID string, r io.Reader) (result BatchResult, err error) {
	t, e := s.Template(ctx, m, tID)
	if e != nil {
		return result, e
	}
	if t.Mode != "async" {
		return result, fmt.Errorf("batch requires async template")
	}
	file, e := excelize.OpenReader(r, excelize.Options{UnzipSizeLimit: 128 << 20, UnzipXMLSizeLimit: 8 << 20})
	if e != nil {
		return result, e
	}
	defer file.Close()
	sheet := file.GetSheetName(0)
	if sheet == "" {
		return result, fmt.Errorf("empty workbook")
	}
	rows, e := file.Rows(sheet)
	if e != nil {
		return result, e
	}
	defer rows.Close()
	result.ID = id()
	result.State = "importing"
	_, e = s.Store.DB.ExecContext(ctx, "INSERT INTO batches(id,merchant_id,template_id,state) VALUES(?,?,?,'importing')", result.ID, m, tID)
	if e != nil {
		return result, e
	}
	defer func() {
		updateCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result.State = "ready"
		if err != nil {
			result.State = "partial"
			result.Error = err.Error()
		}
		// Read cardinality to include an enqueue whose response was lost.
		result.Total = s.R.SCard(updateCtx, "fc:batch:"+result.ID).Val()
		_, e := s.Store.DB.ExecContext(updateCtx, "UPDATE batches SET state=?,total=?,error_text=? WHERE id=?", result.State, result.Total, result.Error, result.ID)
		if e != nil && err == nil {
			err = e
		}
	}()
	var line int
	for rows.Next() {
		line++
		if line > 1000001 {
			return result, fmt.Errorf("maximum 1000000 data rows")
		}
		cols, e := rows.Columns()
		if e != nil {
			return result, e
		}
		if len(cols) == 0 || strings.TrimSpace(cols[0]) == "" {
			continue
		}
		raw := strings.TrimSpace(cols[0])
		if line == 1 && raw == "user_id" {
			continue
		}
		u, e := strconv.ParseInt(raw, 10, 64)
		if e != nil || u <= 0 {
			return result, fmt.Errorf("row %d: user_id must be positive integer", line)
		}
		if _, e = batchEnqueue.Run(ctx, s.R, []string{"fc:batch:" + result.ID, batchStream}, u, result.ID).Result(); e != nil {
			return result, e
		}
	}
	return result, rows.Error()
}
func (s *Service) Batch(ctx context.Context, id string) (BatchResult, error) {
	var b BatchResult
	b.ID = id
	e := s.Store.DB.QueryRowContext(ctx, "SELECT state,total,COALESCE(error_text,'') FROM batches WHERE id=?", id).Scan(&b.State, &b.Total, &b.Error)
	if e != nil {
		return b, e
	}
	e = s.Store.DB.QueryRowContext(ctx, "SELECT COUNT(*),COALESCE(SUM(state='accepted'),0),COALESCE(SUM(state='rejected'),0) FROM batch_items WHERE batch_id=?", id).Scan(&b.Processed, &b.Accepted, &b.Rejected)
	if e != nil {
		return b, e
	}
	e = s.Store.DB.QueryRowContext(ctx, "SELECT COALESCE(SUM(g.state='issued'),0),COALESCE(SUM(g.state='failed'),0) FROM batch_items b LEFT JOIN grants g ON b.event_id=g.id WHERE b.batch_id=?", id).Scan(&b.Issued, &b.Failed)
	return b, e
}
func (s *Service) processBatch(ctx context.Context, msg redis.XMessage) error {
	bid := fmt.Sprint(msg.Values["batch"])
	u, e := strconv.ParseInt(fmt.Sprint(msg.Values["user"]), 10, 64)
	if e != nil {
		return e
	}
	var state string
	e = s.Store.DB.QueryRowContext(ctx, "SELECT state FROM batch_items WHERE batch_id=? AND user_id=?", bid, u).Scan(&state)
	if e == nil {
		return nil
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	var m int64
	var tid string
	if e = s.Store.DB.QueryRowContext(ctx, "SELECT merchant_id,template_id FROM batches WHERE id=?", bid).Scan(&m, &tid); e != nil {
		return e
	}
	ev, claimErr := s.Claim(ctx, m, u, tid, "batch:"+bid)
	e = claimErr
	reason := ""
	state = "accepted"
	if e != nil {
		var ce *ClaimError
		if errors.As(e, &ce) && ce.Code == 5 {
			return e
		} // Missing inventory is a recoverable infrastructure state, never a permanent batch rejection.
		if !errors.As(e, &ce) && !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		state = "rejected"
		reason = e.Error()
	}
	var event any
	if state == "accepted" {
		event = ev.ID
	}
	_, e = s.Store.DB.ExecContext(ctx, "INSERT INTO batch_items(batch_id,user_id,event_id,state,error_text) VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE user_id=user_id", bid, u, event, state, reason)
	return e
}
func (s *Service) batchWorker(ctx context.Context) {
	name := "batch-" + id()
	handle := func(msg redis.XMessage) {
		if e := s.processBatch(ctx, msg); e != nil {
			s.Metrics.Retry.WithLabelValues("batch").Inc()
			slog.Warn("batch retry", "id", msg.ID, "error", e)
			return
		}
		if err := s.R.XAck(ctx, batchStream, "batch", msg.ID).Err(); err != nil {
			s.Metrics.Retry.WithLabelValues("ack").Inc()
			slog.Warn("batch ack retry", "error", err)
		}
	}
	for ctx.Err() == nil {
		start := "0-0"
		for {
			msgs, next, e := s.R.XAutoClaim(ctx, &redis.XAutoClaimArgs{Stream: batchStream, Group: "batch", Consumer: name, MinIdle: 10 * time.Second, Start: start, Count: 100}).Result()
			if e != nil {
				break
			}
			for _, msg := range msgs {
				handle(msg)
			}
			if next == "0-0" {
				break
			}
			start = next
		}
		rows, e := s.R.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "batch", Consumer: name, Streams: []string{batchStream, ">"}, Count: 100, Block: time.Second}).Result()
		if e != nil && e != redis.Nil && ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
		for _, r := range rows {
			for _, msg := range r.Messages {
				handle(msg)
			}
		}
	}
}
