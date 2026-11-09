package app

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func formatInt(n int64) string { return strconv.FormatInt(n, 10) }

// Token: base64url(user_id:expiry).hex(HMAC-SHA256(payload)). The signing key stays on the trusted backend.
func SignToken(u, expiry int64, key string) string {
	p := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%d:%d", u, expiry)))
	h := hmac.New(sha256.New, []byte(key))
	h.Write([]byte(p))
	return p + "." + hex.EncodeToString(h.Sum(nil))
}
func VerifyToken(token, key string) (int64, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid token")
	}
	h := hmac.New(sha256.New, []byte(key))
	h.Write([]byte(parts[0]))
	sig, e := hex.DecodeString(parts[1])
	if e != nil || !hmac.Equal(sig, h.Sum(nil)) {
		return 0, fmt.Errorf("invalid signature")
	}
	b, e := base64.RawURLEncoding.DecodeString(parts[0])
	if e != nil {
		return 0, e
	}
	fields := strings.Split(string(b), ":")
	if len(fields) != 2 {
		return 0, fmt.Errorf("invalid claims")
	}
	u, e := strconv.ParseInt(fields[0], 10, 64)
	if e != nil || u <= 0 {
		return 0, fmt.Errorf("invalid user")
	}
	exp, e := strconv.ParseInt(fields[1], 10, 64)
	if e != nil || exp <= time.Now().Unix() {
		return 0, fmt.Errorf("expired token")
	}
	return u, nil
}
func fail(c *gin.Context, e error) {
	status := http.StatusServiceUnavailable
	var ce *ClaimError
	if errors.Is(e, sql.ErrNoRows) {
		status = 404
	} else if errors.As(e, &ce) {
		status = 409
		if ce.Code == 5 {
			status = 503
		}
	}
	c.JSON(status, gin.H{"error": e.Error()})
}
func bad(c *gin.Context, msg string) { c.JSON(400, gin.H{"error": msg}) }
func positive(raw string) (int64, error) {
	v, e := strconv.ParseInt(raw, 10, 64)
	if e != nil || v <= 0 {
		return 0, fmt.Errorf("positive integer required")
	}
	return v, nil
}
func (s *Service) Router() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(s.Metrics.middleware())
	r.Use(gin.Recovery())
	r.Use(func(c *gin.Context) { c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 20<<20); c.Next() })
	r.GET("/health/live", func(c *gin.Context) { c.JSON(200, gin.H{"status": "ok"}) })
	r.GET("/metrics", s.metricsHandler())
	r.GET("/health/ready", func(c *gin.Context) {
		if e := s.Store.DB.PingContext(c.Request.Context()); e != nil {
			fail(c, e)
			return
		}
		if e := s.Store.Coupons.PingContext(c.Request.Context()); e != nil {
			fail(c, e)
			return
		}
		if e := s.R.Ping(c.Request.Context()).Err(); e != nil {
			fail(c, e)
			return
		}
		c.JSON(200, gin.H{"status": "ready"})
	})
	if s.Config.Role != "api" && s.Config.Role != "all" {
		return r
	}
	admin := r.Group("/admin", func(c *gin.Context) {
		if subtle.ConstantTimeCompare([]byte(c.GetHeader("X-Admin-Key")), []byte(s.Config.AdminKey)) != 1 {
			c.AbortWithStatus(401)
			return
		}
		c.Next()
	})
	admin.POST("/templates", func(c *gin.Context) {
		var t Template
		if e := c.ShouldBindJSON(&t); e != nil {
			bad(c, e.Error())
			return
		}
		if e := t.Validate(); e != nil {
			bad(c, e.Error())
			return
		}
		t, e := s.Create(c.Request.Context(), t)
		if e != nil {
			fail(c, e)
			return
		}
		c.JSON(201, t)
	})
	admin.GET("/templates", func(c *gin.Context) {
		ts, e := s.Store.Templates(c.Request.Context())
		if e != nil {
			fail(c, e)
			return
		}
		c.JSON(200, ts)
	})
	admin.POST("/templates/:merchant/:id/warm", func(c *gin.Context) {
		m, e := positive(c.Param("merchant"))
		if e != nil {
			bad(c, e.Error())
			return
		}
		t, e := s.Store.Template(c.Request.Context(), m, c.Param("id"))
		if e == nil {
			e = s.Warm(c.Request.Context(), t)
		}
		if e != nil {
			fail(c, e)
			return
		}
		c.JSON(200, gin.H{"status": "warmed"})
	})
	admin.GET("/audit", func(c *gin.Context) {
		a, e := s.ReconcileFor(c.Request.Context(), false, c.Query("merchant_id"), c.Query("template_id"))
		if e != nil {
			fail(c, e)
			return
		}
		c.JSON(200, a)
	})
	admin.POST("/templates/:merchant/:id/status", func(c *gin.Context) {
		m, e := positive(c.Param("merchant"))
		if e != nil {
			bad(c, e.Error())
			return
		}
		var req struct {
			Status string `json:"status"`
		}
		if e = c.ShouldBindJSON(&req); e != nil {
			bad(c, e.Error())
			return
		}
		if req.Status != "active" && req.Status != "paused" {
			bad(c, "status must be active or paused")
			return
		}
		if e = s.SetStatus(c.Request.Context(), m, c.Param("id"), req.Status); e != nil {
			fail(c, e)
			return
		}
		c.JSON(200, gin.H{"status": req.Status})
	})
	admin.POST("/reconcile", func(c *gin.Context) {
		a, e := s.Reconcile(c.Request.Context(), true)
		if e != nil {
			fail(c, e)
			return
		}
		c.JSON(200, a)
	})
	admin.POST("/batches/:merchant/:template", func(c *gin.Context) {
		m, e := positive(c.Param("merchant"))
		if e != nil {
			bad(c, e.Error())
			return
		}
		f, _, e := c.Request.FormFile("file")
		if e != nil {
			bad(c, e.Error())
			return
		}
		defer f.Close()
		b, e := s.Import(c.Request.Context(), m, c.Param("template"), f)
		if e != nil {
			c.JSON(400, gin.H{"error": e.Error(), "batch": b})
			return
		}
		c.JSON(202, b)
	})
	admin.GET("/batches/:id", func(c *gin.Context) {
		b, e := s.Batch(c.Request.Context(), c.Param("id"))
		if e != nil {
			fail(c, e)
			return
		}
		c.JSON(200, b)
	})
	api := r.Group("/v1", func(c *gin.Context) {
		token := strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer ")
		u, e := VerifyToken(token, s.Config.UserKey)
		if e != nil {
			c.AbortWithStatusJSON(401, gin.H{"error": e.Error()})
			return
		}
		c.Set("user", u)
		globalKey := "fc:global-rate:" + formatInt(time.Now().Unix())
		if n, err := limitScript.Run(c.Request.Context(), s.R, []string{globalKey}, s.Config.Rate).Int(); err != nil {
			fail(c, err)
			c.Abort()
			return
		} else if n == 0 {
			c.AbortWithStatusJSON(429, gin.H{"error": "global rate limit"})
			return
		}
		key := "fc:rate:" + formatInt(u) + ":" + formatInt(time.Now().Unix())
		n, e := limitScript.Run(c.Request.Context(), s.R, []string{key}, 100).Int()
		if e != nil {
			fail(c, e)
			c.Abort()
			return
		}
		if n == 0 {
			c.AbortWithStatusJSON(429, gin.H{"error": "rate limit"})
			return
		}
		c.Next()
	})
	api.GET("/templates/:merchant/:id", func(c *gin.Context) {
		m, e := positive(c.Param("merchant"))
		if e != nil {
			bad(c, e.Error())
			return
		}
		t, e := s.Template(c.Request.Context(), m, c.Param("id"))
		if e != nil {
			fail(c, e)
			return
		}
		c.JSON(200, t)
	})
	claim := func(direct bool) gin.HandlerFunc {
		return func(c *gin.Context) {
			var req struct {
				MerchantID int64  `json:"merchant_id"`
				TemplateID string `json:"template_id"`
				RequestID  string `json:"request_id"`
			}
			if e := c.ShouldBindJSON(&req); e != nil {
				bad(c, e.Error())
				return
			}
			if req.MerchantID <= 0 || req.TemplateID == "" || len(req.TemplateID) > 64 || len(req.RequestID) < 1 || len(req.RequestID) > 96 {
				bad(c, "merchant_id, template_id and request_id (1..96 bytes) required")
				return
			}
			u := c.GetInt64("user")
			if direct {
				t, err := s.Template(c.Request.Context(), req.MerchantID, req.TemplateID)
				if err != nil {
					fail(c, err)
					return
				}
				if t.Mode != "direct" {
					c.JSON(409, gin.H{"error": "template requires async endpoint"})
					return
				}
			}
			if !direct {
				ev, e := s.Claim(c.Request.Context(), req.MerchantID, u, req.TemplateID, req.RequestID)
				if e != nil {
					fail(c, e)
					return
				}
				c.JSON(202, gin.H{"event": ev, "status": "accepted"})
				return
			}
			ev := Event{ID: eventID(req.MerchantID, u, req.TemplateID, req.RequestID), TemplateID: req.TemplateID, MerchantID: req.MerchantID, UserID: u, AcceptedAt: time.Now().Unix()}
			state, e := s.Deliver(c.Request.Context(), ev, "direct")
			if e != nil {
				fail(c, e)
				return
			}
			if state == "failed" {
				c.JSON(409, gin.H{"event": ev, "status": state})
				return
			}
			c.JSON(200, gin.H{"event": ev, "status": state})
		}
	}
	api.POST("/claims", claim(false))
	api.POST("/claims/direct", claim(true))
	api.GET("/claims/:id", func(c *gin.Context) {
		var state, reason string
		e := s.Store.DB.QueryRowContext(c.Request.Context(), "SELECT state,reason FROM grants WHERE id=? AND user_id=?", c.Param("id"), c.GetInt64("user")).Scan(&state, &reason)
		if e != nil {
			fail(c, e)
			return
		}
		c.JSON(200, gin.H{"id": c.Param("id"), "state": state, "reason": reason})
	})
	api.GET("/coupons", func(c *gin.Context) {
		cs, e := s.Store.UserCoupons(c.Request.Context(), c.GetInt64("user"))
		if e != nil {
			fail(c, e)
			return
		}
		c.JSON(200, cs)
	})
	api.POST("/settlements", func(c *gin.Context) {
		var req struct {
			MerchantID int64 `json:"merchant_id"`
			Amount     int64 `json:"amount"`
		}
		if e := c.ShouldBindJSON(&req); e != nil {
			bad(c, e.Error())
			return
		}
		if req.MerchantID <= 0 || req.Amount < 0 || req.Amount > 100000000000 {
			bad(c, "invalid merchant or amount")
			return
		}
		offers, e := s.Settle(c.Request.Context(), c.GetInt64("user"), req.MerchantID, req.Amount)
		if e != nil {
			fail(c, e)
			return
		}
		c.JSON(200, gin.H{"offers": offers})
	})
	api.POST("/coupons/:id/redeem", func(c *gin.Context) {
		var req struct {
			MerchantID int64  `json:"merchant_id"`
			OrderID    string `json:"order_id"`
			Amount     int64  `json:"amount"`
		}
		if e := c.ShouldBindJSON(&req); e != nil {
			bad(c, e.Error())
			return
		}
		if req.MerchantID <= 0 || req.Amount < 0 || req.Amount > 100000000000 || req.OrderID == "" || len(req.OrderID) > 96 {
			bad(c, "valid merchant_id, amount and order_id required")
			return
		}
		cp, e := s.Store.Redeem(c.Request.Context(), c.GetInt64("user"), req.MerchantID, c.Param("id"), req.OrderID, req.Amount)
		if e != nil {
			if errors.Is(e, sql.ErrNoRows) {
				fail(c, e)
			} else {
				c.JSON(409, gin.H{"error": e.Error()})
			}
			return
		}
		if e = s.CacheCoupon(c.Request.Context(), cp); e != nil {
			fail(c, e)
			return
		}
		c.JSON(200, cp)
	})
	return r
}
