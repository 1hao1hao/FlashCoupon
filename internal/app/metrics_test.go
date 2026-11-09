package app

import (
	"github.com/gin-gonic/gin"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestShutdownWaitsForInFlightConsumer(t *testing.T) {
	m := &Messaging{}
	if !m.beginConsume() {
		t.Fatal("initial admission refused")
	}
	done := make(chan struct{})
	go func() { m.Close(); close(done) }()
	deadline := time.Now().Add(time.Second)
	for {
		m.consumeMu.Lock()
		closing := m.closing
		m.consumeMu.Unlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("close not started")
		}
		time.Sleep(time.Millisecond)
	}
	if m.beginConsume() {
		t.Fatal("consumer admitted during close")
	}
	select {
	case <-done:
		t.Fatal("closed with in-flight callback")
	default:
	}
	m.consumeWG.Done()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("close did not finish")
	}
	m.Close()
}

func TestMetricsBoundedRoutesAndReplayAccounting(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	m := NewMetrics("api")
	r := gin.New()
	r.Use(m.middleware())
	r.POST("/v1/claims", func(c *gin.Context) { c.Status(202) })
	r.GET("/metrics", gin.WrapH(nilHandler{}))
	for _, path := range []string{"/users/111111", "/users/222222", "/v1/claims", "/v1/claims"} {
		method := "GET"
		if path == "/v1/claims" {
			method = "POST"
		}
		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(method, path, nil))
	}
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, f := range families {
		for _, v := range f.Metric {
			for _, l := range v.Label {
				if strings.Contains(l.GetValue(), "111111") || strings.Contains(l.GetValue(), "222222") {
					t.Fatal("unbounded label", l)
				}
			}
			if f.GetName() == "flashcoupon_claim_responses_total" && v.Counter.GetValue() == 2 {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("claim responses must count replay, separately from durable grants")
	}
}

type nilHandler struct{}

func (nilHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {}
func TestWorkerHasNoBusinessEndpoints(t *testing.T) {
	s := &Service{Config: Config{Role: "worker"}, Metrics: NewMetrics("worker")}
	r := s.Router()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("POST", "/v1/claims", nil))
	if w.Code != 404 {
		t.Fatal(w.Code)
	}
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), "flashcoupon_message_retries_total") {
		t.Fatal("metrics not exposed")
	}
}
