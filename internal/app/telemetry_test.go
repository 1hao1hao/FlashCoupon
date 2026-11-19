package app

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func recordedService() (*Service, *tracetest.InMemoryExporter) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	return &Service{Metrics: NewMetrics("test"), Telemetry: &Telemetry{Provider: tp}}, exp
}
func TestTraceCarrierDoesNotChangeEvent(t *testing.T) {
	s, exp := recordedService()
	ctx, end := s.stage(context.Background(), "claim")
	ev := Event{ID: eventID(1, 2, "t", "r"), MerchantID: 1, UserID: 2, TemplateID: "t", AcceptedAt: 1}
	original := encode(ev)
	raw := carrier(ctx)
	next := restoreTrace(context.Background(), raw)
	if trace.SpanContextFromContext(ctx).TraceID() != trace.SpanContextFromContext(next).TraceID() {
		t.Fatal("trace context lost")
	}
	if original != encode(ev) || ev.ID != eventID(1, 2, "t", "r") {
		t.Fatal("tracing mutated business identity")
	}
	_, child := s.stage(next, "deliver")
	child(errors.New("password=must-never-export"))
	end(nil)
	spans := exp.GetSpans()
	if len(spans) != 2 {
		t.Fatal(len(spans))
	}
	if spans[0].Status.Description != "operation failed" {
		t.Fatal("raw error exported")
	}
}
func TestHTTPTraceUsesRouteTemplate(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	s, exp := recordedService()
	r := gin.New()
	r.Use(s.traceHTTP())
	r.GET("/coupon/:id", func(c *gin.Context) { c.Status(200) })
	req := httptest.NewRequest("GET", "/coupon/sensitive-id", nil)
	req.Header.Set("Authorization", "Bearer must-never-export")
	req.Header.Set("traceparent", "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Header().Get("traceparent") == "" {
		t.Fatal("response trace context missing")
	}
	for _, sp := range exp.GetSpans() {
		if sp.Name != "GET /coupon/:id" {
			t.Fatal(sp.Name)
		}
		for _, a := range sp.Attributes {
			if a.Value.AsString() == "sensitive-id" || a.Key == "Authorization" {
				t.Fatal("sensitive attribute")
			}
		}
	}
}
func TestFaultGateDisabled(t *testing.T) {
	t.Setenv("FAULT_INJECTION_ENABLED", "0")
	t.Setenv("FAULT_POINT", "before_lua")
	t.Setenv("FAULT_GATE_DIR", t.TempDir())
	faultPoint(context.Background(), "before_lua")
}
func TestTracingUnavailableCollectorDoesNotFailInitialization(t *testing.T) {
	t.Setenv("TRACING_ENABLED", "1")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	telemetry, err := newTelemetry("api")
	if err != nil {
		t.Fatal(err)
	}
	if telemetry == nil {
		t.Fatal("missing provider")
	}
	// No network contact is required until asynchronous export.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	telemetry.shutdown(ctx)
}
