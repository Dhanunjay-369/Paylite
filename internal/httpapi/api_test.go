package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"paylite/internal/httpapi"
	"paylite/internal/payments"
	"paylite/internal/processor"
	"paylite/internal/testutil"
)

var secret = []byte("test-secret")

type harness struct {
	env *testutil.Env
	srv *httptest.Server
}

func newHarness(t *testing.T, opt testutil.Options) *harness {
	env := testutil.New(t, opt)
	api := httpapi.New(env.Svc, env.Store, env.Cache, secret, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := httptest.NewServer(api.Routes())
	t.Cleanup(srv.Close)
	return &harness{env: env, srv: srv}
}

func (h *harness) do(t *testing.T, method, path string, headers map[string]string, body []byte) (*http.Response, []byte) {
	t.Helper()
	r, err := http.NewRequest(method, h.srv.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

func (h *harness) create(t *testing.T, key string, amount int64) (*http.Response, payments.Payment) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"amount_cents": amount, "currency": "USD", "customer_ref": "cus_1"})
	resp, raw := h.do(t, "POST", "/v1/payments", map[string]string{"X-Merchant-ID": "m1", "Idempotency-Key": key}, body)
	var out struct {
		Payment payments.Payment `json:"payment"`
	}
	_ = json.Unmarshal(raw, &out)
	return resp, out.Payment
}

func (h *harness) webhook(t *testing.T, sig string, ev map[string]string) (int, map[string]string) {
	t.Helper()
	body, _ := json.Marshal(ev)
	if sig == "" {
		sig = httpapi.Sign(secret, body)
	}
	resp, raw := h.do(t, "POST", "/webhooks/processor", map[string]string{"X-Signature": sig}, body)
	var out map[string]string
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

func TestCreateReplayAndErrors(t *testing.T) {
	h := newHarness(t, testutil.Options{})

	resp, p := h.create(t, "k1", 2_500)
	if resp.StatusCode != http.StatusCreated || p.Status != payments.StatusSucceeded {
		t.Fatalf("first: %d %+v", resp.StatusCode, p)
	}
	if resp.Header.Get("Idempotent-Replayed") != "" {
		t.Fatal("the first response must not be marked as a replay")
	}

	resp, again := h.create(t, "k1", 2_500)
	if resp.StatusCode != http.StatusCreated || again.ID != p.ID || resp.Header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay: %d replayed=%q id match=%v", resp.StatusCode, resp.Header.Get("Idempotent-Replayed"), again.ID == p.ID)
	}

	if resp, _ := h.create(t, "k1", 9_999); resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("key reuse: got %d, want 422", resp.StatusCode)
	}
	if resp, _ := h.do(t, "POST", "/v1/payments", map[string]string{"X-Merchant-ID": "m1"}, []byte(`{}`)); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing key: got %d, want 400", resp.StatusCode)
	}
	bad, _ := json.Marshal(map[string]any{"amount_cents": 100, "currency": "DOLLARS", "customer_ref": "c"})
	if resp, _ := h.do(t, "POST", "/v1/payments", map[string]string{"X-Merchant-ID": "m1", "Idempotency-Key": "k2"}, bad); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad currency: got %d, want 400", resp.StatusCode)
	}

	if resp, _ := h.do(t, "GET", "/v1/payments/"+p.ID, map[string]string{"X-Merchant-ID": "someone-else"}, nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("another merchant must not see the payment: got %d", resp.StatusCode)
	}
	if resp, _ := h.do(t, "GET", "/v1/payments/"+p.ID, map[string]string{"X-Merchant-ID": "m1"}, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("owner get: got %d", resp.StatusCode)
	}
}

func TestRateLimit(t *testing.T) {
	h := newHarness(t, testutil.Options{RateLimit: 3})
	for i := 0; i < 3; i++ {
		if resp, _ := h.create(t, "rl-"+string(rune('a'+i)), 100); resp.StatusCode != http.StatusCreated {
			t.Fatalf("request %d: got %d, want 201", i, resp.StatusCode)
		}
	}
	resp, _ := h.create(t, "rl-d", 100)
	if resp.StatusCode != http.StatusTooManyRequests || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("4th request: got %d retry-after=%q, want 429 with Retry-After", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
}

// A timeout leaves the payment "unknown" (HTTP 202). The processor's webhook
// then settles it. Duplicates are absorbed and contradictions are rejected.
func TestWebhookSettlesUnknownPaymentExactlyOnce(t *testing.T) {
	h := newHarness(t, testutil.Options{})
	h.env.Proc.SetMode(processor.ApplyThenTimeout)

	resp, p := h.create(t, "wh-1", 7_000)
	if resp.StatusCode != http.StatusAccepted || p.Status != payments.StatusUnknown {
		t.Fatalf("timeout: %d %+v, want 202 unknown", resp.StatusCode, p)
	}

	ev := map[string]string{"event_id": "evt_1", "type": "payment.succeeded", "payment_id": p.ID, "processor_ref": "ch_x"}
	if code, out := h.webhook(t, "", ev); code != 200 || out["result"] != "applied" {
		t.Fatalf("first delivery: %d %v", code, out)
	}
	if code, out := h.webhook(t, "", ev); code != 200 || out["result"] != "duplicate" {
		t.Fatalf("redelivery: %d %v, want duplicate", code, out)
	}

	// A later, contradictory event must not undo a succeeded payment.
	contra := map[string]string{"event_id": "evt_2", "type": "payment.failed", "payment_id": p.ID}
	if code, out := h.webhook(t, "", contra); code != 200 || out["result"] != "ignored" {
		t.Fatalf("contradiction: %d %v, want ignored", code, out)
	}

	got, err := h.env.Svc.GetPayment(context.Background(), p.ID)
	if err != nil || got.Status != payments.StatusSucceeded {
		t.Fatalf("final status = %v (%v), want succeeded", got.Status, err)
	}
	entries, debits, credits, _ := h.env.Store.LedgerTotals(context.Background())
	if entries != 2 || debits != 7_000 || credits != 7_000 {
		t.Fatalf("ledger: %d entries, debits %d, credits %d", entries, debits, credits)
	}
}

func TestWebhookRejectsBadInput(t *testing.T) {
	h := newHarness(t, testutil.Options{})
	ev := map[string]string{"event_id": "evt_9", "type": "payment.succeeded", "payment_id": "00000000-0000-4000-8000-0000000000aa"}

	if code, _ := h.webhook(t, "deadbeef", ev); code != http.StatusUnauthorized {
		t.Fatalf("bad signature: got %d, want 401", code)
	}
	if code, _ := h.webhook(t, "", map[string]string{"event_id": "evt_9", "type": "payment.succeeded", "payment_id": "not-a-uuid"}); code != http.StatusBadRequest {
		t.Fatalf("malformed payment id: got %d, want 400", code)
	}
	if code, _ := h.webhook(t, "", ev); code != http.StatusNotFound {
		t.Fatalf("unknown payment: got %d, want 404 so the sender retries", code)
	}
	if got := h.env.Count(t, `SELECT count(*) FROM webhook_events`); got != 0 {
		t.Fatalf("an event for an unknown payment must not be recorded as processed, found %d", got)
	}
}
