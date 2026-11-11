// Package httpapi exposes the service over HTTP.
package httpapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"paylite/internal/payments"
)

// API wires HTTP routes to the payment service.
type API struct {
	svc    *payments.Service
	store  *payments.Store
	cache  payments.Cache
	secret []byte
	log    *slog.Logger
}

func New(svc *payments.Service, store *payments.Store, cache payments.Cache, webhookSecret []byte, log *slog.Logger) *API {
	if cache == nil {
		cache = payments.NopCache{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &API{svc: svc, store: store, cache: cache, secret: webhookSecret, log: log}
}

func (a *API) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/payments", a.createPayment)
	mux.HandleFunc("GET /v1/payments/{id}", a.getPayment)
	mux.HandleFunc("POST /webhooks/processor", a.webhook)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return a.accessLog(mux)
}

type createBody struct {
	AmountCents int64  `json:"amount_cents"`
	Currency    string `json:"currency"`
	CustomerRef string `json:"customer_ref"`
}

type paymentResponse struct {
	Payment  payments.Payment `json:"payment"`
	Replayed bool             `json:"replayed"`
	Message  string           `json:"message,omitempty"`
}

// statusCode maps a payment status to an HTTP status. The mapping depends only
// on the payment, so a replayed request gets exactly the original status code.
func statusCode(s payments.Status) (int, string) {
	switch s {
	case payments.StatusSucceeded:
		return http.StatusCreated, ""
	case payments.StatusFailed:
		return http.StatusPaymentRequired, "the payment was declined"
	case payments.StatusUnknown:
		return http.StatusAccepted, "the outcome is being confirmed with the processor; poll GET /v1/payments/{id}"
	default: // pending: the first request with this key is still running
		return http.StatusConflict, "a request with this idempotency key is still in progress; retry shortly"
	}
}

func (a *API) createPayment(w http.ResponseWriter, r *http.Request) {
	merchant := r.Header.Get("X-Merchant-ID") // stand-in for real authentication
	key := r.Header.Get("Idempotency-Key")
	if merchant == "" || key == "" {
		writeError(w, http.StatusBadRequest, "X-Merchant-ID and Idempotency-Key headers are required")
		return
	}

	allowed, retryAfter, err := a.cache.Allow(r.Context(), merchant)
	if err != nil {
		a.log.Warn("rate limiter unavailable, allowing request", "err", err)
	}
	if !allowed {
		w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Round(time.Second).Seconds())+1))
		writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
		return
	}

	var body createBody
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	res, err := a.svc.CreatePayment(r.Context(), payments.CreateRequest{
		MerchantID: merchant, IdempotencyKey: key,
		AmountCents: body.AmountCents, Currency: body.Currency, CustomerRef: body.CustomerRef,
	})
	switch {
	case errors.Is(err, payments.ErrValidation):
		writeError(w, http.StatusBadRequest, err.Error())
		return
	case errors.Is(err, payments.ErrIdempotencyKeyReuse):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	case err != nil:
		a.log.Error("create payment failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	code, msg := statusCode(res.Payment.Status)
	if res.Replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	if code == http.StatusConflict {
		w.Header().Set("Retry-After", "1")
	}
	writeJSON(w, code, paymentResponse{Payment: res.Payment, Replayed: res.Replayed, Message: msg})
}

func (a *API) getPayment(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	merchant := r.Header.Get("X-Merchant-ID")
	if !payments.ValidID(id) {
		writeError(w, http.StatusNotFound, "payment not found")
		return
	}
	p, err := a.svc.GetPayment(r.Context(), id)
	if errors.Is(err, payments.ErrNotFound) || (err == nil && p.MerchantID != merchant) {
		writeError(w, http.StatusNotFound, "payment not found") // never reveal other merchants' payments
		return
	}
	if err != nil {
		a.log.Error("get payment failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, p)
}

type webhookBody struct {
	EventID      string `json:"event_id"`
	Type         string `json:"type"`
	PaymentID    string `json:"payment_id"`
	ProcessorRef string `json:"processor_ref"`
}

// Sign returns the hex HMAC-SHA256 signature the processor must send in X-Signature.
func Sign(secret, body []byte) string {
	m := hmac.New(sha256.New, secret)
	m.Write(body)
	return hex.EncodeToString(m.Sum(nil))
}

func validSignature(secret, body []byte, header string) bool {
	got, err := hex.DecodeString(header)
	if err != nil {
		return false
	}
	m := hmac.New(sha256.New, secret)
	m.Write(body)
	return hmac.Equal(got, m.Sum(nil)) // constant-time comparison
}

// webhook receives processor events. Webhooks are treated as untrusted,
// at-least-once, possibly out-of-order messages: verify the signature, dedupe
// by event ID, and let the state machine reject anything that makes no sense.
func (a *API) webhook(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "could not read body")
		return
	}
	if !validSignature(a.secret, body, r.Header.Get("X-Signature")) {
		writeError(w, http.StatusUnauthorized, "invalid signature")
		return
	}
	var ev webhookBody
	if err := json.Unmarshal(body, &ev); err != nil || ev.EventID == "" || !payments.ValidID(ev.PaymentID) {
		writeError(w, http.StatusBadRequest, "malformed event")
		return
	}

	var outcome payments.Outcome
	switch ev.Type {
	case "payment.succeeded":
		outcome = payments.Outcome{To: payments.StatusSucceeded, ProcessorRef: ev.ProcessorRef, Note: "webhook " + ev.EventID}
	case "payment.failed":
		outcome = payments.Outcome{To: payments.StatusFailed, ProcessorRef: ev.ProcessorRef, Reason: "declined (webhook)", Note: "webhook " + ev.EventID}
	default:
		writeError(w, http.StatusBadRequest, "unsupported event type")
		return
	}

	result, err := a.store.ApplyWebhook(r.Context(), ev.EventID, ev.PaymentID, outcome)
	switch {
	case errors.Is(err, payments.ErrNotFound):
		writeError(w, http.StatusNotFound, "payment not found") // non-2xx: the sender will retry
		return
	case err != nil:
		a.log.Error("webhook failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if result == payments.WebhookIgnored {
		a.log.Warn("webhook contradicts final payment state; ignored", "event_id", ev.EventID, "payment_id", ev.PaymentID, "type", ev.Type)
	}
	writeJSON(w, http.StatusOK, map[string]string{"result": string(result)})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(code int) { s.code = code; s.ResponseWriter.WriteHeader(code) }

func (a *API) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(rec, r)
		a.log.Info("request", "method", r.Method, "path", r.URL.Path, "status", rec.code, "ms", time.Since(start).Milliseconds())
	})
}
