#!/usr/bin/env bash
# Walks through the three scenarios this project exists to get right.
# Needs the API running with ENABLE_DEBUG=1 (docker compose up does this).
set -euo pipefail

BASE="${BASE:-http://localhost:8080}"
SECRET="${WEBHOOK_SECRET:-dev-webhook-secret}"
MERCHANT="demo_merchant"
RUN="$(date +%s)"
BODY='{"amount_cents":12500,"currency":"USD","customer_ref":"cus_demo"}'

say()      { printf '\n\033[1m== %s\033[0m\n' "$*"; }
set_mode() { curl -s -X POST "$BASE/debug/processor/mode?mode=$1" >/dev/null; }
show()     { printf '   processor : '; curl -s "$BASE/debug/processor"; printf '   ledger    : '; curl -s "$BASE/debug/ledger"; }
payment_id() { sed -n 's/.*"payment":{"id":"\([^"]*\)".*/\1/p'; }
post()     { curl -s -w '\n%{http_code}' -X POST "$BASE/v1/payments" -H "X-Merchant-ID: $MERCHANT" \
               -H "Idempotency-Key: $1" -H 'Content-Type: application/json' -d "$BODY"; }
status_of() { curl -s "$BASE/v1/payments/$1" -H "X-Merchant-ID: $MERCHANT" | sed -n 's/.*"status":"\([^"]*\)".*/\1/p'; }
webhook()  { local payload sig; payload="{\"event_id\":\"$1\",\"type\":\"$2\",\"payment_id\":\"$3\"}"
             sig="$(printf '%s' "$payload" | openssl dgst -sha256 -hmac "$SECRET" -hex | sed 's/^.* //')"
             curl -s -X POST "$BASE/webhooks/processor" -H "X-Signature: $sig" -d "$payload"; }

set_mode approve

say "1. Thirty identical requests at the same instant (a client retrying aggressively)"
seq 1 30 | xargs -P 30 -I{} curl -s -o /dev/null -w '%{http_code}\n' -X POST "$BASE/v1/payments" \
  -H "X-Merchant-ID: $MERCHANT" -H "Idempotency-Key: demo-$RUN-race" -H 'Content-Type: application/json' -d "$BODY" \
  | sort | uniq -c | sed 's/^/   HTTP /'
echo "   (201 = created or replayed result, 409 = first request still in flight)"
show
echo "   -> money moved exactly once, and the books balance."

say "2. The processor takes the money, then the response is lost (timeout)"
set_mode apply_then_timeout
out="$(post "demo-$RUN-timeout")"; id="$(echo "$out" | payment_id)"
echo "   first request   : HTTP $(echo "$out" | tail -1)  (202 = outcome unknown, NOT a failure)"
out="$(post "demo-$RUN-timeout")"
echo "   client retries  : HTTP $(echo "$out" | tail -1)  (same key: no second charge)"
show
echo "   waiting for the reconciler to ask the processor what happened..."
for _ in $(seq 1 30); do s="$(status_of "$id")"; [ "$s" = "succeeded" ] && break; sleep 1; done
echo "   payment status  : $s"
show
echo "   -> still one charge per payment: the reconciler looked it up instead of charging again."

say "3. Webhooks: duplicates and contradictions"
set_mode apply_then_timeout
out="$(post "demo-$RUN-webhook")"; id="$(echo "$out" | payment_id)"
echo "   payment created, status: $(status_of "$id")"
echo "   webhook succeeded : $(webhook "evt-$RUN-1" payment.succeeded "$id")"
echo "   same event again  : $(webhook "evt-$RUN-1" payment.succeeded "$id")"
echo "   later 'failed'    : $(webhook "evt-$RUN-2" payment.failed "$id")"
echo "   final status      : $(status_of "$id")"
show
echo "   -> the duplicate was absorbed, the contradiction ignored, money booked once."
set_mode approve
