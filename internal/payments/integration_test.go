package payments_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"paylite/internal/payments"
	"paylite/internal/processor"
	"paylite/internal/testutil"
)

var ctx = context.Background()

func req(key string, amountCents int64) payments.CreateRequest {
	return payments.CreateRequest{
		MerchantID: "merchant_1", IdempotencyKey: key,
		AmountCents: amountCents, Currency: "USD", CustomerRef: "cus_123",
	}
}

// assertLedger checks the books: the expected number of entries, and that
// debits equal credits equal the expected amount.
func assertLedger(t *testing.T, env *testutil.Env, wantEntries int, wantAmount int64) {
	t.Helper()
	entries, debits, credits, err := env.Store.LedgerTotals(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if entries != wantEntries {
		t.Fatalf("ledger entries = %d, want %d", entries, wantEntries)
	}
	if debits != credits {
		t.Fatalf("ledger out of balance: debits %d != credits %d", debits, credits)
	}
	if debits != wantAmount {
		t.Fatalf("ledger amount = %d, want %d", debits, wantAmount)
	}
}

// reconcileUntilTerminal runs the reconciler until the payment is final.
func reconcileUntilTerminal(t *testing.T, env *testutil.Env, id string) payments.Payment {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := env.Svc.ReconcileOnce(ctx); err != nil {
			t.Fatal(err)
		}
		p, err := env.Svc.GetPayment(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if p.Status.Terminal() {
			return p
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("payment %s did not reach a final status in time", id)
	return payments.Payment{}
}

// THE critical scenario: 50 identical requests arrive at the same instant, as
// when a client times out and retries aggressively. Money must move once.
// Run twice: with Redis healthy, and with Redis completely unreachable.
func TestConcurrentDuplicatesChargeOnce(t *testing.T) {
	for _, tc := range []struct {
		name      string
		redisDown bool
	}{{"redis_up", false}, {"redis_down", true}} {
		t.Run(tc.name, func(t *testing.T) {
			env := testutil.New(t, testutil.Options{RedisDown: tc.redisDown})
			env.Proc.SetLatency(30 * time.Millisecond) // widen the race window

			const n = 50
			results := make([]payments.Result, n)
			errs := make([]error, n)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					results[i], errs[i] = env.Svc.CreatePayment(ctx, req("order-1001", 12_500))
				}(i)
			}
			close(start)
			wg.Wait()

			creators := 0
			for i := 0; i < n; i++ {
				if errs[i] != nil {
					t.Fatalf("request %d failed: %v", i, errs[i])
				}
				if results[i].Payment.ID != results[0].Payment.ID {
					t.Fatalf("requests returned different payments: %s vs %s", results[i].Payment.ID, results[0].Payment.ID)
				}
				if !results[i].Replayed {
					creators++
				}
			}
			if creators != 1 {
				t.Fatalf("%d requests believed they created the payment, want exactly 1", creators)
			}
			if got := env.Count(t, `SELECT count(*) FROM payments`); got != 1 {
				t.Fatalf("payment rows = %d, want 1", got)
			}
			if got := env.Proc.ApprovedCharges(); got != 1 {
				t.Fatalf("processor moved money %d times, want exactly 1", got)
			}
			// Real processors dedupe too, but we must not rely on that: our own
			// guard has to stop the duplicate calls from being made at all.
			if got := env.Proc.Calls(); got != 1 {
				t.Fatalf("processor was called %d times, want exactly 1", got)
			}
			assertLedger(t, env, 2, 12_500)
		})
	}
}

func TestKeyReuseWithDifferentPayloadIsRejected(t *testing.T) {
	env := testutil.New(t, testutil.Options{})
	first, err := env.Svc.CreatePayment(ctx, req("order-7", 1_000))
	if err != nil || first.Payment.Status != payments.StatusSucceeded {
		t.Fatalf("first request: %v %+v", err, first)
	}
	// Same key, different amount: a client bug. Must not charge and must not return the old result.
	if _, err := env.Svc.CreatePayment(ctx, req("order-7", 2_000)); err != payments.ErrIdempotencyKeyReuse {
		t.Fatalf("err = %v, want ErrIdempotencyKeyReuse", err)
	}
	if got := env.Proc.ApprovedCharges(); got != 1 {
		t.Fatalf("charges = %d, want 1", got)
	}
}

func TestDeclineIsFinalAndReplayed(t *testing.T) {
	env := testutil.New(t, testutil.Options{})
	env.Proc.SetMode(processor.Decline)

	first, err := env.Svc.CreatePayment(ctx, req("order-9", 5_000))
	if err != nil || first.Payment.Status != payments.StatusFailed {
		t.Fatalf("first: %v %+v", err, first.Payment)
	}
	// Even after the processor recovers, the same key must keep returning the
	// original decline. A customer who wants to retry sends a NEW key.
	env.Proc.SetMode(processor.Approve)
	again, err := env.Svc.CreatePayment(ctx, req("order-9", 5_000))
	if err != nil || again.Payment.Status != payments.StatusFailed || !again.Replayed {
		t.Fatalf("replay: %v %+v", err, again)
	}
	if env.Proc.ApprovedCharges() != 0 {
		t.Fatal("a declined payment must never move money")
	}
	assertLedger(t, env, 0, 0)
}

// The processor takes the money and then the response is lost. Treating this as
// a failure and retrying would double charge. We must park it as "unknown" and
// ask the processor what happened.
func TestTimeoutAfterChargeBecomesUnknownThenReconciles(t *testing.T) {
	env := testutil.New(t, testutil.Options{})
	env.Proc.SetMode(processor.ApplyThenTimeout)

	res, err := env.Svc.CreatePayment(ctx, req("order-42", 9_900))
	if err != nil {
		t.Fatal(err)
	}
	if res.Payment.Status != payments.StatusUnknown {
		t.Fatalf("status = %s, want unknown (a timeout is not a failure)", res.Payment.Status)
	}
	if env.Proc.ApprovedCharges() != 1 {
		t.Fatal("setup: the processor should have taken the money")
	}

	// The client retries with the same key: no second call to the processor.
	callsBefore := env.Proc.Calls()
	retry, err := env.Svc.CreatePayment(ctx, req("order-42", 9_900))
	if err != nil || !retry.Replayed || retry.Payment.ID != res.Payment.ID {
		t.Fatalf("retry: %v %+v", err, retry)
	}
	if env.Proc.Calls() != callsBefore {
		t.Fatal("a retry of an unknown payment must not call the processor again")
	}

	final := reconcileUntilTerminal(t, env, res.Payment.ID)
	if final.Status != payments.StatusSucceeded {
		t.Fatalf("reconciled status = %s, want succeeded", final.Status)
	}
	if got := env.Proc.ApprovedCharges(); got != 1 {
		t.Fatalf("money moved %d times, want 1", got)
	}
	assertLedger(t, env, 2, 9_900)
}

// The opposite timeout: the processor never received the charge. Reconciliation
// must find nothing, send it again with the same key, and charge exactly once.
func TestTimeoutBeforeChargeIsRetriedSafely(t *testing.T) {
	env := testutil.New(t, testutil.Options{})
	env.Proc.SetMode(processor.DropRequest)

	res, err := env.Svc.CreatePayment(ctx, req("order-43", 3_300))
	if err != nil || res.Payment.Status != payments.StatusUnknown {
		t.Fatalf("create: %v %+v", err, res.Payment)
	}
	if env.Proc.ApprovedCharges() != 0 {
		t.Fatal("setup: nothing should have been charged yet")
	}

	env.Proc.SetMode(processor.Approve) // the processor recovers
	final := reconcileUntilTerminal(t, env, res.Payment.ID)
	if final.Status != payments.StatusSucceeded || env.Proc.ApprovedCharges() != 1 {
		t.Fatalf("status %s, charges %d; want succeeded and 1", final.Status, env.Proc.ApprovedCharges())
	}
	assertLedger(t, env, 2, 3_300)
}

// The service crashes after inserting the row but before calling the processor.
// Without recovery the key would answer "in progress" forever.
func TestOrphanedPendingPaymentIsRecovered(t *testing.T) {
	env := testutil.New(t, testutil.Options{})
	r := req("order-crash", 4_400)
	orphan, created, err := env.Store.InsertOrGet(ctx, payments.Payment{
		ID: "00000000-0000-4000-8000-000000000001", MerchantID: r.MerchantID, IdempotencyKey: r.IdempotencyKey,
		RequestHash: r.Hash(), AmountCents: r.AmountCents, Currency: r.Currency, CustomerRef: r.CustomerRef,
	}, 200*time.Millisecond)
	if err != nil || !created {
		t.Fatalf("setup: %v created=%v", err, created)
	}

	// A retry in the meantime sees an in-progress payment, and does not charge.
	mid, err := env.Svc.CreatePayment(ctx, r)
	if err != nil || mid.Payment.Status != payments.StatusPending || !mid.Replayed {
		t.Fatalf("mid-flight retry: %v %+v", err, mid)
	}

	time.Sleep(250 * time.Millisecond) // the row is now "stale"
	final := reconcileUntilTerminal(t, env, orphan.ID)
	if final.Status != payments.StatusSucceeded || env.Proc.ApprovedCharges() != 1 {
		t.Fatalf("status %s, charges %d", final.Status, env.Proc.ApprovedCharges())
	}
	assertLedger(t, env, 2, 4_400)
}

// When the processor stays unreachable, the reconciler backs off and finally
// hands the payment to a human instead of retrying forever.
func TestUnresolvablePaymentIsParkedForManualReview(t *testing.T) {
	env := testutil.New(t, testutil.Options{})
	env.Proc.SetMode(processor.DropRequest)

	res, err := env.Svc.CreatePayment(ctx, req("order-44", 700))
	if err != nil || res.Payment.Status != payments.StatusUnknown {
		t.Fatalf("create: %v %+v", err, res.Payment)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := env.Svc.ReconcileOnce(ctx); err != nil {
			t.Fatal(err)
		}
		if env.Count(t, `SELECT count(*) FROM payment_events WHERE note = 'parked: needs manual review'`) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if env.Count(t, `SELECT count(*) FROM payment_events WHERE note = 'parked: needs manual review'`) != 1 {
		t.Fatal("payment was never parked")
	}
	p, _ := env.Svc.GetPayment(ctx, res.Payment.ID)
	if p.Status != payments.StatusUnknown || env.Proc.ApprovedCharges() != 0 {
		t.Fatalf("parked payment must stay unknown and uncharged: %s, %d", p.Status, env.Proc.ApprovedCharges())
	}
	if env.Count(t, `SELECT count(*) FROM payments WHERE next_reconcile_at IS NULL`) != 1 {
		t.Fatal("a parked payment must leave the reconcile schedule")
	}
}

// Several workers (or app instances) reconcile at once. FOR UPDATE SKIP LOCKED
// means each payment is claimed by exactly one of them.
func TestConcurrentReconcilersNeverDoubleProcess(t *testing.T) {
	env := testutil.New(t, testutil.Options{})
	const n = 20
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		r := req("bulk-"+string(rune('a'+i)), 1_000)
		id := "00000000-0000-4000-8000-0000000001" + string(rune('0'+i/10)) + string(rune('0'+i%10))
		if _, created, err := env.Store.InsertOrGet(ctx, payments.Payment{
			ID: id, MerchantID: r.MerchantID, IdempotencyKey: r.IdempotencyKey, RequestHash: r.Hash(),
			AmountCents: r.AmountCents, Currency: r.Currency, CustomerRef: r.CustomerRef,
		}, 50*time.Millisecond); err != nil || !created {
			t.Fatalf("setup %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	time.Sleep(100 * time.Millisecond)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var wg sync.WaitGroup
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := env.Svc.ReconcileOnce(ctx); err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		if env.Count(t, `SELECT count(*) FROM payments WHERE status = 'succeeded'`) == n {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := env.Count(t, `SELECT count(*) FROM payments WHERE status = 'succeeded'`); got != n {
		t.Fatalf("succeeded = %d, want %d", got, n)
	}
	if got := env.Proc.ApprovedCharges(); got != n {
		t.Fatalf("money moved %d times for %d payments", got, n)
	}
	assertLedger(t, env, 2*n, int64(n)*1_000)
	_ = ids
}
