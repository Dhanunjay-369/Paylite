package payments

import "testing"

func TestCanTransition(t *testing.T) {
	cases := []struct {
		from, to Status
		want     bool
	}{
		{StatusPending, StatusSucceeded, true},
		{StatusPending, StatusFailed, true},
		{StatusPending, StatusUnknown, true},
		{StatusUnknown, StatusSucceeded, true},
		{StatusUnknown, StatusFailed, true},
		{StatusUnknown, StatusPending, false},
		{StatusSucceeded, StatusFailed, false}, // money moved: it cannot "un-succeed"
		{StatusFailed, StatusSucceeded, false}, // a declined payment is final
		{StatusSucceeded, StatusUnknown, false},
		{StatusSucceeded, StatusSucceeded, false}, // same-state is handled as a no-op by the store
	}
	for _, c := range cases {
		if got := CanTransition(c.from, c.to); got != c.want {
			t.Errorf("CanTransition(%s -> %s) = %v, want %v", c.from, c.to, got, c.want)
		}
	}
}

func TestRequestValidationAndHash(t *testing.T) {
	good := CreateRequest{MerchantID: "m1", IdempotencyKey: "k1", AmountCents: 1000, Currency: "USD", CustomerRef: "cus_1"}
	if err := good.Validate(); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	bad := []CreateRequest{
		{IdempotencyKey: "k", AmountCents: 1, Currency: "USD", CustomerRef: "c"},
		{MerchantID: "m", AmountCents: 1, Currency: "USD", CustomerRef: "c"},
		{MerchantID: "m", IdempotencyKey: "k", AmountCents: 0, Currency: "USD", CustomerRef: "c"},
		{MerchantID: "m", IdempotencyKey: "k", AmountCents: -5, Currency: "USD", CustomerRef: "c"},
		{MerchantID: "m", IdempotencyKey: "k", AmountCents: 1, Currency: "US", CustomerRef: "c"},
		{MerchantID: "m", IdempotencyKey: "k", AmountCents: 1, Currency: "usd", CustomerRef: "c"},
		{MerchantID: "m", IdempotencyKey: "k", AmountCents: 1, Currency: "USD"},
	}
	for i, r := range bad {
		if r.Validate() == nil {
			t.Errorf("bad request %d was accepted", i)
		}
	}
	other := good
	other.AmountCents = 2000
	if good.Hash() == other.Hash() {
		t.Error("different amounts must produce different hashes")
	}
	same := good
	same.IdempotencyKey = "different-key"
	if good.Hash() != same.Hash() {
		t.Error("the idempotency key itself must not be part of the content hash")
	}
}
