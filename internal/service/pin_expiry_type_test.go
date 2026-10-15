package service

import (
	"errors"
	"net/http"
	"testing"
)

func TestOnlyTypedStoragePinExpiryAllowsRotation(t *testing.T) {
	for _, test := range []struct {
		name   string
		err    error
		rotate bool
	}{
		{"typed pin expiry", &HTTPStatusError{StatusCode: 409, Code: "pin_expired"}, true},
		{"expired object", &HTTPStatusError{StatusCode: 410, Code: "expired"}, false},
		{"untyped Gone", &HTTPStatusError{StatusCode: 410}, false},
		{"wrong status for pin code", &HTTPStatusError{StatusCode: 410, Code: "pin_expired"}, false},
		{"owner absent", &HTTPStatusError{StatusCode: 409, Code: "owner_absent"}, false},
		{"transport uncertainty", errors.New("lost response"), false},
	} {
		if got := isExpiredPinError(test.err); got != test.rotate {
			t.Errorf("%s: rotate=%v", test.name, got)
		}
	}
}

func TestExpiredSourceObjectDoesNotRotateAcquireIdentity(t *testing.T) {
	state := newTestStore()
	pins := &testPins{acquireFirstError: &HTTPStatusError{StatusCode: http.StatusGone, Code: "expired"}}
	server := newTestServer(t, state, &testAccess{allowed: true}, pins)
	response := createRequest(server, createBody, "expired-source-resource")
	if response.Code != http.StatusServiceUnavailable || len(pins.acquireKeys) != 1 || len(state.expiredKeys) != 0 {
		t.Fatalf("generic object expiry rotated a pin request: status=%d keys=%v expired=%v", response.Code, pins.acquireKeys, state.expiredKeys)
	}
}
