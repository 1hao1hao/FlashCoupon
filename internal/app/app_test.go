package app

import (
	"testing"
	"time"
)

func TestTokenIsolation(t *testing.T) {
	token := SignToken(21, time.Now().Add(time.Minute).Unix(), "secret")
	u, e := VerifyToken(token, "secret")
	if e != nil || u != 21 {
		t.Fatal(u, e)
	}
	for _, input := range []string{token + "0", "x", SignToken(21, time.Now().Add(-time.Second).Unix(), "secret"), SignToken(-1, time.Now().Add(time.Minute).Unix(), "secret")} {
		if _, e = VerifyToken(input, "secret"); e == nil {
			t.Fatal("accepted invalid token")
		}
	}
	if _, e = VerifyToken(token, "other"); e == nil {
		t.Fatal("accepted another signing key")
	}
}
func TestOffers(t *testing.T) {
	cs := []Coupon{{ID: "a", MerchantID: 1, Threshold: 100, Discount: 40, ExpiresAt: 200, Status: "available"}, {ID: "b", MerchantID: 1, Threshold: 0, Discount: 200, ExpiresAt: 200, Status: "available"}, {ID: "c", MerchantID: 2, Discount: 30, ExpiresAt: 200, Status: "available"}, {ID: "d", MerchantID: 1, Discount: 30, ExpiresAt: 100, Status: "available"}, {ID: "e", MerchantID: 1, Discount: 50, ExpiresAt: 200, Status: "used"}}
	os := Offers(cs, 1, 100, 100)
	if len(os) != 2 || os[0].CouponID != "b" || os[0].Payable != 0 || os[1].Discount != 40 {
		t.Fatal(os)
	}
	if len(Offers(nil, 1, 0, 100)) != 0 {
		t.Fatal("empty")
	}
}
func TestRequestIdentity(t *testing.T) {
	a := eventID(1, 2, "t", "r")
	if a != eventID(1, 2, "t", "r") {
		t.Fatal("unstable")
	}
	for _, b := range []string{eventID(2, 2, "t", "r"), eventID(1, 3, "t", "r"), eventID(1, 2, "t2", "r"), eventID(1, 2, "t", "r2")} {
		if a == b {
			t.Fatal("identity collision")
		}
	}
}
