package service

import (
	"testing"

	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

func TestInitiatorEncodingPreservesPrincipalBoundaries(t *testing.T) {
	want := storagev1.Principal{Subject: "tenant/a:user", Domain: "project:domain"}
	encoded, err := EncodeInitiator(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeInitiator(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("DecodeInitiator() = %#v, want %#v", got, want)
	}
	if got, err := DecodeInitiator(encoded + "junk"); err == nil {
		t.Fatalf("accepted noncanonical initiator %q as %#v", encoded+"junk", got)
	}
}

func TestInitiatorEncodingRejectsEmptyAndUnboundedPrincipals(t *testing.T) {
	for _, principal := range []storagev1.Principal{
		{}, {Subject: "subject"}, {Domain: "domain"},
		{Subject: "bad\nsubject", Domain: "domain"},
		{Subject: string(make([]byte, 257)), Domain: "domain"},
	} {
		if _, err := EncodeInitiator(principal); err == nil {
			t.Errorf("EncodeInitiator(%#v) unexpectedly succeeded", principal)
		}
	}
}
