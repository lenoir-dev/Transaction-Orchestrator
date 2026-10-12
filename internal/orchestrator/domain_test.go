package orchestrator

import (
	"bytes"
	"errors"
	"testing"
)

func TestCanonicalRequest(t *testing.T) {
	a := Request{TenantID: "tenant", IdempotencyKey: "key", ChainID: "00031337", SignerID: "operator",
		To: "0x1111111111111111111111111111111111111111", ValueWei: "00042", Calldata: "0xABCD",
		GasProfile: "standard"}
	b := a
	b.ChainID, b.ValueWei, b.Calldata = "31337", "42", "0xabcd"
	x, err := normalize(a)
	if err != nil {
		t.Fatal(err)
	}
	y, err := normalize(b)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(x.digest[:], y.digest[:]) {
		t.Fatal("equivalent quantities and bytes produced different digests")
	}
	b.ValueWei = "43"
	z, err := normalize(b)
	if err != nil || bytes.Equal(x.digest[:], z.digest[:]) {
		t.Fatal("different value did not change digest", err)
	}
	for _, bad := range []Request{{}, {TenantID: "t", IdempotencyKey: "k", ChainID: "-1"}} {
		if _, err := normalize(bad); !errors.Is(err, ErrInvalid) {
			t.Fatalf("wanted invalid request, got %v", err)
		}
	}
}

func TestTransitionEdges(t *testing.T) {
	for _, edge := range [][2]State{{Created, Reserved}, {Reserved, Signing}, {Ready, Uncertain},
		{Uncertain, Pending}, {Pending, Mined}, {Mined, Confirmed}, {Confirmed, Reorged},
		{Reorged, Pending}, {Blocked, Reserved}} {
		if !CanTransition(edge[0], edge[1]) {
			t.Fatalf("missing edge %s -> %s", edge[0], edge[1])
		}
	}
	for _, edge := range [][2]State{{Created, Confirmed}, {Reserved, Failed}, {Ready, Mined},
		{Pending, Confirmed}, {Confirmed, Created}, {Failed, Signing}} {
		if CanTransition(edge[0], edge[1]) {
			t.Fatalf("unexpected edge %s -> %s", edge[0], edge[1])
		}
	}
}
