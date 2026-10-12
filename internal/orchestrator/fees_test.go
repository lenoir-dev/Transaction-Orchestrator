package orchestrator

import (
	"errors"
	"math/big"
	"testing"
)

func TestBumpFeesBoundsAndRounding(t *testing.T) {
	for _, tc := range []struct {
		fee, tip         string
		bps              int
		wantFee, wantTip string
	}{
		{"1", "1", 1, "2", "2"},
		{"10000", "1000", 1500, "11500", "1150"},
		{"10001", "101", 1000, "11002", "112"},
		{"7", "3", 1500, "9", "4"},
	} {
		got, err := BumpFees(FeeInput{tc.fee, tc.tip}, FeeInput{"1000000", "1000000"}, tc.bps)
		if err != nil || got.MaxFeePerGasWei != tc.wantFee || got.MaxTipPerGasWei != tc.wantTip {
			t.Fatalf("bump %+v: %+v %v", tc, got, err)
		}
	}
	max := new(big.Int).Set(maxUint256)
	prev := new(big.Int).Div(max, big.NewInt(2)).String()
	got, err := BumpFees(FeeInput{prev, prev}, FeeInput{max.String(), max.String()}, 1500)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := new(big.Int).SetString(got.MaxFeePerGasWei, 10); n.Cmp(max) > 0 {
		t.Fatal("overflow")
	}
	for _, tc := range []struct {
		previous, ceiling FeeInput
		bps               int
		want              error
	}{
		{FeeInput{"100", "10"}, FeeInput{"110", "12"}, 1500, ErrFeeCeiling},
		{FeeInput{max.String(), "10"}, FeeInput{max.String(), max.String()}, 1, ErrFeeCeiling},
		{FeeInput{"100", "10"}, FeeInput{"1000", "1000"}, 0, ErrInvalid},
		{FeeInput{"100", "10"}, FeeInput{"1000", "1000"}, 100001, ErrInvalid},
		{FeeInput{"10", "11"}, FeeInput{"1000", "1000"}, 1500, ErrTransactionFields},
	} {
		_, err := BumpFees(tc.previous, tc.ceiling, tc.bps)
		if !errors.Is(err, tc.want) {
			t.Fatalf("want %v, got %v", tc.want, err)
		}
	}
}

func FuzzBumpFees(f *testing.F) {
	f.Add(uint64(1), uint64(1), uint16(1500))
	f.Add(uint64(10000), uint64(1000), uint16(1000))
	f.Fuzz(func(t *testing.T, fee, tip uint64, bps uint16) {
		if fee == 0 || tip == 0 || tip > fee || bps == 0 || bps > 10000 {
			return
		}
		cap := new(big.Int).Set(maxUint256).String()
		out, err := BumpFees(FeeInput{new(big.Int).SetUint64(fee).String(),
			new(big.Int).SetUint64(tip).String()}, FeeInput{cap, cap}, int(bps))
		if err != nil {
			t.Fatal(err)
		}
		nf, _ := new(big.Int).SetString(out.MaxFeePerGasWei, 10)
		nt, _ := new(big.Int).SetString(out.MaxTipPerGasWei, 10)
		if nf.Cmp(new(big.Int).SetUint64(fee)) <= 0 || nt.Cmp(new(big.Int).SetUint64(tip)) <= 0 ||
			nt.Cmp(nf) > 0 || !validUint256(nf, true) || !validUint256(nt, true) {
			t.Fatal("non-monotone fee progression")
		}
	})
}
