package orchestrator

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"strings"
	"testing"
)

func TestReceiptConfirmationArithmetic(t *testing.T) {
	for _, tc := range []struct {
		head, block uint64
		want        int
		invalid     bool
	}{
		{10, 10, 1, false}, {11, 10, 2, false}, {12, 10, 3, false}, {13, 10, 4, false},
		{9, 10, 0, true}, {math.MaxUint64, math.MaxUint64, 1, false},
		{math.MaxUint64, 0, 0, true}, {1000000000, 0, 0, true},
	} {
		got, err := ReceiptConfirmations(tc.head, tc.block)
		if got != tc.want || (err != nil) != tc.invalid {
			t.Fatalf("head=%d block=%d got=%d err=%v", tc.head, tc.block, got, err)
		}
	}
}

func TestReceiptReorgWindowArithmetic(t *testing.T) {
	for _, tc := range []struct {
		previous          int
		head, block, want uint64
	}{
		{1, 10, 10, 1}, {3, 11, 10, 3}, {1, 14, 10, 5}, {1, math.MaxUint64, 0, math.MaxUint64},
	} {
		if got := receiptReorgDepth(tc.previous, tc.head, tc.block); got != tc.want {
			t.Fatalf("depth=%d want=%d", got, tc.want)
		}
	}
}

type receiptBodyTransport string

func (body receiptBodyTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body)))}, nil
}
func TestReceiptRPCEncoding(t *testing.T) {
	hash := "0x" + strings.Repeat("11", 32)
	valid := `{"jsonrpc":"2.0","id":1,"result":{"transactionHash":"` + hash + `","blockHash":"` + hash + `","blockNumber":"0x1","status":"0x1","transactionIndex":"0x0","gasUsed":"0x5208","effectiveGasPrice":"0x1"}}`
	for _, tc := range []struct{ from, to string }{
		{`"blockNumber":"0x1"`, `"blockNumber":"0x01"`},
		{`"status":"0x1"`, `"status":"0x2"`},
		{`"transactionIndex":"0x0"`, `"transactionIndex":"0x-1"`},
		{`"gasUsed":"0x5208"`, `"gasUsed":"0x"`},
		{`"effectiveGasPrice":"0x1"`, `"effectiveGasPrice":"0x+1"`},
	} {
		r := &HTTPRPC{URL: "http://fixture.invalid", Client: &http.Client{Transport: receiptBodyTransport(strings.Replace(valid, tc.from, tc.to, 1))}}
		if _, err := r.ReceiptByHash(context.Background(), [32]byte{}); !errors.Is(err, ErrRPCMalformed) {
			t.Fatalf("malformed encoding %s: %v", tc.to, err)
		}
	}
	r := &HTTPRPC{URL: "http://fixture.invalid", Client: &http.Client{Transport: receiptBodyTransport(valid)}}
	got, err := r.ReceiptByHash(context.Background(), [32]byte{})
	if err != nil || got.GasUsed != 21000 || got.TransactionIndex != 0 || got.EffectiveGasPrice != "1" {
		t.Fatalf("valid receipt %+v %v", got, err)
	}
}
