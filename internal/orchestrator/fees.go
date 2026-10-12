package orchestrator

import (
	"errors"
	"math/big"
)

var ErrFeeCeiling = errors.New("replacement fee ceiling reached")

// BumpFees uses exact integer arithmetic. Each new value is the greater of
// previous+1 and ceil(previous*(10000+bps)/10000). It never clamps to a cap.
func BumpFees(previous FeeInput, ceiling FeeInput, bps int) (FeeInput, error) {
	if bps < 1 || bps > 100000 {
		return FeeInput{}, ErrInvalid
	}
	oldFee, err := decimalUint256(previous.MaxFeePerGasWei, true)
	if err != nil {
		return FeeInput{}, err
	}
	oldTip, err := decimalUint256(previous.MaxTipPerGasWei, true)
	if err != nil || oldTip.Cmp(oldFee) > 0 {
		return FeeInput{}, ErrTransactionFields
	}
	capFee, err := decimalUint256(ceiling.MaxFeePerGasWei, true)
	if err != nil {
		return FeeInput{}, err
	}
	capTip, err := decimalUint256(ceiling.MaxTipPerGasWei, true)
	if err != nil || capTip.Cmp(capFee) > 0 {
		return FeeInput{}, ErrTransactionFields
	}
	bump := func(n *big.Int) *big.Int {
		factor := big.NewInt(int64(10000 + bps))
		numerator := new(big.Int).Mul(n, factor)
		quotient, remainder := new(big.Int).QuoRem(numerator, big.NewInt(10000), new(big.Int))
		if remainder.Sign() != 0 {
			quotient.Add(quotient, big.NewInt(1))
		}
		minimum := new(big.Int).Add(n, big.NewInt(1))
		if quotient.Cmp(minimum) < 0 {
			return minimum
		}
		return quotient
	}
	fee, tip := bump(oldFee), bump(oldTip)
	if !validUint256(fee, true) || !validUint256(tip, true) ||
		fee.Cmp(capFee) > 0 || tip.Cmp(capTip) > 0 || tip.Cmp(fee) > 0 {
		return FeeInput{}, ErrFeeCeiling
	}
	return FeeInput{MaxFeePerGasWei: fee.String(), MaxTipPerGasWei: tip.String()}, nil
}
