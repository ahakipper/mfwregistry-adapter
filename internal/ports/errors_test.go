package ports

import (
	"errors"
	"fmt"
	"testing"
)

type classifiedError struct {
	permanent bool
	err       error
}

func (e classifiedError) Error() string   { return "classified" }
func (e classifiedError) Permanent() bool { return e.permanent }
func (e classifiedError) Unwrap() error   { return e.err }

func TestIsPermanentErrorRequiresEveryFailure(t *testing.T) {
	p := classifiedError{permanent: true}
	u := errors.New("timeout")
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"unknown", u, false},
		{"permanent", p, true},
		{"wrapped", fmt.Errorf("wrap: %w", p), true},
		{"all permanent", errors.Join(p, fmt.Errorf("second: %w", p)), true},
		{"permanent first", errors.Join(p, u), false},
		{"transient first", errors.Join(u, p), false},
		{"nested mixed", fmt.Errorf("full: %w", errors.Join(p, errors.Join(p, u))), false},
		{"explicit retryable wrapper", classifiedError{permanent: false, err: p}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsPermanentError(tc.err); got != tc.want {
				t.Fatalf("IsPermanentError(%v)=%t, want %t", tc.err, got, tc.want)
			}
		})
	}
}
