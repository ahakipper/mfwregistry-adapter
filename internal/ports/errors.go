package ports

// IsPermanentError is the shared retry classification contract. An aggregate
// may be dropped only if every non-nil failure is permanent. errors.As alone
// is unsuitable: it finds one Permanent leaf in errors.Join and silently
// ignores a sibling timeout that still requires retry.
func IsPermanentError(err error) bool {
	if err == nil {
		return false
	}
	// Explicit aggregate policies are authoritative, including false. Do not
	// walk past an intentional retryable wrapper into a permanent child.
	if classified, ok := err.(interface{ Permanent() bool }); ok {
		return classified.Permanent()
	}
	if aggregate, ok := err.(interface{ Unwrap() []error }); ok {
		found := false
		for _, child := range aggregate.Unwrap() {
			if child == nil {
				continue
			}
			found = true
			if !IsPermanentError(child) {
				return false
			}
		}
		return found
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return IsPermanentError(wrapped.Unwrap())
	}
	// Unknown errors must remain retryable; absence of a permanent marker is
	// not permission to discard desired-state work.
	return false
}
