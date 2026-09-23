package command

// Error is a command-handling failure annotated with the command_id from
// the command payload, so NACKs can be correlated server-side instead of
// falling back to the event id (issue #29). Errors raised before the
// command id is known (decode failures, empty events) are never wrapped.
type Error struct {
	CommandID string
	Err       error
}

func (e *Error) Error() string { return e.Err.Error() }
func (e *Error) Unwrap() error { return e.Err }
