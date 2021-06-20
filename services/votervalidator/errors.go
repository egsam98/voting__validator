package votervalidator

import (
	"errors"
)

var (
	ErrInvalidInput = errors.New("invalid input")
	ErrInvalidVoter = errors.New("invalid voter")
)
