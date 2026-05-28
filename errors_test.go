package gochan

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrLaggedError(t *testing.T) {
	err := ErrLagged{Missed: 7}
	assert.Equal(t, "gochan: receiver lagged, missed 7 values", err.Error())
}
