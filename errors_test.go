package gochan_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/amorey/gochan"
)

func TestErrLaggedError(t *testing.T) {
	err := gochan.ErrLagged{Missed: 7}
	assert.Equal(t, "gochan: receiver lagged, missed 7 values", err.Error())
}
