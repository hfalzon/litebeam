package litebeam

import (
	"testing"
)

func TestNewSharder(t *testing.T) {
	c := Config{
		BasePath:   "./tests",
		SoftCap:    100,
		MaxDBCount: 10,
	}
	_, err := NewSharder(c)
	if err != nil {
		t.Error(err)
	}
}
