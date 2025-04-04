package litebeam

import "testing"

func TestNewShard(t *testing.T) {
	c := Config{
		BasePath:       "./tests",
		SoftCap:        2,
		MaxDBCount:     5,
		GenerationMode: "on-startup",
		BalancingMode:  "round-robbin",
	}
	s, err := NewSharder(c)
	if err != nil {
		t.Error(err)
	}

	for range 23 {
		i, err := s.AssignItemToShard()
		if err != nil {
			t.Error(err)
		}
		t.Log(i)
	}
}
