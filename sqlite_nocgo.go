//go:build !cgo
// +build !cgo

package litebeam

import (
	_ "modernc.org/sqlite"
)

const sqliteDriverName = "sqlite" //Can swap out for something else
