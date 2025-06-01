//go:build cgo
// +build cgo

package litebeam

import _ "github.com/mattn/go-sqlite3"

const sqliteDriverName = "sqlite3" // I can swap this out for something else later
