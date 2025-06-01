package litebeam

import (
	"database/sql"
	"testing"
)

func TestNewSharder(t *testing.T) {
	c := Config{
		BasePath:    "./tests",
		TotalShards: 10,
	}
	_, err := NewLitebeam(c)
	if err != nil {
		t.Error(err)
	}
}

func TestNewSharderWithFunc(t *testing.T) {
	c := Config{
		BasePath:    "./tests",
		TotalShards: 10,
		InitSchemaFunc: func(db *sql.DB) error {
			createTableSQL := `
			CREATE TABLE IF NOT EXISTS users (
				id TEXT PRIMARY KEY,
				name TEXT NOT NULL
			);`
			_, err := db.Exec(createTableSQL)
			if err != nil {
				return err
			}
			return nil
		},
	}
	_, err := NewLitebeam(c)
	if err != nil {
		t.Error(err)
	}
}
