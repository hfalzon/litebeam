package litebeam

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math/big"
	"net/url"

	_ "github.com/ncruces/go-sqlite3/driver"

	_ "github.com/ncruces/go-sqlite3/embed"
)

const (
	dbFilePattern = "shard_%d.db"
)

type Litebeam struct {
	Config *Config
	Shards map[int]*Shard
}

type Config struct {
	BasePath       string
	TotalShards    int
	InitSchemaFunc func(db *sql.DB) error
}

type Shard struct {
	Writer *sql.DB
	Reader *sql.DB
}

func NewLitebeam(c Config) (*Litebeam, error) {
	conf := c.validateConfig()
	s, err := NewShards(conf)
	if err != nil {
		return nil, err
	}

	return &Litebeam{
		Config: conf,
		Shards: s,
	}, nil
}

func NewShards(c *Config) (map[int]*Shard, error) {
	shards := map[int]*Shard{}
	var openDbs []*sql.DB

	for i := 0; i < c.TotalShards; i++ {
		val := i + 1
		dbPath := c.BasePath + fmt.Sprintf(dbFilePattern, val)
		u := createDSN(dbPath)

		db, err := sql.Open("sqlite3", u)
		if err != nil {
			closeAll(openDbs)
			return nil, fmt.Errorf("error generating writer for shard %d: %v", val, err)
		}
		openDbs = append(openDbs, db)
		db.SetMaxOpenConns(1)

		if c.InitSchemaFunc != nil {
			err = c.InitSchemaFunc(db)
			if err != nil {
				closeAll(openDbs)
				return nil, fmt.Errorf("error initializing database: %v", err)
			}
		}

		rdb, err := sql.Open("sqlite3", u)
		if err != nil {
			closeAll(openDbs)
			return nil, fmt.Errorf("error generating reader for shard %d: %v", val, err)
		}
		openDbs = append(openDbs, rdb)

		s := Shard{
			Writer: db,
			Reader: rdb,
		}
		shards[val] = &s
	}

	return shards, nil
}

func closeAll(dbs []*sql.DB) {
	for _, db := range dbs {
		_ = db.Close()
	}
}

func (l *Litebeam) AssignToShard(base string) (int, error) {
	hash := sha256.Sum256([]byte(base))
	hashHex := hex.EncodeToString(hash[:])

	bigIntHash := new(big.Int)
	_, ok := bigIntHash.SetString(hashHex, 16)
	if !ok {
		return 0, fmt.Errorf("failed to convert hash to integer")
	}

	mod := new(big.Int).Mod(bigIntHash, big.NewInt(int64(l.Config.TotalShards)))
	return int(mod.Int64()) + 1, nil
}

func (l *Litebeam) Close() error {
	var firstErr error
	for i, shard := range l.Shards {
		if err := shard.Writer.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to close writer for shard %d: %w", i, err)
		}
		if err := shard.Reader.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to close reader for shard %d: %w", i, err)
		}
	}
	return firstErr
}

func (c *Config) validateConfig() *Config {
	if c.BasePath[len(c.BasePath)-1] != '/' {
		c.BasePath = c.BasePath + "/"
	}

	return c
}

func createDSN(dbPath string) string {
	//Create connection URL
	connectionUrlParams := make(url.Values)
	connectionUrlParams.Add("_txlock", "immediate")
	connectionUrlParams.Add("_journal_mode", "WAL")
	connectionUrlParams.Add("_busy_timeout", "5000")
	connectionUrlParams.Add("_synchronous", "NORMAL")
	connectionUrlParams.Add("_cache_size", "1000000000")
	connectionUrlParams.Add("_foreign_keys", "true")
	return fmt.Sprintf("file:%s?", dbPath) + connectionUrlParams.Encode()
}
