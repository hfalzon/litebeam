package litebeam

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"sync"
)

const (
	metadataDBFilename = "meta.db"
	dbFilePattern      = "shard_%d.db"
	dbConnOptions      = "?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000" //Only for Meta.DB
	metaQuery          = `CREATE TABLE IF NOT EXISTS shards (
		shard_id INTEGER PRIMARY KEY, -- Explicitly defining as PK, not auto-inc initially
		db_path TEXT NOT NULL UNIQUE,
		item_count INTEGER NOT NULL DEFAULT 0,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);`
)

type BalancingMode string

const (
	RoundRobbin BalancingMode = "round-robin"
	Fill        BalancingMode = "fill"
)

type GenerationMode string

const (
	OnStartup GenerationMode = "on-startup"
	Dynamic   GenerationMode = "dynamic"
)

type Config struct {
	BasePath       string
	SoftCap        int
	MaxDBCount     int
	BalancingMode  BalancingMode  //accepts "round-robin", "fill"
	GenerationMode GenerationMode //accepts "on-startup", "dynamic"
	InitSchemaFunc func(db *sql.DB) error
}

type Sharder struct {
	Config     Config
	MetaDB     *sql.DB
	MetaDBPath string
	Mutex      sync.RWMutex
}

type Shard struct {
	Writer *sql.DB
	Reader *sql.DB
}

func NewSharder(c Config) (*Sharder, error) {
	if c.BasePath == "" {
		return nil, errors.New("BasePath cannot be empty")
	}
	if c.SoftCap <= 0 {
		return nil, errors.New("SoftCap must be greater than 0")
	}
	if c.MaxDBCount <= 0 {
		return nil, errors.New("MaxDBCount must be greater than 0")
	}
	switch c.GenerationMode {
	case "":
		//Set to default
		c.GenerationMode = "dynamic"
		log.Print("sharding starting in dynamic mode (generates new sqlite file when required)")
	case "dynamic":
		log.Print("sharding starting in dynamic mode (generates new sqlite file when required)")
	case "on-startup":
		log.Print("sharding starting on startup (generates max required sqlite files on startup (does not reduce) )")
	default:
		return nil, fmt.Errorf("failed to parse litebeam config: %s is not a valid GenerationMode", c.GenerationMode)
	}

	switch c.BalancingMode {
	case "":
		//Set to default
		c.BalancingMode = "fill"
		log.Print("litebeam will fill a database to the softcap before sharding")
	case "fill":
		log.Print("litebeam will fill a database to the softcap before sharding")
	case "round-robin":
		log.Print("litebeam will fill the database with the lowest user-count at the time of insert")
	default:
		return nil, fmt.Errorf("failed to parse litebeam config: %s is not a valid BalancingMode", c.BalancingMode)
	}

	// Create base directory if it doesn't exist
	if err := os.MkdirAll(c.BasePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create BasePath directory '%s': %w", c.BasePath, err)
	}

	metaPath := filepath.Join(c.BasePath, metadataDBFilename)
	metaDBDSN := metaPath + dbConnOptions

	// Open the metadata database (creates if not exists)
	db, err := sql.Open(sqliteDriverName, metaDBDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open/create metadata db '%s': %w", metaPath, err)
	}
	// Recommendation: Keep metadata DB connection pool small, often 1 is fine.
	db.SetMaxOpenConns(1)

	s := &Sharder{
		Config:     c,
		MetaDB:     db,
		MetaDBPath: metaPath,
	}

	// Initialize metadata schema
	if err := s.initMetadataSchema(); err != nil {
		db.Close() // Close DB if initialization fails
		return nil, fmt.Errorf("failed to initialize metadata schema: %w", err)
	}

	// Ensure shards required exists
	if err := s.setUpShards(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ensure initial shard 0 exists: %w", err)
	}

	return s, nil
}

func (s *Sharder) initMetadataSchema() error {
	_, err := s.MetaDB.Exec(metaQuery)
	return err
}

func (s *Sharder) ensureShardExists(shardID int) (int, error) {
	s.Mutex.Lock() // Use exclusive lock for check-and-potentially-create
	defer s.Mutex.Unlock()

	var exists int
	err := s.MetaDB.QueryRow("SELECT COUNT(*) FROM shards WHERE shard_id = ?", shardID).Scan(&exists)
	if err != nil {
		return -1, fmt.Errorf("failed to check existence of shard %d: %w", shardID, err)
	}

	if exists == 0 {
		// Shard doesn't exist, create it
		err = s.createAndRegisterNewShard(shardID) // This also checks MaxDBCount
		if err != nil {
			return -1, fmt.Errorf("failed to create shard %d: %w", shardID, err)
		}
	}

	return shardID, nil
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

func (s *Sharder) setUpShards() error {
	switch s.Config.GenerationMode {
	case "dynamic":
		//Only set up shard0
		shardID, err := s.ensureShardExists(0)
		if err != nil {
			return fmt.Errorf("failed to check existence of shard %d: %w", 0, err)
		}
		if shardID > 0 {
			return nil
		}
		//Create new shard
		err = s.createAndRegisterNewShard(0)
		if err != nil {
			return fmt.Errorf("failed to generate intial shard on dynamic startup: %w", err)
		}

	case "on-startup":
		//Create all files
		count, err := s.GetShardCount()
		if err != nil {
			return fmt.Errorf("failed to get shard count in sharder: %w", err)
		}
		//Create new shard
		if count >= s.Config.MaxDBCount {
			return nil //Do nothing if we have more databases than maxConfig allows
		}
		//Create remainder of shards
		for i := count; i < s.Config.MaxDBCount; i++ {
			err := s.createAndRegisterNewShard(i)
			if err != nil {
				return fmt.Errorf("failed to generate shard %d on startup: %w", i, err)
			}
		}
	}

	return nil
}

// createAndRegisterNewShard handles creating the shard DB file and adding its record to the metadata DB.
// NOTE: This assumes the caller holds the write lock (s.Mutex.Lock()).
// TODO: Cleanup Fill Method
func (s *Sharder) createAndRegisterNewShard(shardID int) error {
	// Check max count first (read operation, but logically part of creation)
	var currentCount int
	err := s.MetaDB.QueryRow("SELECT COUNT(*) FROM shards").Scan(&currentCount)
	if err != nil {
		return fmt.Errorf("failed to query current shard count: %w", err)
	}
	if currentCount >= s.Config.MaxDBCount {
		return fmt.Errorf("maximum number of shards (%d) reached", s.Config.MaxDBCount)
	}
	// Re-verify if the specific shardID already exists just in case of race conditions
	// Although the outer lock should prevent this, belt-and-suspenders.
	var exists int
	err = s.MetaDB.QueryRow("SELECT COUNT(*) FROM shards WHERE shard_id = ?", shardID).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if shard %d exists: %w", shardID, err)
	}
	if exists > 0 {
		// This shard somehow got created between the initial check and now.
		// This shouldn't happen with the current locking, but handle defensively.
		fmt.Printf("litebeam: warning - shard %d already existed when attempting creation\n", shardID)
		return nil // Treat as success, the shard is there.
	}

	dbFilename := fmt.Sprintf(dbFilePattern, shardID)
	dbPath := filepath.Join(s.Config.BasePath, dbFilename)
	shardDSN := createDSN(dbPath)

	// --- Create the actual shard SQLite database file ---
	shardDB, err := sql.Open(sqliteDriverName, shardDSN)
	if err != nil {
		return fmt.Errorf("failed to open/create shard DB file '%s': %w", dbPath, err)
	}
	defer shardDB.Close() // Close connection used for creation/init

	if err := shardDB.Ping(); err != nil {
		return fmt.Errorf("failed to ping newly created shard DB '%s': %w", dbPath, err)
	}

	// Optionally run schema initialization
	if s.Config.InitSchemaFunc != nil {
		if err := s.Config.InitSchemaFunc(shardDB); err != nil {
			_ = os.Remove(dbPath) // Attempt cleanup on schema error
			return fmt.Errorf("failed to initialize schema for shard %d: %w", shardID, err)
		}
	}
	// --- End Shard DB Initialization ---

	// --- Register in metadata DB ---
	tx, err := s.MetaDB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction for registering shard %d: %w", shardID, err)
	}
	defer tx.Rollback() // Rollback if anything fails before commit

	_, err = tx.Exec("INSERT INTO shards (shard_id, db_path, item_count) VALUES (?, ?, 0)", shardID, dbPath)
	if err != nil {
		return fmt.Errorf("failed to insert shard %d metadata: %w", shardID, err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction for registering shard %d: %w", shardID, err)
	}
	// --- End Metadata Registration ---

	fmt.Printf("litebeam: Created and registered new shard %d at %s\n", shardID, dbPath)
	return nil
}

// AssignItemToShard finds a suitable shard or creates one, increments the count in meta.db.
// Returns the ID of the assigned shard.
func (s *Sharder) AssignItemToShard() (int, error) {
	s.Mutex.Lock() // Exclusive lock for find/update/create cycle
	defer s.Mutex.Unlock()
	// Begin transaction for find-and-update or create-and-update
	tx, err := s.MetaDB.Begin()
	if err != nil {
		return -1, fmt.Errorf("failed to begin transaction for user assignment: %w", err)
	}
	defer tx.Rollback() // Ensure rollback if commit isn't reached
	var targetShardID int = -1

	switch s.Config.BalancingMode {
	case "round-robin":
		err = tx.QueryRow("SELECT shard_id FROM shards ORDER BY item_count ASC, shard_id ASC LIMIT 1").Scan(&targetShardID)
		if err != nil {
			tx.Rollback()
			return -1, fmt.Errorf("failed to determine next shard ID: %w", err)
		}
		_, updateErr := tx.Exec("UPDATE shards SET item_count = item_count + 1 WHERE shard_id = ?", targetShardID)
		if updateErr != nil {
			return -1, fmt.Errorf("failed to increment item_count for shard %d: %w", targetShardID, updateErr)
		}
	case "fill":
		// --- Find an existing shard with space ---
		// Query within the transaction
		err = tx.QueryRow("SELECT shard_id FROM shards WHERE item_count < ? ORDER BY shard_id LIMIT 1", s.Config.SoftCap).Scan(&targetShardID)

		if err == nil {
			// Found a shard, increment its count
			_, updateErr := tx.Exec("UPDATE shards SET item_count = item_count + 1 WHERE shard_id = ?", targetShardID)
			if updateErr != nil {
				return -1, fmt.Errorf("failed to increment item_count for shard %d: %w", targetShardID, updateErr)
			}
		} else if errors.Is(err, sql.ErrNoRows) {
			// --- No existing shard has space, need to create a new one ---
			// Rollback the current transaction as we'll perform creation outside of it,
			// but under the same initial lock.
			tx.Rollback() // Explicit rollback before potentially long operation

			// Determine the next shard ID
			var maxID sql.NullInt64
			err = s.MetaDB.QueryRow("SELECT MAX(shard_id) FROM shards").Scan(&maxID)
			if err != nil {
				return -1, fmt.Errorf("failed to determine next shard ID: %w", err)
			}
			nextShardID := 0
			if maxID.Valid {
				nextShardID = int(maxID.Int64) + 1
			}
			log.Print(nextShardID)
			//If next Shard ID is greater than Max Shards
			if nextShardID > s.Config.MaxDBCount-1 { //Have to minus 1 as the shards start at 0
				// Now, start a NEW transaction just to increment the count for the newly created shard
				err = s.MetaDB.QueryRow("SELECT shard_id FROM shards ORDER BY item_count ASC, shard_id ASC").Scan(&targetShardID)
				if err != nil {
					return -1, fmt.Errorf("failed to determine next shard ID where shards are exhausted: %w", err)
				}
				txUpdate, errUpdate := s.MetaDB.Begin()
				if errUpdate != nil {
					return -1, fmt.Errorf("failed to begin transaction for updating new shard %d count: %w", nextShardID, errUpdate)
				}
				defer txUpdate.Rollback()
				_, updateErr := txUpdate.Exec("UPDATE shards SET item_count = item_count + 1 WHERE shard_id = ?", targetShardID)
				if updateErr != nil {
					return -1, fmt.Errorf("failed to increment item_count for shard %d: %w", targetShardID, updateErr)
				}
				// Commit the count update transaction
				if errCommit := txUpdate.Commit(); errCommit != nil {
					return -1, fmt.Errorf("failed to commit item_count update for new shard %d: %w", nextShardID, errCommit)
				}
				return targetShardID, nil
			}

			// Create and register the new shard (checks MaxDBCount inside)
			// This operation commits its own insertion into 'shards' table.
			err = s.createAndRegisterNewShard(nextShardID) // Still under the initial lock
			if err != nil {
				// Creation failed (e.g., max count reached)
				return -1, err // Error from createAndRegisterNewShard is descriptive
			}

			// Now, start a NEW transaction just to increment the count for the newly created shard
			txUpdate, errUpdate := s.MetaDB.Begin()
			if errUpdate != nil {
				return -1, fmt.Errorf("failed to begin transaction for updating new shard %d count: %w", nextShardID, errUpdate)
			}
			defer txUpdate.Rollback()

			_, errUpdate = txUpdate.Exec("UPDATE shards SET item_count = 1 WHERE shard_id = ?", nextShardID)
			if errUpdate != nil {
				return -1, fmt.Errorf("failed to set initial item_count for new shard %d: %w", nextShardID, errUpdate)
			}

			// Commit the count update transaction
			if errCommit := txUpdate.Commit(); errCommit != nil {
				return -1, fmt.Errorf("failed to commit item_count update for new shard %d: %w", nextShardID, errCommit)
			}
			targetShardID = nextShardID // Set the target ID to the newly created one

			// Since we committed the update, return success here
			return targetShardID, nil

		} else {
			// Other SQL error during find query
			return -1, fmt.Errorf("failed to query for available shard: %w", err)
		}
	default:
		return -1, fmt.Errorf("litebeam config error, %s is not a valid balancing mode", s.Config.BalancingMode)
	}

	// Commit the transaction if we found and updated an existing shard
	if err = tx.Commit(); err != nil {
		return -1, fmt.Errorf("failed to commit transaction for shard assignment: %w", err)
	}

	return targetShardID, nil
}

func (s *Sharder) RemoveItemFromShard(ID int) error {
	s.Mutex.Lock() // Exclusive lock for find/update/create cycle
	defer s.Mutex.Unlock()

	txUpdate, errUpdate := s.MetaDB.Begin()
	if errUpdate != nil {
		return fmt.Errorf("failed to begin transaction for removing user from shard %d: %w", ID, errUpdate)
	}
	defer txUpdate.Rollback()

	_, errUpdate = txUpdate.Exec("UPDATE shards SET item_count = MAX(item_count - 1, 0) WHERE shard_id = ?", ID)
	if errUpdate != nil {
		return fmt.Errorf("failed to remove user from meta for shard with id: %d: %w", ID, errUpdate)
	}

	//Commit the update
	if errCommit := txUpdate.Commit(); errCommit != nil {
		return fmt.Errorf("failed to commit the removed user update for shard %d: %w", ID, errCommit)
	}

	return nil
}

func (s *Sharder) GetDB(shardID int) (*sql.DB, error) {
	s.Mutex.RLock() // Read lock sufficient to query metadata
	var dbPath string
	err := s.MetaDB.QueryRow("SELECT db_path FROM shards WHERE shard_id = ?", shardID).Scan(&dbPath)
	s.Mutex.RUnlock() // Release lock after query

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("shard ID %d not found in metadata", shardID)
		}
		return nil, fmt.Errorf("failed to query path for shard %d: %w", shardID, err)
	}

	if dbPath == "" { // Should not happen if query succeeded, but defensive check
		return nil, fmt.Errorf("metadata contains empty path for shard ID %d", shardID)
	}

	// Open connection to the specific shard DB
	shardDSN := dbPath + dbConnOptions
	db, err := sql.Open(sqliteDriverName, shardDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open database for shard %d (%s): %w", shardID, dbPath, err)
	}

	return db, nil
}

func (s *Sharder) GetShard(shardID int) (*Shard, error) {
	reader, err := s.GetDB(shardID)
	if err != nil {
		return nil, err
	}

	writer, err := s.GetDB(shardID)
	if err != nil {
		return nil, err
	}
	writer.SetMaxOpenConns(1)

	shard := Shard{
		Writer: writer,
		Reader: reader,
	}

	return &shard, nil
}

func (s *Sharder) GetAllShards() (map[string]*Shard, error) {
	count, err := s.GetShardCount()
	if err != nil {
		return nil, fmt.Errorf("litebeam: failed to count shards while getting all shards: %w", err)
	}

	var m = map[string]*Shard{}

	for i := range count {
		s, err := s.GetShard(i)
		if err != nil {
			return nil, fmt.Errorf("litebeam: failed to get shard: %w", err)
		}
		m[fmt.Sprintf("shard_%d", i)] = s
	}

	return m, nil
}

// GetItemCount returns the number of items assigned to a specific shard.
func (s *Sharder) GetItemCount(shardID int) (int, error) {
	s.Mutex.RLock() // Read lock sufficient
	defer s.Mutex.RUnlock()

	var count int
	err := s.MetaDB.QueryRow("SELECT item_count FROM shards WHERE shard_id = ?", shardID).Scan(&count)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("shard ID %d not found in metadata", shardID)
		}
		return 0, fmt.Errorf("failed to query item_count for shard %d: %w", shardID, err)
	}
	return count, nil
}

// GetTotalItemCount returns the total number of items across all shards.
func (s *Sharder) GetTotalItemCount() (int, error) {
	s.Mutex.RLock() // Read lock sufficient
	defer s.Mutex.RUnlock()

	var total sql.NullInt64 // Use NullInt64 to handle case where table is empty (SUM returns NULL)
	err := s.MetaDB.QueryRow("SELECT SUM(item_count) FROM shards").Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("failed to query total item_count: %w", err)
	}

	if !total.Valid {
		return 0, nil // No shards or all counts are zero -> total is 0
	}
	return int(total.Int64), nil
}

// GetShardCount returns the current number of active shard databases.
func (s *Sharder) GetShardCount() (int, error) {
	s.Mutex.RLock() // Read lock sufficient
	defer s.Mutex.RUnlock()

	var count int
	err := s.MetaDB.QueryRow("SELECT COUNT(*) FROM shards").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to query shard count: %w", err)
	}
	return count, nil
}

// Close cleans up resources, specifically closing the connection to the metadata database.
func (s *Sharder) Close() error {
	fmt.Println("litebeam: Closing metadata database connection.")
	if s.MetaDB != nil {
		return s.MetaDB.Close()
	}
	return nil
}
