package godb

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/dimonrus/gocli"
)

// Queryer interface
type Queryer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Prepare(query string) (*SqlStmt, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// Database Object Connection Interface
type Connection interface {
	String() string
	GetDbType() string
	GetMaxConnection() int
	GetConnMaxLifetime() int
	GetMaxIdleConns() int
}

// Options database object options
type Options struct {
	// Debug mode shows logs
	Debug bool
	// Logger
	Logger gocli.Logger
	// log message data
	logMessage chan string
	// TTL for transaction
	TransactionTTL time.Duration `yaml:"transactionTTL"`
}

// Main Database Object
type DBO struct {
	*sql.DB
	Options
	Connection Connection
}

// Transaction
type SqlTx struct {
	m sync.Mutex
	*sql.Tx
	Options
	transaction *Transaction
}

// Stmt
type SqlStmt struct {
	m sync.Mutex
	*sql.Stmt
	Options
	query string
}
