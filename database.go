package godb

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/dimonrus/gocli"
)

var logger = func(lg gocli.Logger, message chan string) {
	for s := range message {
		lg.Println(s)
	}
}

// Init Database Object
func (dbo DBO) Init() (*DBO, error) {
	db, err := getDb(dbo.Connection)
	if err != nil {
		return &dbo, err
	}
	dbo.DB = db
	dbo.logMessage = make(chan string, dbo.Connection.GetMaxConnection())
	go logger(dbo.Logger, dbo.logMessage)
	return &dbo, nil
}

// Get Db Instance
func getDb(connection Connection) (*sql.DB, error) {
	//Open connection
	dbo, err := sql.Open(connection.GetDbType(), connection.String())
	if err != nil {
		return nil, err
	}
	// Ping db
	err = dbo.Ping()
	if err != nil {
		return nil, err
	}
	// Set connection options
	dbo.SetMaxIdleConns(connection.GetMaxIdleConns())
	dbo.SetConnMaxLifetime(time.Second * time.Duration(connection.GetConnMaxLifetime()))
	dbo.SetMaxOpenConns(connection.GetMaxConnection())
	return dbo, nil
}

// Query SQL exec query
func (dbo *DBO) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return dbo.QueryContext(context.Background(), query, args...)
}

// QueryContext SQL exec query
func (dbo *DBO) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if strings.Contains(query, "?") {
		query = preparePositionalArgsQuery(query)
	}
	if dbo.Debug == true {
		dbo.logMessage <- query
	}
	return dbo.DB.QueryContext(ctx, query, args...)
}

// Exec SQL run query
func (dbo *DBO) Exec(query string, args ...interface{}) (sql.Result, error) {
	return dbo.ExecContext(context.Background(), query, args...)
}

// ExecContext SQL run query
func (dbo *DBO) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if strings.Contains(query, "?") {
		query = preparePositionalArgsQuery(query)
	}
	if dbo.Debug == true {
		dbo.logMessage <- query
	}
	return dbo.DB.ExecContext(ctx, query, args...)
}

// QueryRow SQL query row
func (dbo *DBO) QueryRow(query string, args ...interface{}) *sql.Row {
	return dbo.QueryRowContext(context.Background(), query, args...)
}

// QueryRowContext SQL query row
func (dbo *DBO) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	if strings.Contains(query, "?") {
		query = preparePositionalArgsQuery(query)
	}
	if dbo.Debug == true {
		dbo.logMessage <- query
	}
	return dbo.DB.QueryRowContext(ctx, query, args...)
}

// Prepare statement
func (dbo *DBO) Prepare(query string) (*SqlStmt, error) {
	return dbo.PrepareContext(context.Background(), query)
}

// PrepareContext statement
func (dbo *DBO) PrepareContext(ctx context.Context, query string) (*SqlStmt, error) {
	if strings.Contains(query, "?") {
		query = preparePositionalArgsQuery(query)
	}
	stmt, err := dbo.DB.PrepareContext(ctx, query)
	return &SqlStmt{Stmt: stmt, Options: dbo.Options, query: query}, err
}

// Begin transaction
func (dbo *DBO) Begin() (*SqlTx, error) {
	return dbo.BeginContext(context.Background())
}

// BeginContext transaction
func (dbo *DBO) BeginContext(ctx context.Context) (*SqlTx, error) {
	tx, err := dbo.DB.BeginTx(ctx, nil)
	stx := &SqlTx{
		Tx:      tx,
		Options: dbo.Options,
		transaction: &Transaction{
			TTL: int(dbo.Options.TransactionTTL),
		}}
	stx.delayedRollback()
	return stx, err
}

// Delayed rollback
func (tx *SqlTx) delayedRollback() {
	if tx.transaction != nil && tx.transaction.TTL > 0 {
		go func() {
			timer := time.After(time.Duration(tx.transaction.TTL) * time.Second)
			tx.transaction.done = make(chan struct{})
			for {
				select {
				case <-tx.transaction.done:
					close(tx.transaction.done)
					return
				case <-timer:
					err := tx.rollback()
					if err != nil {
						tx.Logger.Println(err)
					}
					return
				}
			}
		}()
	}
	return
}

// Commit
func (tx *SqlTx) commit() error {
	// Commit
	return tx.Tx.Commit()
}

// Commit transaction
func (tx *SqlTx) Commit() error {
	// Stop timer
	if tx.transaction.done != nil {
		tx.transaction.done <- struct{}{}
	}
	// Commit
	return tx.commit()
}

func (tx *SqlTx) rollback() error {
	// Rollback
	return tx.Tx.Rollback()
}

// Rollback transaction
func (tx *SqlTx) Rollback() error {
	// Stop timer
	if tx.transaction.done != nil {
		tx.transaction.done <- struct{}{}
	}
	// rollback
	return tx.rollback()
}

// Prepare Stmt
func (tx *SqlTx) Prepare(query string) (*SqlStmt, error) {
	return tx.PrepareContext(context.Background(), query)
}

// PrepareContext Stmt
func (tx *SqlTx) PrepareContext(ctx context.Context, query string) (*SqlStmt, error) {
	tx.m.Lock()
	defer tx.m.Unlock()
	if strings.Contains(query, "?") {
		query = preparePositionalArgsQuery(query)
	}
	stmt, err := tx.Tx.PrepareContext(ctx, query)
	return &SqlStmt{Stmt: stmt, Options: tx.Options, query: query}, err
}

// Stmt Get Stmt
func (tx *SqlTx) Stmt(stmt *SqlStmt) *SqlStmt {
	return tx.StmtContext(context.Background(), stmt)
}

// StmtContext Get Stmt
func (tx *SqlTx) StmtContext(ctx context.Context, stmt *SqlStmt) *SqlStmt {
	tx.m.Lock()
	defer tx.m.Unlock()
	stm := tx.Tx.StmtContext(ctx, stmt.Stmt)
	return &SqlStmt{Stmt: stm, Options: tx.Options, query: stmt.query}
}

// Exec Transaction
func (tx *SqlTx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return tx.ExecContext(context.Background(), query, args...)
}

// ExecContext Transaction
func (tx *SqlTx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	tx.m.Lock()
	defer tx.m.Unlock()
	if strings.Contains(query, "?") {
		query = preparePositionalArgsQuery(query)
	}
	if tx.Debug == true {
		tx.logMessage <- query
	}
	return tx.Tx.ExecContext(ctx, query, args...)
}

// Query Transaction
func (tx *SqlTx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return tx.QueryContext(context.Background(), query, args...)
}

// QueryContext Transaction
func (tx *SqlTx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	tx.m.Lock()
	defer tx.m.Unlock()
	if strings.Contains(query, "?") {
		query = preparePositionalArgsQuery(query)
	}
	if tx.Debug == true {
		tx.logMessage <- query
	}
	return tx.Tx.QueryContext(ctx, query, args...)
}

// QueryRow Query Row transaction
func (tx *SqlTx) QueryRow(query string, args ...interface{}) *sql.Row {
	return tx.QueryRowContext(context.Background(), query, args...)
}

// QueryRowContext Query Row transaction
func (tx *SqlTx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	tx.m.Lock()
	defer tx.m.Unlock()
	if strings.Contains(query, "?") {
		query = preparePositionalArgsQuery(query)
	}
	if tx.Debug == true {
		tx.logMessage <- query
	}
	return tx.Tx.QueryRowContext(ctx, query, args...)
}

// Exec Stmt Exec
func (st *SqlStmt) Exec(args ...interface{}) (sql.Result, error) {
	return st.ExecContext(context.Background(), args...)
}

// ExecContext Stmt Exec
func (st *SqlStmt) ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error) {
	st.m.Lock()
	defer st.m.Unlock()
	if strings.Contains(st.query, "?") {
		st.query = preparePositionalArgsQuery(st.query)
	}
	if st.Debug == true {
		st.logMessage <- st.query
	}
	return st.Stmt.ExecContext(ctx, args...)
}

// Query Stmt Query
func (st *SqlStmt) Query(args ...interface{}) (*sql.Rows, error) {
	return st.QueryContext(context.Background(), args...)
}

// QueryContext Stmt Query
func (st *SqlStmt) QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error) {
	st.m.Lock()
	defer st.m.Unlock()
	if strings.Contains(st.query, "?") {
		st.query = preparePositionalArgsQuery(st.query)
	}
	if st.Debug == true {
		st.logMessage <- st.query
	}
	return st.Stmt.QueryContext(ctx, args...)
}

// QueryRow Stmt Query Row
func (st *SqlStmt) QueryRow(args ...interface{}) *sql.Row {
	return st.QueryRowContext(context.Background(), args...)
}

// QueryRowContext Stmt Query Row
func (st *SqlStmt) QueryRowContext(ctx context.Context, args ...interface{}) *sql.Row {
	st.m.Lock()
	defer st.m.Unlock()
	if strings.Contains(st.query, "?") {
		st.query = preparePositionalArgsQuery(st.query)
	}
	if st.Debug == true {
		st.logMessage <- st.query
	}
	return st.Stmt.QueryRowContext(ctx, args...)
}

// PreparePositionalArgsQuery Position argument
func preparePositionalArgsQuery(query string) string {
	var ll = len(query)
	var b = make([]byte, ll*2)
	var j int64 = 1
	var i, k, l, s int
	for i < len(query) {
		if query[i] == '?' {
			p := query[s:i] + "$" + strconv.FormatInt(j, 10)
			l += len(p)
			if l > len(b) {
				ll = ll * 2
				b = append(b, make([]byte, ll)...)
			}
			copy(b[k:l], p)
			s = i + 1
			k = l
			j++
		}
		i++
	}
	if i > s {
		p := query[s:]
		l += len(p)
		copy(b[k:l], p)
	}
	b = b[:l]
	return *(*string)(unsafe.Pointer(&b))
}
