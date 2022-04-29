package godb

import (
	"context"
	"database/sql"
	"github.com/dimonrus/gocli"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

var logger = func(lg gocli.Logger, message chan string) {
	for s := range message {
		lg.Print("\n " + s)
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

// SQL Query
func (dbo *DBO) Query(query string, args ...interface{}) (*sql.Rows, error) {
	if strings.Contains(query, "?") {
		query = preparePositionalArgsQuery(query)
	}
	if dbo.Debug == true {
		dbo.logMessage <- query
	}
	return dbo.DB.QueryContext(context.Background(), query, args...)
}

// SQL Exec
func (dbo *DBO) Exec(query string, args ...interface{}) (sql.Result, error) {
	if strings.Contains(query, "?") {
		query = preparePositionalArgsQuery(query)
	}
	if dbo.Debug == true {
		dbo.logMessage <- query
	}
	return dbo.DB.ExecContext(context.Background(), query, args...)
}

// SQL Query Row
func (dbo *DBO) QueryRow(query string, args ...interface{}) *sql.Row {
	if strings.Contains(query, "?") {
		query = preparePositionalArgsQuery(query)
	}
	if dbo.Debug == true {
		dbo.logMessage <- query
	}
	return dbo.DB.QueryRowContext(context.Background(), query, args...)
}

// Prepare statement
func (dbo *DBO) Prepare(query string) (*SqlStmt, error) {
	if strings.Contains(query, "?") {
		query = preparePositionalArgsQuery(query)
	}
	stmt, err := dbo.DB.PrepareContext(context.Background(), query)
	return &SqlStmt{Stmt: stmt, Options: dbo.Options, query: query}, err
}

// Begin transaction
func (dbo *DBO) Begin() (*SqlTx, error) {
	tx, err := dbo.DB.BeginTx(context.Background(), nil)
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

// Commit
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

// Rollback
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
	tx.m.Lock()
	defer tx.m.Unlock()
	if strings.Contains(query, "?") {
		query = preparePositionalArgsQuery(query)
	}
	stmt, err := tx.PrepareContext(context.Background(), query)
	return &SqlStmt{Stmt: stmt, Options: tx.Options, query: query}, err
}

// Get Stmt
func (tx *SqlTx) Stmt(stmt *SqlStmt) *SqlStmt {
	tx.m.Lock()
	defer tx.m.Unlock()
	stm := tx.StmtContext(context.Background(), stmt.Stmt)
	return &SqlStmt{Stmt: stm, Options: tx.Options, query: stmt.query}
}

// Exec Transaction
func (tx *SqlTx) Exec(query string, args ...interface{}) (sql.Result, error) {
	tx.m.Lock()
	defer tx.m.Unlock()
	if strings.Contains(query, "?") {
		query = preparePositionalArgsQuery(query)
	}
	if tx.Debug == true {
		tx.logMessage <- query
	}
	return tx.Tx.ExecContext(context.Background(), query, args...)
}

// Query Transaction
func (tx *SqlTx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	tx.m.Lock()
	defer tx.m.Unlock()
	if strings.Contains(query, "?") {
		query = preparePositionalArgsQuery(query)
	}
	if tx.Debug == true {
		tx.logMessage <- query
	}
	return tx.Tx.QueryContext(context.Background(), query, args...)
}

// Query Row Transaction
func (tx *SqlTx) QueryRow(query string, args ...interface{}) *sql.Row {
	tx.m.Lock()
	defer tx.m.Unlock()
	if strings.Contains(query, "?") {
		query = preparePositionalArgsQuery(query)
	}
	if tx.Debug == true {
		tx.logMessage <- query
	}
	return tx.Tx.QueryRowContext(context.Background(), query, args...)
}

// Stmt Exec
func (st *SqlStmt) Exec(args ...interface{}) (sql.Result, error) {
	st.m.Lock()
	defer st.m.Unlock()
	if strings.Contains(st.query, "?") {
		st.query = preparePositionalArgsQuery(st.query)
	}
	if st.Debug == true {
		st.logMessage <- st.query
	}
	return st.Stmt.ExecContext(context.Background(), args...)
}

// Stmt Query
func (st *SqlStmt) Query(args ...interface{}) (*sql.Rows, error) {
	st.m.Lock()
	defer st.m.Unlock()
	if strings.Contains(st.query, "?") {
		st.query = preparePositionalArgsQuery(st.query)
	}
	if st.Debug == true {
		st.logMessage <- st.query
	}
	return st.Stmt.QueryContext(context.Background(), args...)
}

// Stmt Query Row
func (st *SqlStmt) QueryRow(args ...interface{}) *sql.Row {
	st.m.Lock()
	defer st.m.Unlock()
	if strings.Contains(st.query, "?") {
		st.query = preparePositionalArgsQuery(st.query)
	}
	if st.Debug == true {
		st.logMessage <- st.query
	}
	return st.Stmt.QueryRowContext(context.Background(), args...)
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
