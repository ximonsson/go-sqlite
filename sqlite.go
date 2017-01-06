package sqlite

/*
#cgo pkg-config: sqlite3
#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>

static int bind_text (sqlite3_stmt * st, int i, const char * p, int l)
{
	return sqlite3_bind_text (st, i, p, l, SQLITE_TRANSIENT);
}

static int bind_blob (sqlite3_stmt * st, int i, const void * p, int l)
{
	return sqlite3_bind_blob (st, i, p, l, SQLITE_TRANSIENT);
}
*/
import "C"
import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"time"
	"unsafe"
)

type SQLiteDriver struct {
	// nada
}

type SQLiteConnection struct {
	db *C.sqlite3
}

type SQLiteTx struct {
	connection *SQLiteConnection
}

type SQLiteStmt struct {
	connection *SQLiteConnection
	statement  *C.sqlite3_stmt
	tail       string
}

type SQLiteRows struct {
	statement *SQLiteStmt
}

type SQLiteResult struct {
	id   int64
	rows int64
}

func init() {
	sql.Register("sqlite3", &SQLiteDriver{})
}

func execute_sql(db *C.sqlite3, query string) error {
	var error_message *C.char
	sql_query := C.CString(query)
	defer C.free(unsafe.Pointer(sql_query))

	if res := C.sqlite3_exec(db, sql_query, nil, nil, &error_message); res != C.SQLITE_OK {
		msg := C.GoString(error_message)
		C.sqlite3_free(unsafe.Pointer(error_message))
		return errors.New(msg)
	}
	return nil
}

func bind(stmt *SQLiteStmt, i int, v driver.Value) error {
	var res C.int
	switch v := v.(type) {
	case nil:
		res = C.sqlite3_bind_null(stmt.statement, C.int(i))

	case int64:
		res = C.sqlite3_bind_int64(stmt.statement, C.int(i), C.sqlite3_int64(v))

	case float64:
		res = C.sqlite3_bind_double(stmt.statement, C.int(i), C.double(v))

	case bool:
		if v {
			res = C.sqlite3_bind_int(stmt.statement, C.int(i), 1)
		} else {
			res = C.sqlite3_bind_int(stmt.statement, C.int(i), 0)
		}

	case []byte:
		res = C.bind_blob(stmt.statement, C.int(i), unsafe.Pointer(&v[0]), C.int(len(v)))

	case string:
		text := C.CString(v)
		defer C.free(unsafe.Pointer(text))
		res = C.bind_text(stmt.statement, C.int(i), text, C.int(len(v)))

	case time.Time:
		// todo
	}
	if res != C.SQLITE_OK {
		return errors.New("Could not bind")
	}
	return nil
}

/** DRIVER -------------------------------------------------------------------------------------- */

func (d *SQLiteDriver) Open(name string) (driver.Conn, error) {
	var db *C.sqlite3
	db_name := C.CString(name)
	defer C.free(unsafe.Pointer(db_name))

	if res := C.sqlite3_open(db_name, &db); res != C.SQLITE_OK {
		return nil, errors.New("Could not open database")
	}
	return &SQLiteConnection{db}, nil
}

/** CONNECTION ---------------------------------------------------------------------------------- */

func (conn *SQLiteConnection) Begin() (driver.Tx, error) {
	if e := execute_sql(conn.db, "BEGIN"); e != nil {
		return nil, e
	}
	return &SQLiteTx{connection: conn}, nil
}

func (conn *SQLiteConnection) Close() error {
	if res := C.sqlite3_close(conn.db); res != C.SQLITE_OK {
		return errors.New("Error closing connection")
	}
	return nil
}

func (conn *SQLiteConnection) Prepare(query string) (driver.Stmt, error) {
	var statement *C.sqlite3_stmt
	var t *C.char
	var tail string

	sql_query := C.CString(query)
	defer C.free(unsafe.Pointer(sql_query))

	if res := C.sqlite3_prepare(conn.db, sql_query, -1, &statement, &t); res != C.SQLITE_OK {
		return nil, errors.New(C.GoString(C.sqlite3_errmsg(conn.db)))
	}

	if t != nil && C.strlen(t) > 0 {
		tail = strings.TrimSpace(C.GoString(t))
	}
	return &SQLiteStmt{connection: conn, statement: statement, tail: tail}, nil
}

/** ROWS ---------------------------------------------------------------------------------------- */

func (rows *SQLiteRows) Columns() []string {
	n := int(C.sqlite3_column_count(rows.statement.statement))
	columns := make([]string, n)

	for i := range columns {
		columns[i] = C.GoString(C.sqlite3_column_name(rows.statement.statement, C.int(i)))
	}
	return columns
}

func (rows *SQLiteRows) Close() error {
	if r := C.sqlite3_reset(rows.statement.statement); r != C.SQLITE_OK {
		return errors.New("Could not close rows")
	}
	return nil
}

func (rows *SQLiteRows) Next(dest []driver.Value) error {
	s := rows.statement.statement
	if r := C.sqlite3_step(s); r == C.SQLITE_DONE {
		return io.EOF
	} else if r != C.SQLITE_ROW {
		// fattar inte riktigt varför dehär
		if r := C.sqlite3_reset(s); r != C.SQLITE_OK {
			return errors.New("trying to reset")
		}
		return nil
	}

	for i := range dest {
		switch C.sqlite3_column_type(rows.statement.statement, C.int(i)) {
		case C.SQLITE_INTEGER:
			dest[i] = int64(C.sqlite3_column_int(s, C.int(i)))
			// kan tydligen vara bool eller tid också

		case C.SQLITE_FLOAT:
			dest[i] = float64(C.sqlite3_column_double(s, C.int(i)))

		case C.SQLITE_BLOB:
			ptr := C.sqlite3_column_blob(s, C.int(i))
			n := C.sqlite3_column_bytes(s, C.int(i))
			buf := make([]byte, n)

			copy(buf, (*[1 << 30]byte)(unsafe.Pointer(ptr))[:n])
			dest[i] = buf

		case C.SQLITE_NULL:
			dest[i] = nil

		case C.SQLITE_TEXT:
			str := C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_column_text(s, C.int(i)))))
			dest[i] = []byte(str)
			// kan vara bool eller tid också
		}
	}
	return nil
}

/** TX ------------------------------------------------------------------------------------------ */

func (tx *SQLiteTx) Commit() error {
	return execute_sql(tx.connection.db, "COMMIT")
}

func (tx *SQLiteTx) Rollback() error {
	return execute_sql(tx.connection.db, "ROLLBACK")
}

/** STATEMENT ----------------------------------------------------------------------------------- */

func (stmt *SQLiteStmt) Close() error {
	if rc := C.sqlite3_finalize(stmt.statement); rc != C.SQLITE_OK {
		return errors.New("Could not close statement")
	}
	return nil
}

func (stmt *SQLiteStmt) NumInput() int {
	return int(C.sqlite3_bind_parameter_count(stmt.statement))
}

func (stmt *SQLiteStmt) Exec(args []driver.Value) (driver.Result, error) {
	// reset statement first
	res := C.sqlite3_reset(stmt.statement)
	if res != C.SQLITE_OK {
		return nil, errors.New("Error reseting statement")
	}

	for i, v := range args {
		if e := bind(stmt, i+1, v); e != nil {
			return nil, e
		}
	}

	res = C.sqlite3_step(stmt.statement)
	if res != C.SQLITE_OK && res != C.SQLITE_ROW && res != C.SQLITE_DONE {
		return nil, errors.New(C.GoString(C.sqlite3_errstr(res)))
		// return nil, errors.New(C.GoString(C.sqlite3_errmsg(stmt.statement)))
	}
	sql_result := &SQLiteResult{
		id:   int64(C.sqlite3_last_insert_rowid(stmt.connection.db)),
		rows: int64(C.sqlite3_changes(stmt.connection.db)),
	}
	return sql_result, nil
}

func (stmt *SQLiteStmt) Query(args []driver.Value) (driver.Rows, error) {
	for i, v := range args {
		if e := bind(stmt, i+1, v); e != nil {
			return nil, e
		}
	}
	return &SQLiteRows{statement: stmt}, nil
}

/** RESULT -------------------------------------------------------------------------------------- */

func (res *SQLiteResult) LastInsertId() (int64, error) {
	return res.id, nil
}

func (res SQLiteResult) RowsAffected() (int64, error) {
	return res.rows, nil
}
