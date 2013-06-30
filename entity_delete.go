package gorb

import (
	"database/sql"
	"fmt"
	"reflect"
)

func (t *Table) cascadeDelete(txn *sql.Tx, pk interface{}) error {
	for _, child := range t.Children {
		child.cascadeDelete(txn, pk)
	}
	stmt := txn.Stmt(t.stmts.stmtDelete)
	_, e := stmt.Exec(pk)
	return e
}

func (conn *GorbManager) deleteEntity(ent *Entity, pk interface{}) error {
	txn, e := conn.db.Begin()
	if e != nil {
		return e
	}
	e = ent.cascadeDelete(txn, pk)
	if e == nil {
		e = txn.Commit()
	} else {
		txn.Rollback()
	}
	return e
}

func (conn *GorbManager) EntityDelete(eType reflect.Type, pk interface{}) error {
	if conn.db == nil {
		return fmt.Errorf("Database connection is not set")
	}

	if pk == nil {
		return fmt.Errorf("EntityGet: parameters cannot be nil")
	}

	var ent *Entity
	if eType.Kind() == reflect.Ptr {
		eType = eType.Elem()
	}
	ent = conn.LookupEntity(eType)
	if ent == nil {
		return fmt.Errorf("Unsupported entity %s", eType.Name())
	}

	var e error = nil
	var txn *sql.Tx = nil

	if len(ent.Children) > 0 {
		txn, e = conn.db.Begin()
		if e != nil {
			return e
		}
	}

	var stmt *sql.Stmt

	chldns := ent.FlattenChildren()
	for i := len(chldns) - 1; i >= 0; i-- {
		child := chldns[i]
		stmt = child.stmts.stmtDelete
		if txn != nil {
			stmt = txn.Stmt(stmt)
		}

		_, e = stmt.Exec(pk)
		if txn != nil {
			stmt.Close()
			stmt = nil
		}
		if e != nil {
			break
		}
	}

	stmt = ent.stmts.stmtDelete
	if txn != nil {
		stmt = txn.Stmt(stmt)
	}

	_, e = stmt.Exec(pk)
	if txn != nil {
		stmt.Close()
		stmt = nil
	}

	if txn != nil {
		if e == nil {
			e = txn.Commit()
		} else {
			txn.Rollback()
		}
	}

	return e
}
