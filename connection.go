package gorb

import (
	"database/sql"
	"fmt"
)

type (
	tableStmts struct {
		stmtInfo   *sql.Stmt
		stmtSelect *sql.Stmt
		stmtInsert *sql.Stmt
		stmtUpdate *sql.Stmt
		stmtRemove *sql.Stmt
		stmtDelete *sql.Stmt
	}
)

const (
	timeFormat = "2006-01-02 15:04:05"
)

func (c *ChildTable) dumpChildQueries(tablePath []*ChildTable) {
	fmt.Printf("Select \"%s\": %s\n", c.TableName, c.getSelectQuery(tablePath))
	fmt.Printf("Delete \"%s\": %s\n", c.TableName, c.getDeleteQuery(tablePath))
	for _, chld := range c.Children {
		chld.dumpChildQueries(append(tablePath, c))
	}
}
func (e *Entity) DumpQueries() {
	fmt.Printf("Select \"%s\": %s\n", e.TableName, e.getSelectQuery())
	fmt.Printf("Delete \"%s\": %s\n", e.TableName, e.getDeleteQuery())

	for _, chld := range e.Children {
		chld.dumpChildQueries([]*ChildTable{})
	}
}

func (stmts *tableStmts) releaseStatements() {
	if stmts.stmtInfo != nil {
		stmts.stmtInfo.Close()
		stmts.stmtInfo = nil
	}
	if stmts.stmtSelect != nil {
		stmts.stmtSelect.Close()
		stmts.stmtSelect = nil
	}
	if stmts.stmtInsert != nil {
		stmts.stmtInsert.Close()
		stmts.stmtInsert = nil
	}
	if stmts.stmtUpdate != nil {
		stmts.stmtUpdate.Close()
		stmts.stmtUpdate = nil
	}
	if stmts.stmtRemove != nil {
		stmts.stmtRemove.Close()
		stmts.stmtRemove = nil
	}
	if stmts.stmtDelete != nil {
		stmts.stmtDelete.Close()
		stmts.stmtDelete = nil
	}
}

func (c *ChildTable) createStatements(db *sql.DB, tablePath []*ChildTable) error {
	stmts := new(tableStmts)
	var e error = nil

	if e == nil {
		stmts.stmtInfo, e = db.Prepare(c.getInfoQuery(tablePath))
	}
	if e == nil {
		stmts.stmtSelect, e = db.Prepare(c.getSelectQuery(tablePath))
	}
	if e == nil {
		stmts.stmtInsert, e = db.Prepare(c.getInsertQuery())
	}
	if e == nil {
		stmts.stmtUpdate, e = db.Prepare(c.getUpdateQuery())
	}
	if e == nil {
		stmts.stmtRemove, e = db.Prepare(c.getRemoveQuery())
	}
	if e == nil {
		stmts.stmtDelete, e = db.Prepare(c.getDeleteQuery(tablePath))
	}
	if e != nil {
		stmts.releaseStatements()
		return e
	}

	if c.stmts != nil {
		c.stmts.releaseStatements()
		c.stmts = nil
	}
	c.stmts = stmts

	for _, child := range c.Children {
		e = child.createStatements(db, append(tablePath, c))
		if e != nil {
			return e
		}
	}
	return nil
}

func (entity *Entity) createStatements(db *sql.DB) error {
	stmts := new(tableStmts)
	var e error = nil

	if e == nil {
		stmts.stmtInfo, e = db.Prepare(entity.getInfoQuery())
	}
	if e == nil {
		stmts.stmtSelect, e = db.Prepare(entity.getSelectQuery())
	}
	if e == nil {
		stmts.stmtInsert, e = db.Prepare(entity.getInsertQuery())
	}
	if e == nil {
		stmts.stmtUpdate, e = db.Prepare(entity.getUpdateQuery())
	}
	if e == nil {
		stmts.stmtRemove, e = db.Prepare(entity.getRemoveQuery())
	}
	if e == nil {
		stmts.stmtDelete, e = db.Prepare(entity.getDeleteQuery())
	}
	if e != nil {
		stmts.releaseStatements()
		return e
	}

	if entity.stmts != nil {
		entity.stmts.releaseStatements()
		entity.stmts = nil
	}
	entity.stmts = stmts

	for _, child := range entity.Children {
		e = child.createStatements(db, []*ChildTable{})
		if e != nil {
			return e
		}
	}
	return nil
}

func (t *Table) cascadeDelete(txn *sql.Tx, pk interface{}) error {
	for _, child := range t.Children {
		child.cascadeDelete(txn, pk)
	}
	stmt := txn.Stmt(t.stmts.stmtDelete)
	_, e := stmt.Exec(pk)
	return e
}

func (conn *GorbManager) deleteEntity(ent *Entity, pk interface{}) (bool, error) {
	var e error
	txn, e := conn.db.Begin()
	if e != nil {
		return false, e
	}
	e = ent.cascadeDelete(txn, pk)
	if e == nil {
		e = txn.Commit()
	} else {
		txn.Rollback()
	}
	return e == nil, e
}
