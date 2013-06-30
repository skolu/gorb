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
	var query string

	if e == nil {
		query = c.getInfoQuery(tablePath)
		stmts.stmtInfo, e = db.Prepare(query)
	}
	if e == nil {
		query = c.getSelectQuery(tablePath)
		stmts.stmtSelect, e = db.Prepare(query)
	}
	if e == nil {
		query = c.getInsertQuery()
		fmt.Println(query)
		stmts.stmtInsert, e = db.Prepare(query)
	}
	if e == nil {
		query = c.getUpdateQuery()
		stmts.stmtUpdate, e = db.Prepare(query)
	}
	if e == nil {
		query = c.getRemoveQuery()
		stmts.stmtRemove, e = db.Prepare(query)
	}
	if e == nil {
		query = c.getDeleteQuery(tablePath)
		stmts.stmtDelete, e = db.Prepare(query)
	}
	if e != nil {
		fmt.Printf("Invalid query: %s", query)
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
	var query string

	if e == nil {
		query = entity.getInfoQuery()
		stmts.stmtInfo, e = db.Prepare(query)
	}
	if e == nil {
		query = entity.getSelectQuery()
		stmts.stmtSelect, e = db.Prepare(query)
	}
	if e == nil {
		query = entity.getInsertQuery()
		stmts.stmtInsert, e = db.Prepare(query)
	}
	if e == nil {
		query = entity.getUpdateQuery()
		stmts.stmtUpdate, e = db.Prepare(query)
	}
	if e == nil {
		query = entity.getRemoveQuery()
		stmts.stmtRemove, e = db.Prepare(query)
	}
	if e == nil {
		query = entity.getDeleteQuery()
		stmts.stmtDelete, e = db.Prepare(query)
	}
	if e != nil {
		fmt.Printf("Invalid query: %s", query)
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
