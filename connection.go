package gorb

import (
	"database/sql"
	"fmt"
	"reflect"
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

func (t *Table) populateChildren(row reflect.Value) error {
	if t.RowClass != row.Type() {
		return fmt.Errorf("populateChildren: row and schema mismatch")
	}

	var rowKey reflect.Value
	rowKey = row.FieldByIndex(t.PrimaryKey.classIdx)

	for _, childTable := range t.Children {
		var e error
		var rows *sql.Rows

		childStorage := row.FieldByIndex(childTable.ClassIdx)
		if childStorage.IsNil() {
			switch childTable.ChildClass.Kind() {
			case reflect.Slice:
				{
					childStorage = reflect.MakeSlice(childTable.ChildClass, 0, 16)
				}
			case reflect.Map:
				{
					childStorage = reflect.MakeMap(childTable.ChildClass)
				}
			}
		}

		rows, e = childTable.stmts.stmtSelect.Query(rowKey.Interface())
		if rows == nil {
			return e
		}
		var flds []interface{}
		flds = make([]interface{}, len(childTable.Fields))
		for rows.Next() {
			var childRow reflect.Value
			childRow = reflect.New(childTable.RowClass)

			for i, f := range t.Fields {
				if f.FieldType.Kind() == reflect.Ptr {
					ptrV := reflect.New(f.FieldType)
					fldV := childRow.FieldByIndex(f.classIdx)
					fldV.Set(ptrV)
					flds[i] = ptrV.Elem()
				} else {
					flds[i] = childRow.FieldByIndex(f.classIdx)
				}
			}
			e = rows.Scan(flds...)
			if e != nil {
				rows.Close()
				return e
			}
			switch childTable.ChildClass.Kind() {
			case reflect.Ptr:
				{
					childStorage.Set(childRow)
				}
			case reflect.Slice:
				{
					childStorage.Set(reflect.Append(childStorage, childRow))
				}
			case reflect.Map:
				{
					childKey := childRow.FieldByIndex(childTable.PrimaryKey.classIdx)
					childStorage.SetMapIndex(childKey, childRow)
				}
			}

			childTable.populateChildren(childRow)

		}
		e = rows.Err()
		if e != nil {
			return e
		}

	}

	return nil
}

func (conn *GorbManager) EntityGet(object interface{}, pk interface{}) error {
	if conn.db == nil {
		return fmt.Errorf("Database connection is not set")
	}

	if object == nil || pk == nil {
		return fmt.Errorf("EntityGet: parameters cannot be nil")
	}

	eType := reflect.TypeOf(object)
	var isPtr bool = eType.Kind() == reflect.Ptr
	if isPtr {
		eType = eType.Elem()
	}

	var ent *Entity = conn.LookupEntity(eType)
	if ent == nil {
		return fmt.Errorf("Unsupported entity %s", eType.Name())
	}

	var e error
	var flds []interface{} = make([]interface{}, len(ent.Fields))

	rowValue := reflect.ValueOf(object)
	if isPtr {
		rowValue = rowValue.Elem()
	}
	for i, f := range ent.Fields {
		pV := rowValue.FieldByIndex(f.classIdx).Addr().Interface()
		var gs gorbScanner
		gs.ptr = pV
		flds[i] = &gs
	}

	e = ent.stmts.stmtSelect.QueryRow(pk).Scan(flds...)
	if e != nil {
		return e
	}

	return ent.populateChildren(rowValue)
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
