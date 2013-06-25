package gorb

import (
	"database/sql"
	"fmt"
	"reflect"
)

type (
	tableStmts struct {
		stmtSelect *sql.Stmt
		stmtInsert *sql.Stmt
		stmtUpdate *sql.Stmt
		stmtDelete *sql.Stmt
	}
)

const (
	timeFormat = "2006-01-02 15:04:05"
)

func (t *Table) dumpQueries(tablePath []*Table) {
	fmt.Printf("Select \"%s\": %s\n", t.TableName, t.getSelectQuery(tablePath))
	for _, chld := range t.Children {
		chld.dumpQueries(append(tablePath, t))
	}
}
func (ent *Entity) DumpQueries() {
	ent.dumpQueries(make([]*Table, 0, 8))
}

func (t *Table) createStatements(db *sql.DB, tablePath []*Table) (bool, error) {
	stmts := new(tableStmts)
	var e error
	var res bool
	var s *sql.Stmt

	s, e = db.Prepare(t.getSelectQuery(tablePath))
	if s == nil {
		return false, e
	}
	stmts.stmtSelect = s

	s, e = db.Prepare(t.getInsertQuery())
	if s == nil {
		return false, e
	}
	stmts.stmtInsert = s

	s, e = db.Prepare(t.getUpdateQuery())
	if s == nil {
		return false, e
	}
	stmts.stmtUpdate = s

	s, e = db.Prepare(t.getDeleteQuery(tablePath))
	if s == nil {
		return false, e
	}
	stmts.stmtDelete = s

	if t.stmts != nil {
		t.stmts.stmtSelect.Close()
		t.stmts.stmtInsert.Close()
		t.stmts.stmtUpdate.Close()
		t.stmts.stmtDelete.Close()

		t.stmts = nil
	}
	t.stmts = stmts

	for _, child := range t.Children {
		res, e = child.createStatements(db, append(tablePath, t))
		if !res {
			return res, e
		}
	}
	return true, nil
}

func (t *Table) populateChildren(row reflect.Value) (bool, error) {
	if t.RowClass != row.Type() {
		return false, fmt.Errorf("populateChildren: row and schema mismatch")
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
			return false, e
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
				return false, e
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
			return false, e
		}

	}

	return true, nil
}

func (conn *GorbManager) EntityGet(object interface{}, pk interface{}) (bool, error) {
	if conn.db == nil {
		return false, fmt.Errorf("Database connection is not set")
	}

	if object == nil || pk == nil {
		return false, fmt.Errorf("EntityGet: parameters cannot be nil")
	}

	eType := reflect.TypeOf(object)
	var isPtr bool = eType.Kind() == reflect.Ptr
	if isPtr {
		eType = eType.Elem()
	}

	var ent *Entity
	ent = conn.LookupEntity(eType)
	if ent == nil {
		return false, fmt.Errorf("Unsupported entity %s", eType.Name())
	}

	var e error
	var flds []interface{}
	flds = make([]interface{}, len(ent.Fields))

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
		return false, e
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
