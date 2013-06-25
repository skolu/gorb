package gorb

import (
	"bytes"
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

type (
	GorbConnection interface {
		EntityGet(entity interface{}, pk interface{}) (bool, error)
		EntityPut(entity interface{}) (bool, error)
		EntityDelete(eType reflect.Type, pk interface{}) (bool, error)
	}
)

func (t *table) getSelectQuery(tablePath []*table) string {
	var buffer bytes.Buffer

	buffer.WriteString("SELECT ")
	for i, f := range t.fields {
		if i > 0 {
			buffer.WriteString(", ")
		}
		buffer.WriteString(f.sqlName)
	}
	buffer.WriteString(" FROM ")
	buffer.WriteString(t.tableName)
	buffer.WriteString(" WHERE ")
	if t.parentKey == nil {
		buffer.WriteString(t.primaryKey.sqlName)
	} else {
		buffer.WriteString(t.parentKey.sqlName)
	}
	buffer.WriteString(" =?")

	return buffer.String()
}

func (t *table) getInsertQuery() string {
	var buffer bytes.Buffer

	buffer.WriteString("INSERT INTO ")
	buffer.WriteString(t.tableName)
	buffer.WriteString("(")

	i := 0
	for _, f := range t.fields {
		if i > 0 {
			buffer.WriteString(", ")
		}
		if f != t.primaryKey {
			buffer.WriteString(f.sqlName)
			i++
		}
	}
	buffer.WriteString(") VALUES (")
	for j := 0; j < i; j++ {
		if j > 0 {
			buffer.WriteString(", ")
		}
		buffer.WriteString("?")
	}
	buffer.WriteString(")")

	return buffer.String()
}

func (t *table) getUpdateQuery() string {
	var buffer bytes.Buffer

	buffer.WriteString("UPDATE ")
	buffer.WriteString(t.tableName)
	buffer.WriteString(" SET ")

	i := 0
	for _, f := range t.fields {
		if i > 0 {
			buffer.WriteString(", ")
		}
		if f != t.primaryKey {
			i++
			buffer.WriteString(f.sqlName)
			buffer.WriteString("=?")
		}
	}
	buffer.WriteString(" WHERE ")
	buffer.WriteString(t.primaryKey.sqlName)
	buffer.WriteString("=?")

	return buffer.String()
}

func (t *table) getDeleteQuery(tablePath []*table) string {
	var buffer bytes.Buffer

	if len(tablePath) == 0 {
		buffer.WriteString(fmt.Sprintf("DELETE FROM %s WHERE %s = ?", t.tableName, t.primaryKey.sqlName))
	} else {
		tables := append(tablePath, t)

		buffer.WriteString(fmt.Sprintf("DELETE t%d FROM %s t%d", len(tables), t.tableName, len(tables)))
		if len(tablePath) > 0 {
			for i := len(tablePath) - 1; i > 0; i-- {
				buffer.WriteString(fmt.Sprintf("INNER JOIN %s t%d ON t%d.%s = t%d.%s", tables[i-1].tableName, i, i, tables[i-1].primaryKey.sqlName, i, tables[i].parentKey.sqlName))
			}
		}
		buffer.WriteString(fmt.Sprintf(" WHERE t1.%s = ?", tables[0].primaryKey.sqlName))
	}

	return buffer.String()
}

func (t *table) createStatements(db *sql.DB, tablePath []*table) (bool, error) {
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

	for _, child := range t.children {
		res, e = child.createStatements(db, append(tablePath, t))
		if !res {
			return res, e
		}
	}
	return true, nil
}

func (t *table) populateChildren(row reflect.Value) (bool, error) {
	if t.rowClass != row.Type() {
		return false, fmt.Errorf("populateChildren: row and schema mismatch")
	}

	var rowKey reflect.Value
	rowKey = row.FieldByIndex(t.primaryKey.classIdx)

	for _, childTable := range t.children {
		var e error
		var rows *sql.Rows

		childStorage := row.FieldByIndex(childTable.classIdx)
		if childStorage.IsNil() {
			switch childTable.childClass.Kind() {
			case reflect.Slice:
				{
					childStorage = reflect.MakeSlice(childTable.childClass, 0, 16)
				}
			case reflect.Map:
				{
					childStorage = reflect.MakeMap(childTable.childClass)
				}
			}
		}

		rows, e = childTable.stmts.stmtSelect.Query(rowKey.Interface())
		if rows == nil {
			return false, e
		}
		var flds []interface{}
		flds = make([]interface{}, len(childTable.fields))
		for rows.Next() {
			var childRow reflect.Value
			childRow = reflect.New(childTable.rowClass)

			for i, f := range t.fields {
				if f.fieldType.Kind() == reflect.Ptr {
					ptrV := reflect.New(f.fieldType)
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
			switch childTable.childClass.Kind() {
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
					childKey := childRow.FieldByIndex(childTable.primaryKey.classIdx)
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
	flds = make([]interface{}, len(ent.fields))

	rowValue := reflect.ValueOf(object)
	if isPtr {
		rowValue = rowValue.Elem()
	}
	for i, f := range ent.fields {
		pV := rowValue.FieldByIndex(f.classIdx).Addr().Interface()
		switch f.dataType {
		case Int32, Int64, Float, Bool, DateTime, String:
			{
				var gs gorbScanner
				gs.ptr = pV
				flds[i] = &gs
			}
		default:
			{
				flds[i] = pV
			}
		}
	}

	e = ent.stmts.stmtSelect.QueryRow(pk).Scan(flds...)
	if e != nil {
		return false, e
	}

	return ent.populateChildren(rowValue)
}

func (t *table) cascadeDelete(txn *sql.Tx, pk interface{}) error {
	for _, child := range t.children {
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

func (conn *GorbManager) EntityDelete(eType reflect.Type, pk interface{}) (bool, error) {
	if conn.db == nil {
		return false, fmt.Errorf("Database connection is not set")
	}

	if pk == nil {
		return false, fmt.Errorf("EntityGet: parameters cannot be nil")
	}

	var ent *Entity
	if eType.Kind() == reflect.Ptr {
		eType = eType.Elem()
	}
	ent = conn.LookupEntity(eType)
	if ent == nil {
		return false, fmt.Errorf("Unsupported entity %s", eType.Name())
	}

	return conn.deleteEntity(ent, pk)
}
