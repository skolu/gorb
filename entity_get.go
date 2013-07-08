package gorb

import (
	"database/sql"
	"fmt"
	"reflect"
)

func (t *Table) populateChildren(row reflect.Value) error {
	if t.RowClass != row.Type() {
		return fmt.Errorf("populateChildren: row and schema mismatch")
	}

	var rowKey reflect.Value
	rowKey = row.FieldByIndex(t.PrimaryKey.ClassIdx)

	for _, childTable := range t.Children {
		var e error
		var rows *sql.Rows

		childStorage := row.FieldByIndex(childTable.ClassIdx)
		if childStorage.IsNil() {
			switch childTable.ChildClass.Kind() {
			case reflect.Slice:
				{
					childStorage.Set(reflect.MakeSlice(childTable.ChildClass, 0, 16))
				}
			case reflect.Map:
				{
					childStorage.Set(reflect.MakeMap(childTable.ChildClass))
				}
			}
		}

		rows, e = childTable.stmts.stmtSelect.Query(rowKey.Interface())
		if rows == nil {
			return e
		}
		var flds []interface{}
		flds = make([]interface{}, len(childTable.Fields))
		for i := 0; i < len(flds); i++ {
			flds[i] = new(gorbScanner)
		}
		for rows.Next() {
			var childRow reflect.Value
			childRow = reflect.New(childTable.RowClass)
			childRow = childRow.Elem()

			for i, f := range childTable.Fields {
				pV := childRow.FieldByIndex(f.ClassIdx).Addr().Interface()
				gs, ok := flds[i].(*gorbScanner)
				if ok {
					gs.ptr = pV
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
					childStorage.Set(childRow.Addr())
				}
			case reflect.Slice:
				{
					childStorage.Set(reflect.Append(childStorage, childRow.Addr()))
				}
			case reflect.Map:
				{
					childKey := childRow.FieldByIndex(childTable.PrimaryKey.ClassIdx)
					childStorage.SetMapIndex(childKey, childRow.Addr())
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
		pV := rowValue.FieldByIndex(f.ClassIdx).Addr().Interface()
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
