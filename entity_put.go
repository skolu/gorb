package gorb

import (
	"database/sql"
	"fmt"
	"reflect"
)

type (
	entityLog interface {
		rowUpdated(tableNo int32, rowId int64)
		rowInserted(tableNo int32, rowId int64)
		rowDeleted(tableNo int32, rowId int64)
		rowSkipped(tableNo int32, rowId int64)
	}

	entityData struct {
		pk      int64
		token   uint32
		teenant uint32

		children []rowData
	}
	rowData struct {
		tableNo int32
		pk      int64
	}
)

func (data *entityData) rowUpdated(tableNo int32, rowId int64)  {}
func (data *entityData) rowInserted(tableNo int32, rowId int64) {}
func (data *entityData) rowDeleted(tableNo int32, rowId int64)  {}
func (data *entityData) rowSkipped(tableNo int32, rowId int64)  {}

func (ch *ChildTable) populateData(data *entityData, pk int64) error {
	var e error = nil
	var rows *sql.Rows

	rows, e = ch.stmts.stmtInfo.Query(pk)
	if e == nil {
		var rd rowData
		rd.tableNo = ch.tableNo
		defer rows.Close()
		for rows.Next() {
			e = rows.Scan(&(rd.pk))
			if e != nil {
				break
			}
			data.children = append(data.children, rd)
		}
	}

	return e
}
func (ent *Entity) populateData(data *entityData, pk int64) error {
	var e error = nil
	e = ent.stmts.stmtInfo.QueryRow(pk).Scan(&((*data).pk), &((*data).token), &((*data).teenant))
	for _, ch := range ent.FlattenChildren() {
		ch.populateData(data, pk)
	}

	return e
}

func (conn *GorbManager) EntityPut(entity interface{}) error {
	if conn.db == nil {
		return fmt.Errorf("Database connection is not set")
	}

	var ent *Entity
	eType := reflect.TypeOf(entity)
	var isPtr = eType.Kind() == reflect.Ptr
	if isPtr {
		eType = eType.Elem()
	}
	ent = conn.LookupEntity(eType)
	if ent == nil {
		return fmt.Errorf("Unsupported entity %s", eType.Name())
	}

	var e error

	initf, ok := entity.(interface {
		OnEntitySave() (bool, error)
	})
	if ok {
		ok, e = initf.OnEntitySave()
		if !ok {
			return e
		}
	}

	var eData entityData

	eValue := reflect.ValueOf(entity)
	if isPtr {
		eValue = eValue.Elem()
	}
	pkValue := eValue.FieldByIndex(ent.PrimaryKey.classIdx)
	eData.pk = pkValue.Int()
	if eData.pk != 0 {
		eData.children = make([]rowData, 0, 16)
		e := ent.populateData(&eData, eData.pk)
		if e != nil {
			return e
		}
		if ent.TokenField != nil {
			var token int64 = eValue.FieldByIndex(ent.TokenField.classIdx).Int()
			if token != int64(eData.token) {
				return fmt.Errorf("Invalid Edit Token")
			}
		}
		if ent.TeenantField != nil {
			var teenant int64 = eValue.FieldByIndex(ent.TeenantField.classIdx).Int()
			if teenant != int64(eData.teenant) {
				return fmt.Errorf("Invalid Teenant")
			}
		}
	}
	var txn *sql.Tx = nil
	if len(ent.Children) > 0 {
		txn, e = conn.db.Begin()
		if e != nil {
			return e
		}
	}
	var stmt *sql.Stmt
	var res sql.Result

	var flds []interface{} = make([]interface{}, 0, len(ent.Fields))
	for _, f := range ent.Fields {
		if f != ent.PrimaryKey {
			fv := eValue.FieldByIndex(f.classIdx)
			flds = append(flds, fv.Interface())
		}
	}
	if eData.pk == 0 {
		stmt = ent.stmts.stmtInsert
		if txn != nil {
			stmt = txn.Stmt(stmt)
		}
	} else {
		flds = append(flds, eData.pk)
		stmt = ent.stmts.stmtUpdate
		if txn != nil {
			stmt = txn.Stmt(stmt)
		}
	}
	res, e = stmt.Exec(flds...)
	if e == nil {
		if eData.pk == 0 {
			eData.pk, e = res.LastInsertId()
			if e == nil {
				pkValue.SetInt(eData.pk)
			}
		}
	}
	if e == nil && txn != nil {
		e = ent.storeChildren(txn, eValue, eData.pk, &eData)
		if e == nil {
			e = txn.Commit()
		} else {
			txn.Rollback()
		}
	}

	return e
}
func (c *ChildTable) storeRow(txn *sql.Tx, row reflect.Value, parentId int64, logger entityLog) error {
	var res sql.Result
	var stmt *sql.Stmt
	var e error

	pkValue := row.FieldByIndex(c.PrimaryKey.classIdx)
	pk := pkValue.Int()
	fkValue := row.FieldByIndex(c.ParentKey.classIdx)
	if fkValue.Int() != parentId {
		fkValue.SetInt(parentId)
	}

	var flds []interface{} = make([]interface{}, 0, len(c.Fields))
	for _, f := range c.Fields {
		if f != c.PrimaryKey {
			fv := row.FieldByIndex(f.classIdx)
			flds = append(flds, fv.Interface())
		}
	}
	if pk == 0 {
		stmt = txn.Stmt(c.stmts.stmtInsert)
		res, e = stmt.Exec(flds...)
	} else {
		flds = append(flds, pk)
		stmt = txn.Stmt(c.stmts.stmtUpdate)
		res, e = stmt.Exec(flds...)
	}
	if e != nil {
		return e
	}
	if pk == 0 {
		pk, e = res.LastInsertId()
		if e == nil {
			pkValue.SetInt(pk)
			logger.rowInserted(c.tableNo, pk)
		}
	} else {
		logger.rowUpdated(c.tableNo, pk)
	}

	return c.storeChildren(txn, row, pk, logger)
}

func (t *Table) storeChildren(txn *sql.Tx, row reflect.Value, pk int64, logger entityLog) error {
	var e error = nil
	for _, child := range t.Children {
		childStorage := row.FieldByIndex(child.ClassIdx)
		if childStorage.IsNil() {
			continue
		}
		var childRow reflect.Value
		switch child.ChildClass.Kind() {
		case reflect.Ptr:
			{
				childRow = childStorage.Elem()
				e = child.storeRow(txn, childRow, pk, logger)
			}
		case reflect.Slice:
			{
				for i := 0; i < childStorage.Len(); i++ {
					childRow = childStorage.Index(i)
					e = child.storeRow(txn, childRow, pk, logger)
					if e != nil {
						break
					}
				}
			}
		case reflect.Map:
			{
				for _, key := range childStorage.MapKeys() {
					childRow = childStorage.MapIndex(key)
					e = child.storeRow(txn, childRow, pk, logger)
					if e != nil {
						break
					}
				}
			}
		}

		if e != nil {
			break
		}
	}
	return e
}
