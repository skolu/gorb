package gorb

import (
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"time"
)

type childRows []rowData

type childRowStatus uint32

const (
	RowRead childRowStatus = iota
	RowInserted
	RowUpdated
	RowDeleted
	RowNotModified
)

type (
	entityInfo interface {
		hasRow(tableNo int32, rowId int64) bool
		rowUpdated(tableNo int32, rowId int64)
		rowInserted(tableNo int32, rowId int64)
		rowDeleted(tableNo int32, rowId int64)
		rowSkipped(tableNo int32, rowId int64)
	}

	entityData struct {
		pk    int64
		token uint32

		updated  int
		inserted int
		skipped  int
		deleted  int
		missed   int

		children childRows
	}
	rowData struct {
		tableNo int32
		pk      int64
		status  childRowStatus
	}
)

func (data *entityData) findRow(tableNo int32, rowId int64) (index int, exact bool) {
	exact = false
	index = sort.Search(data.children.Len(), func(i int) bool {
		if data.children[i].tableNo > tableNo {
			return true
		}
		if data.children[i].tableNo == tableNo {
			return data.children[i].pk >= rowId
		}
		return false
	})

	if index >= 0 && index < data.children.Len() {
		if data.children[index].tableNo == tableNo && data.children[index].pk == rowId {
			exact = true
		}
	}

	return
}

func (data *entityData) hasRow(tableNo int32, rowId int64) bool {
	_, xct := data.findRow(tableNo, rowId)
	return xct
}

func (data *entityData) rowUpdated(tableNo int32, rowId int64) {
	idx, xct := data.findRow(tableNo, rowId)
	if xct {
		data.children[idx].status = RowUpdated
		data.updated++
	} else {
		data.missed++
	}
}

func (data *entityData) rowInserted(tableNo int32, rowId int64) {
	if tableNo == 0 {
		data.pk = rowId
	}
	idx, xct := data.findRow(tableNo, rowId)
	if xct {
		data.children[idx].status = RowInserted
		data.inserted++
	} else {
		data.missed++
	}
}
func (data *entityData) rowDeleted(tableNo int32, rowId int64) {
	idx, xct := data.findRow(tableNo, rowId)
	if xct {
		data.children[idx].status = RowDeleted
		data.deleted++
	} else {
		data.missed++
	}
}
func (data *entityData) rowSkipped(tableNo int32, rowId int64) {
	idx, xct := data.findRow(tableNo, rowId)
	if xct {
		data.children[idx].status = RowNotModified
		data.skipped++
	} else {
		data.missed++
	}
}

func (s childRows) Len() int {
	return len(s)
}
func (s childRows) Less(i, j int) bool {
	if s[i].tableNo < s[j].tableNo {
		return true
	}
	if s[i].tableNo == s[j].tableNo {
		return s[i].pk < s[j].pk
	}
	return false
}
func (s childRows) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

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
	e = ent.stmts.stmtInfo.QueryRow(pk).Scan(&((*data).pk), &((*data).token))
	if e != nil {
		return e
	}

	data.children = append(data.children, rowData{tableNo: ent.tableNo, pk: pk})
	for _, ch := range ent.FlattenChildren() {
		ch.populateData(data, pk)
	}

	return e
}

func (t *Table) storeRow(txn *sql.Tx, row reflect.Value, logger entityInfo) error {
	var res sql.Result
	var stmt *sql.Stmt
	var e error
	var pk int64

	pkValue := row.FieldByIndex(t.PrimaryKey.ClassIdx)
	switch pkValue.Kind() {
	case reflect.Int, reflect.Int32, reflect.Int64:
		pk = pkValue.Int()
	case reflect.Uint, reflect.Uint32, reflect.Uint64:
		pk = int64(pkValue.Uint())
	default:
		return fmt.Errorf("Unsupported Primary Key type")
	}

	var isUpdate bool = pk != 0
	if !t.IsPkSerial {
		isUpdate = logger.hasRow(t.tableNo, pk)
	}

	var flds []interface{} = make([]interface{}, 0, len(t.Fields))
	for _, f := range t.Fields {
		if f != t.PrimaryKey {
			fv := row.FieldByIndex(f.ClassIdx)
			if fv.Kind() == reflect.Ptr {
				if !fv.IsNil() {
					fv = fv.Elem()
				}
			}
			fvi := fv.Interface()
			switch p := fvi.(type) {
			case time.Time:
				fvi = p.UTC()
			case nil:

			}
			flds = append(flds, fvi)
		}
	}

	if isUpdate {
		flds = append(flds, pk)
		stmt = t.stmts.stmtUpdate
		if txn != nil {
			stmt = txn.Stmt(stmt)
		}
	} else {
		if !t.IsPkSerial {
			flds = append([]interface{}{pk}, flds...)
		}
		stmt = t.stmts.stmtInsert
		if txn != nil {
			stmt = txn.Stmt(stmt)
		}
	}
	res, e = stmt.Exec(flds...)
	if e != nil {
		return e
	}

	var rowsAffected int64
	rowsAffected, e = res.RowsAffected()
	if e != nil {
		return e
	}
	if rowsAffected > 1 {
		fmt.Errorf("Insert/Update: expected 0 or 1 row to be affected: %d", rowsAffected)
	}

	if isUpdate {
		if rowsAffected == 0 {
			logger.rowSkipped(t.tableNo, pk)
		} else {
			logger.rowUpdated(t.tableNo, pk)
		}
	} else {
		pk, e = res.LastInsertId()
		if e == nil {
			switch pkValue.Kind() {
			case reflect.Int, reflect.Int32, reflect.Int64:
				pkValue.SetInt(pk)
			case reflect.Uint, reflect.Uint32, reflect.Uint64:
				pkValue.SetUint(uint64(pk))
			}
			logger.rowInserted(t.tableNo, pk)
		}
	}

	if e != nil {
		return e
	}
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
				e = child.storeChildRow(txn, childRow, pk, logger)
			}
		case reflect.Slice:
			{
				for i := 0; i < childStorage.Len(); i++ {
					childRow = childStorage.Index(i)
					if !childRow.IsValid() {
						continue
					}
					if childRow.Kind() == reflect.Ptr {
						if childRow.IsNil() {
							continue
						}
						childRow = childRow.Elem()
					}
					e = child.storeChildRow(txn, childRow, pk, logger)
					if e != nil {
						break
					}
				}
			}
		case reflect.Map:
			{
				for _, key := range childStorage.MapKeys() {
					childRow = childStorage.MapIndex(key)
					if childRow.Kind() == reflect.Ptr {
						childRow = childRow.Elem()
					}
					e = child.storeChildRow(txn, childRow, pk, logger)
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

func (c *ChildTable) storeChildRow(txn *sql.Tx, row reflect.Value, parentId int64, logger entityInfo) error {
	fkValue := row.FieldByIndex(c.ParentKey.ClassIdx)
	fkKind := fkValue.Type().Kind()
	if fkKind == reflect.Ptr {
		if fkValue.IsNil() {
			v := reflect.New(fkValue.Type().Elem())
			fkValue.Set(v)
			fkValue = v
		}
	}
	switch fkKind {
	case reflect.Int, reflect.Int32, reflect.Int64:
		fkValue.SetInt(parentId)
	case reflect.Uint, reflect.Uint32, reflect.Uint64:
		fkValue.SetUint(uint64(parentId))
	default:
		return fmt.Errorf("Unsupported Primary Key type")
	}

	return c.storeRow(txn, row, logger)
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
	pkValue := eValue.FieldByIndex(ent.PrimaryKey.ClassIdx)
	switch pkValue.Kind() {
	case reflect.Int, reflect.Int32, reflect.Int64:
		eData.pk = pkValue.Int()
	case reflect.Uint, reflect.Uint32, reflect.Uint64:
		eData.pk = int64(pkValue.Uint())
	default:
		return fmt.Errorf("Unsupported Primary Key type")
	}
	if eData.pk != 0 {
		eData.children = make([]rowData, 0, 16)
		e := ent.populateData(&eData, eData.pk)
		if e != nil {
			return e
		}
		if len(eData.children) > 0 {
			sort.Sort(eData.children)
		}

		if ent.TokenField != nil {
			var token int64 = eValue.FieldByIndex(ent.TokenField.ClassIdx).Int()
			if token != int64(eData.token) {
				return fmt.Errorf("Invalid Edit Token")
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

	var t *Table = &((*ent).Table)
	e = t.storeRow(txn, eValue, &eData)

	if e == nil {
		if len(eData.children) > eData.updated+eData.skipped {
			chlds := ent.FlattenChildren()
			var lastTableNo int32 = 0
			var stmt *sql.Stmt = nil
			var res sql.Result
			var rowsAffected int64
			for i := len(eData.children) - 1; i >= 0; i-- {
				rd := eData.children[i]
				if rd.tableNo > 0 && rd.status == RowRead {
					child := chlds[rd.tableNo-1]
					if child.tableNo == rd.tableNo {
						if lastTableNo != child.tableNo {
							stmt = child.stmts.stmtRemove
							if txn != nil {
								stmt = txn.Stmt(stmt)
							}
							lastTableNo = child.tableNo
						}
						res, e = stmt.Exec(rd.pk)
						if e != nil {
							break
						}
						rowsAffected, e = res.RowsAffected()
						if e != nil {
							break
						}
						if rowsAffected == 1 {
							(&eData).rowDeleted(rd.tableNo, rd.pk)
						}
					}
				}
			}
		}
	}

	if e == nil && txn != nil {
		if e == nil {
			e = txn.Commit()
		} else {
			txn.Rollback()
		}
	}

	return e
}
