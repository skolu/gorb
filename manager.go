package gorb

import (
	"database/sql"
	"fmt"
	"reflect"
)

type (
	GorbEntityEvents interface {
		OnEntitySave() (bool, error)
		OnEntityInit()
	}

	GorbConnection interface {
		EntityGet(entity interface{}, pk interface{}) error
		EntityPut(entity interface{}) error
		EntityDelete(eType reflect.Type, pk interface{}) error
	}

	GorbManager struct {
		Entities map[reflect.Type]*Entity
		names    map[string]reflect.Type
		db       *sql.DB
	}
)

func (mgr *GorbManager) LookupEntity(class reflect.Type) *Entity {
	if mgr.Entities != nil {
		return mgr.Entities[class]
	}
	return nil
}

func (mgr *GorbManager) LookupEntityType(tableName string) reflect.Type {
	return mgr.names[tableName]
}

func (mgr *GorbManager) RegisterEntity(class reflect.Type, tableName string) (*Entity, error) {
	if class.Kind() != reflect.Struct {
		return nil, fmt.Errorf("Invalid Gorb entity type: %s. Struct expected", class.Name())
	}
	var ok bool
	if mgr.Entities != nil {
		if _, ok = mgr.Entities[class]; ok {
			return nil, fmt.Errorf("Type %s is already registered.", class.Name())
		}
	}
	if mgr.names != nil {
		if _, ok = mgr.names[tableName]; ok {
			return nil, fmt.Errorf("SQL entity %s is already registered.", tableName)
		}
	}

	e := new(Entity)
	e.init()
	e.TableName = tableName
	e.RowClass = class
	res, err := e.extractGorbSchema(class, []int{}, e)
	if res {
		res, err = e.check()
		if res {
			if mgr.Entities == nil {
				mgr.Entities = make(map[reflect.Type]*Entity, 16)
			}
			if mgr.names == nil {
				mgr.names = make(map[string]reflect.Type, 16)
			}
			e.Table.tableNo = 0
			tables := e.FlattenChildren()
			for i, t := range tables {
				t.tableNo = i + 1
			}
			mgr.Entities[class] = e
			mgr.names[e.TableName] = class
		}
	}

	return e, err
}

func (mgr *GorbManager) EntityByType(class reflect.Type) (interface{}, error) {
	e, ok := mgr.Entities[class]
	if !ok {
		return nil, fmt.Errorf("Class not registered")
	}

	ret := reflect.New(e.RowClass).Interface()
	initf, ok := ret.(interface {
		OnEntityInit()
	})
	if ok {
		initf.OnEntityInit()
	}

	return ret, nil
}

func (mgr *GorbManager) EntityByName(name string) (interface{}, error) {
	if len(name) == 0 {
		return nil, fmt.Errorf("Empty entity name")
	}
	class, ok := mgr.names[name]
	if !ok {
		return nil, fmt.Errorf("Entity %s is not registered", name)
	}
	ret, err := mgr.EntityByType(class)
	return ret, err
}

func (mgr *GorbManager) SetDB(db *sql.DB) error {
	mgr.db = db

	for _, ent := range mgr.Entities {
		e := ent.createStatements(db)
		if e != nil {
			return e
		}
	}

	return nil
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
	if eType.Kind() == reflect.Ptr {
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

	value := reflect.ValueOf(entity)
	var pk int64 = value.FieldByIndex(ent.PrimaryKey.classIdx).Int()
	if pk != 0 {
		eData.children = make([]rowData, 0, 16)
		e := ent.populateData(&eData, pk)
		if e != nil {
			return e
		}
		if ent.TokenField != nil {
			var token int64 = value.FieldByIndex(ent.TokenField.classIdx).Int()
			if token != int64(eData.token) {
				return fmt.Errorf("Invalid Edit Token")
			}
		}
		if ent.TeenantField != nil {
			var teenant int64 = value.FieldByIndex(ent.TeenantField.classIdx).Int()
			if teenant != int64(eData.teenant) {
				return fmt.Errorf("Invalid Teenant")
			}
		}
	}
	var txn *sql.Tx
	txn, e = conn.db.Begin()
	if e == nil {
		var stmt *sql.Stmt
		var res sql.Result

		var flds []interface{} = make([]interface{}, len(ent.Fields)-1)
		if eData.pk == 0 {
			stmt = txn.Stmt(ent.stmts.stmtInsert)
			res, e = stmt.Exec(flds...)
		} else {
			flds = append(flds, pk)
			stmt = txn.Stmt(ent.stmts.stmtUpdate)
			res, e = stmt.Exec(flds...)
		}
		if e == nil {
			if eData.pk == 0 {
				eData.pk, e = res.LastInsertId()
			}
		}
		if e == nil {
			e = txn.Commit()
		} else {
			txn.Rollback()
		}
	}

	return e
}
