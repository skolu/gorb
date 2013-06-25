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
		EntityGet(entity interface{}, pk interface{}) (bool, error)
		EntityPut(entity interface{}) (bool, error)
		EntityDelete(eType reflect.Type, pk interface{}) (bool, error)
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
	res, err := e.extractGorbSchema(class, []int{})
	if res {
		res, err = e.check()
		if res {
			if mgr.Entities == nil {
				mgr.Entities = make(map[reflect.Type]*Entity, 16)
			}
			if mgr.names == nil {
				mgr.names = make(map[string]reflect.Type, 16)
			}
			tables := e.Flatten()
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

func (mgr *GorbManager) SetDB(db *sql.DB) (bool, error) {
	mgr.db = db

	for _, ent := range mgr.Entities {
		res, e := ent.createStatements(db, []*Table{})
		if !res {
			return res, e
		}
	}

	return true, nil
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

func (conn *GorbManager) EntityPut(entity interface{}) (bool, error) {
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

	return nil, nil
}
