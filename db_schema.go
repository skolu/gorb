package gorb

import ()

type (
	DbSchemaUpgrader interface {
		GetVersion() int
		ReadTableSchema(tableName string) (*TableSchema, error)
		CreateTable(schema *TableSchema) error
		AlterTableAddColumn(tableName string, column *ColumnSchema) error
		AlterTableAddIndex(tableName string, index *IndexSchema) error
	}

	ColumnSchema struct {
		Name      string
		Type      DataType
		IsNull    bool
		Precision uint16
	}
	IndexSchema struct {
		Name     string
		Column   *ColumnSchema
		IsUnique bool
	}
	TableSchema struct {
		Name       string
		PrimaryKey *ColumnSchema
		ForeignKey *ColumnSchema
		Columns    []*ColumnSchema
		Indice     []*IndexSchema
	}
)

type (
	SchemaUpgrader struct {
		SqlDmlDriver DbSchemaUpgrader
	}
)

func (su *SchemaUpgrader) getSchemaForTable(t *Table) *TableSchema {
	var ts *TableSchema = new(TableSchema)
	ts.Name = t.TableName
	ts.Columns = make([]*ColumnSchema, len(t.Fields))
	ts.Indice = make([]*IndexSchema, 0, 8)
	for i, f := range t.Fields {
		cs := new(ColumnSchema)
		cs.Name = f.sqlName
		cs.Type = f.DataType
		cs.IsNull = f.isNullable
		cs.Precision = f.precision
		if f == t.PrimaryKey {
			ts.PrimaryKey = cs
		}
		ts.Columns[i] = cs

		if f.isIndex {
			var is *IndexSchema = new(IndexSchema)
			is.Name = ""
			is.Column = cs
			is.IsUnique = false

			ts.Indice = append(ts.Indice, is)
		}
	}

	return ts
}

func (su *SchemaUpgrader) GetSchemaForChild(child *ChildTable) *TableSchema {
	var t *Table = &((*child).Table)
	ts := su.getSchemaForTable(t)
	if child.PrimaryKey != child.ParentKey {
		for _, col := range ts.Columns {
			if col.Name == child.ParentKey.sqlName {
				var is *IndexSchema = new(IndexSchema)
				is.Name = ""
				is.Column = col
				is.IsUnique = false
				ts.Indice = append(ts.Indice, is)
				break
			}
		}
	}

	return ts
}

func (su *SchemaUpgrader) GetSchemaForEntity(entity *Entity) *TableSchema {
	var t *Table = &((*entity).Table)
	ts := su.getSchemaForTable(t)
	if entity.TeenantField != nil {
		for _, col := range ts.Columns {
			if col.Name == entity.TeenantField.sqlName {
				var is *IndexSchema = new(IndexSchema)
				is.Name = ""
				is.Column = col
				is.IsUnique = false
				ts.Indice = append(ts.Indice, is)
				break
			}
		}
	}

	return ts
}

func (su *SchemaUpgrader) UpgradeEntity(ent *Entity) error {

	children := ent.FlattenChildren()
	var tables []*TableSchema = make([]*TableSchema, len(children)+1)
	tables[0] = su.GetSchemaForEntity(ent)
	for i, child := range children {
		tables[i+1] = su.GetSchemaForChild(child)
	}

	for _, classSchema := range tables {
		dbSchema, e := su.SqlDmlDriver.ReadTableSchema(classSchema.Name)

		if e == nil { // upgrade
			for _, fsc := range classSchema.Columns {
				found := false
				for _, fsdb := range dbSchema.Columns {
					if fsc.Name == fsdb.Name {
						found = true
						break
					}
				}
				if !found {
					e = su.SqlDmlDriver.AlterTableAddColumn(dbSchema.Name, fsc)
					if e != nil {
						return e
					}
				}
			}

			for _, isc := range classSchema.Indice {
				found := false
				for _, isdb := range dbSchema.Indice {
					if isc.Column.Name == isdb.Column.Name {
						found = true
						break
					}
				}
				if !found {
					e = su.SqlDmlDriver.AlterTableAddIndex(dbSchema.Name, isc)
					if e != nil {
						return e
					}
				}
			}
		} else { // create
			e = su.SqlDmlDriver.CreateTable(classSchema)
			if e != nil {
				return e
			}
		}
	}

	return nil
}
