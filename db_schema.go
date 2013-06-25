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

func (su *SchemaUpgrader) GetSchemaForTable(t *table) *TableSchema {
	var ts *TableSchema = new(TableSchema)
	ts.Name = t.tableName
	ts.Columns = make([]*ColumnSchema, len(t.fields))
	ts.Indice = make([]*IndexSchema, 0, 8)
	for i, f := range t.fields {
		cs := new(ColumnSchema)
		cs.Name = f.sqlName
		cs.Type = f.dataType
		cs.IsNull = f.isNullable
		cs.Precision = f.precision
		if f == t.primaryKey {
			ts.PrimaryKey = cs
		}
		ts.Columns[i] = cs

		if f == t.parentKey || f.isIndex {
			var is *IndexSchema = new(IndexSchema)
			is.Name = ""
			is.Column = cs
			is.IsUnique = false

			ts.Indice = append(ts.Indice, is)
		}
	}

	return ts
}

func (su *SchemaUpgrader) UpgradeEntity(ent *Entity) error {
	var e error
	tables := ent.Flatten()
	for _, t := range tables {
		var dbSchema *TableSchema
		var classSchema *TableSchema
		dbSchema, e = su.SqlDmlDriver.ReadTableSchema(t.tableName)
		classSchema = su.GetSchemaForTable(t)

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
					e = su.SqlDmlDriver.AlterTableAddColumn(t.tableName, fsc)
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
					e = su.SqlDmlDriver.AlterTableAddIndex(t.tableName, isc)
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