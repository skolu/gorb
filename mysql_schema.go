package gorb

import (
	"bytes"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type (
	MySqlSchemaUpgrader struct {
		Db         *sql.DB
		IsTestMode bool
	}
)

// MySql
func MySqlColumnType(columnType string) (dataType DataType, precision uint16) {
	columnType = strings.ToLower(columnType)
	dataType = Unsupported
	precision = 0

	if strings.HasPrefix(columnType, "varchar") {
		dataType = String
	}
	if strings.HasPrefix(columnType, "smallint") {
		dataType = Int32
	}
	if strings.HasPrefix(columnType, "int") {
		dataType = Int32
	}
	if strings.HasPrefix(columnType, "bigint") {
		dataType = Int64
	}
	if strings.HasPrefix(columnType, "tinyint") {
		dataType = Bool
	}
	if strings.HasPrefix(columnType, "bit") {
		dataType = Bool
	}
	if strings.HasPrefix(columnType, "char") {
		dataType = String
	}
	if strings.HasPrefix(columnType, "decimal") {
		dataType = Float
	}
	if strings.HasPrefix(columnType, "real") {
		dataType = Float
	}
	if strings.HasPrefix(columnType, "double") {
		dataType = Float
	}
	if strings.HasPrefix(columnType, "numeric") {
		dataType = Float
	}
	if strings.HasPrefix(columnType, "date") {
		dataType = DateTime
	}
	if strings.HasPrefix(columnType, "timestamp") {
		dataType = DateTime
	}
	if strings.HasPrefix(columnType, "datetime") {
		dataType = DateTime
	}
	if strings.HasSuffix(columnType, "blob") {
		dataType = Blob
	}
	if strings.HasPrefix(columnType, "text") {
		dataType = String
	}
	if strings.HasPrefix(columnType, "tinytext") {
		dataType = String
	}
	if strings.HasPrefix(columnType, "mediumtext") {
		dataType = String
	}
	if strings.HasPrefix(columnType, "longtext") {
		dataType = String
	}

	if dataType == String {
		re, e := regexp.Compile(".*\\((.+)\\).*")
		if e == nil {
			matches := re.FindStringSubmatch(columnType)
			if len(matches) > 1 {
				var i int64
				i, e = strconv.ParseInt(matches[1], 10, 16)
				if e == nil {
					precision = uint16(i)
				}
			}
		}
	}

	return
}

func MySqlColumnDefinition(col *ColumnSchema) string {
	var typeDef []string = make([]string, 3)
	typeDef[0] = col.Name

	switch col.Type {
	case Bool:
		typeDef[1] = "Bit"
	case Int32:
		typeDef[1] = "Integer"
	case Int64:
		typeDef[1] = "Bigint"
	case Float:
		typeDef[1] = "Double"
	case String:
		if col.Precision > 0 {
			typeDef[1] = fmt.Sprintf("Varchar(%d)", col.Precision)
		} else {
			typeDef[1] = "Text"
		}
	case DateTime:
		typeDef[1] = "Timestamp"
	case Blob:
		typeDef[1] = "Blob"

	}
	if col.IsNull {
		typeDef[2] = "Null"
	} else {
		typeDef[2] = "Not Null"
	}
	return strings.Join(typeDef, " ")
}

func (u *MySqlSchemaUpgrader) GetVersion() int {
	return -1
}

func (u *MySqlSchemaUpgrader) ReadTableSchema(tableName string) (*TableSchema, error) {
	if u.Db == nil {
		return nil, fmt.Errorf("MySqlShemaUpgrade: Connection has not been set")
	}

	var rows *sql.Rows
	var e error

	//| Field       | Type                | Null | Key | Default             | Extra                       |
	rows, e = u.Db.Query(fmt.Sprintf("Show Columns From %s;", tableName))
	if e != nil {
		return nil, e
	}
	var tableSchema *TableSchema = new(TableSchema)
	var cs *ColumnSchema
	tableSchema.Name = tableName

	tableSchema.Columns = make([]*ColumnSchema, 0, 32)
	for rows.Next() {
		cs = new(ColumnSchema)
		var columnType, columnNull, columnKey string
		var columnDefault, columnExtra *string

		e = rows.Scan(&cs.Name, &columnType, &columnNull, &columnKey, &columnDefault, &columnExtra)
		if e != nil {
			return nil, e
		}
		if len(columnKey) > 0 {
			switch columnKey {
			case "PRI":
				tableSchema.PrimaryKey = cs
			}
		}
		cs.Type, cs.Precision = MySqlColumnType(columnType)

		cs.IsNull, e = strconv.ParseBool(columnNull)
		if e != nil {
			if columnNull == "YES" {
				cs.IsNull = true
			}
		}
		tableSchema.Columns = append(tableSchema.Columns, cs)
	}

	rows, e = u.Db.Query(fmt.Sprintf("Show Index From %s Where Seq_In_Index=1;", tableName))
	if e != nil {
		return nil, e
	}
	//| Table | Non_unique | Key_name | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment |
	tableSchema.Indice = make([]*IndexSchema, 0, 32)
	for rows.Next() {
		var tabName, columnName string
		var seqNo int
		var skip *string
		is := new(IndexSchema)
		e = rows.Scan(&tabName, &is.IsUnique, &is.Name, &seqNo, &columnName, &skip, &skip, &skip, &skip, &skip, &skip, &skip, &skip)
		if e != nil {
			return nil, e
		}
		var col *ColumnSchema
		for _, col = range tableSchema.Columns {
			if col.Name == columnName {
				is.Column = col
				break
			}
		}
		if is.Column != nil {
			if is.Column != tableSchema.PrimaryKey {
				tableSchema.Indice = append(tableSchema.Indice, is)
			}
		}
	}

	return tableSchema, nil
}

func (u *MySqlSchemaUpgrader) CreateTable(schema *TableSchema) error {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("Create Table %s (\n", schema.Name))
	for _, col := range schema.Columns {
		if col == schema.PrimaryKey {
			buffer.WriteString(fmt.Sprintf("\t%s %s,\n", col.Name, "SERIAL"))
		} else {
			buffer.WriteString(fmt.Sprintf("\t%s,\n", MySqlColumnDefinition(col)))
		}
	}

	buffer.WriteString(fmt.Sprintf("\tPrimary Key(%s)\n);", schema.PrimaryKey.Name))

	var query string = buffer.String()
	if u.IsTestMode {
		fmt.Println(query)
	} else {
		_, e := u.Db.Exec(query)
		if e != nil {
			return e
		}
	}

	for _, idx := range schema.Indice {
		buffer.Reset()

		if len(idx.Name) == 0 {
			idx.Name = fmt.Sprintf("%s_%s_IDX", strings.ToUpper(schema.Name), strings.ToUpper(idx.Column.Name))
		}

		buffer.WriteString("Create")
		if idx.IsUnique {
			buffer.WriteString(" Unique")
		}
		buffer.WriteString(" Index")

		buffer.WriteString(fmt.Sprintf(" %s On %s (%s);", idx.Name, schema.Name, idx.Column.Name))

		var query string = buffer.String()
		if u.IsTestMode {
			fmt.Println(query)
		} else {
			_, e := u.Db.Exec(query)
			if e != nil {
				return e
			}
		}
	}

	return nil
}

func (u *MySqlSchemaUpgrader) AlterTableAddColumn(tableName string, column *ColumnSchema) error {
	var buffer bytes.Buffer

	buffer.WriteString(fmt.Sprintf("Alter Table %s Add Column %s;", tableName, MySqlColumnDefinition(column)))

	var query string = buffer.String()
	if u.IsTestMode {
		fmt.Println(query)
	} else {
		_, e := u.Db.Exec(query)
		if e != nil {
			return e
		}
	}
	return nil
}

func (u *MySqlSchemaUpgrader) AlterTableAddIndex(tableName string, index *IndexSchema) error {
	var buffer bytes.Buffer

	if len(index.Name) == 0 {
		index.Name = fmt.Sprintf("%s_%s_IDX", strings.ToUpper(tableName), strings.ToUpper(index.Column.Name))
	}

	buffer.WriteString(fmt.Sprintf("Alter Table %s Add Index %s (%s);", tableName, index.Name, index.Column.Name))

	var query string = buffer.String()
	if u.IsTestMode {
		fmt.Println(query)
	} else {
		_, e := u.Db.Exec(query)
		if e != nil {
			return e
		}
	}

	return nil
}
