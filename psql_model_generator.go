package crud

import (
"bytes"
"database/sql"
"errors"
"fmt"
"os"
"os/exec"
"strings"
"text/template"
"regexp"
)

var dbo DSLer

// Column information
type Column struct {
	Name         string  // DB column name
	ModelName    string  // Model name
	Default      *string // DB default value
	IsNullable   bool    // DB is nullable
	DataType     string  // DB column type
	ModelType    string  // Model type
	Schema       string  // DB Schema
	Table        string  // DB table
	Sequence     *string // DB sequence
	IsPrimaryKey bool    // DB is primary key
	Json         string  // Model Json name
	Import       string  // Model Import custom lib
}

// Array of columns
type Columns []Column

// Parse Row
func parseColumnRow(rows *sql.Rows) (*Column, error) {
	column := Column{}
	err := rows.Scan(
		&column.Name,
		&column.DataType,
		&column.IsNullable,
		&column.Schema,
		&column.Table,
		&column.IsPrimaryKey,
		&column.Default,
		&column.Sequence,
	)

	if err != nil {
		return nil, err
	}

	return &column, nil
}

// Get table columns from db
func GetTableColumns(schema string, table string) (*Columns, error) {
	query := fmt.Sprintf(`
SELECT a.attname                                                                       AS column_name,
       format_type(a.atttypid, a.atttypmod)                                            AS data_type,
       CASE WHEN a.attnotnull THEN FALSE ELSE TRUE END                                 AS is_nullable,
       s.nspname                                                                       AS schema,
       t.relname                                                                       AS table,
       CASE WHEN max(i.indisprimary::int)::BOOLEAN THEN TRUE ELSE FALSE END            AS is_primary,
       ic.column_default,
       pg_get_serial_sequence(ic.table_schema || '.' || ic.table_name, ic.column_name) AS sequence
FROM pg_attribute a
       JOIN pg_class t ON a.attrelid = t.oid
       JOIN pg_namespace s ON t.relnamespace = s.oid
       LEFT JOIN pg_index i ON i.indrelid = a.attrelid AND a.attnum = ANY (i.indkey)
       LEFT JOIN information_schema.columns AS ic
                 ON ic.column_name = a.attname AND ic.table_name = t.relname AND ic.table_schema = s.nspname
WHERE a.attnum > 0
  AND NOT a.attisdropped
  AND s.nspname = '%s'
  AND t.relname = '%s'
GROUP BY a.attname, a.atttypid, a.atttypmod, a.attnotnull, s.nspname, t.relname, ic.column_default,
         ic.table_schema, ic.table_name, ic.column_name, a.attnum
ORDER BY a.attnum;
`, schema, table)

	rows, err := dbo.Query(query)
	if err != nil {
		return nil, err
	}

	var columns Columns
	var hasPrimary bool

	for rows.Next() {
		column, err := parseColumnRow(rows)
		if err != nil {
			return nil, err
		}

		name, err := toCamelCase(column.Name, true)
		if err != nil {
			return nil, err
		}

		json, err := toCamelCase(column.Name, false)
		if err != nil {
			return nil, err
		}

		column.ModelName = name
		column.Json = fmt.Sprintf(`%cjson:"%s"%c`, '`', json, '`')

		switch {
		case column.DataType == "bigint":
			column.ModelType = "int64"
		case column.DataType == "integer":
			column.ModelType = "int"
		case column.DataType == "text":
			column.ModelType = "string"
		case column.DataType == "double precision":
			column.ModelType = "float64"
		case column.DataType == "boolean":
			column.ModelType = "bool"
		case column.DataType == "ARRAY":
			column.ModelType = "[]interface{}"
		case column.DataType == "json":
			column.ModelType = "json.RawMessage"
			column.Import = `"encoding/json"`
		case column.DataType == "smallint":
			column.ModelType = "int"
		case column.DataType == "date":
			column.ModelType = "time.Time"
			column.Import = `"time"`
		case strings.Contains(column.DataType, "character varying"):
			column.ModelType = "string"
		case strings.Contains(column.DataType, "numeric"):
			column.ModelType = "float32"
		case column.DataType == "uuid":
			column.ModelType = "string"
		case column.DataType == "jsonb":
			column.ModelType = "struct{}"
		case column.DataType == "uuid[]":
			column.ModelType = "[]string"
		case strings.Contains(column.DataType, "timestamp"):
			column.ModelType = "time.Time"
			column.Import = `"time"`
		default:
			return nil, errors.New(fmt.Sprintf("unknown column type: %s", column.DataType))
		}

		if column.IsNullable {
			column.ModelType = "*" + column.ModelType
		}

		if column.IsPrimaryKey == true {
			hasPrimary = true
		}

		columns = append(columns, *column)
	}

	// column named id will be primary if no primary key
	if !hasPrimary {
		for key, column := range columns {
			if column.Name == "id" {
				columns[key].IsPrimaryKey = true
				break
			}
		}
	}

	return &columns, nil
}

// Start script
func MakeModel(db DSLer, path string, schema string, table string) error {
	if table == "" {
		return errors.New("table name is empty")
	}
	dbo = db
	return CreateModel(schema, table, path)
}

// Create file in os
func CreateModelFile(schema string, table string, path string) (*os.File, string, error) {
	fileName := fmt.Sprintf("%s", table)
	folderPath := fmt.Sprintf(path)
	err := os.MkdirAll(folderPath, os.ModePerm)
	if err != nil {
		return nil, "", err
	}
	filePath := fmt.Sprintf("%s/%s.go", folderPath, fileName)

	f, err := os.Create(filePath)
	if err != nil {
		return nil, "", err
	}

	return f, filePath, nil
}

// Get model file header
func getModelHeader(imports []string) (bytes.Buffer, error) {
	baseImports := []string{
		`"strings"`,
		`"database/sql"`,
		`"errors"`,
		`"fmt"`,
		`"github.com/dimonrus/godb"`,
		`"github.com/cadyrov/gocrud"`,
		`"github.com/cadyrov/govalidation"`,
	}
	imports = append(imports, baseImports...)
	t := `package models
import ({{ range $key, $import := .Imports }}{{ $import }}
	{{ end }}
)
`
	var buf bytes.Buffer

	tml := template.Must(template.New("").Parse(t))
	err := tml.Execute(&buf, struct {
		Imports []string
	}{
		Imports: imports,
	})

	return buf, err
}

// Get model struct
func getModelStruct(model string, table string, columns Columns) (bytes.Buffer, error) {
	t := `type {{ .Model }} struct { {{ range $key, $column := .Columns }}
	{{ $column.ModelName }} {{ $column.ModelType }} {{ $column.Json }}{{ end }}
}
`
	return ParseCrudMethodTemplate(t, model, table, columns)
}

// Get model parser
func getModelParser(model string, table string, columns Columns) (bytes.Buffer, error) {
	t := `
// validate
func (m *{{ .Model }}) Validate() (err error) {
	return nil
}

func (m *{{ .Model }}) PrimaryKey() (names []string, attributeLinks []interface{}) {
	names = append(names {{ range $key, $column := .Columns }}{{ if $column.IsPrimaryKey }} , "{{ $column.Name }}" {{ end }} {{ end }})
	attributeLinks = append(attributeLinks {{ range $key, $column := .Columns }}{{ if $column.IsPrimaryKey }} , &m.{{ $column.ModelName }}{{ end }}{{ end }})
	return
}


func (m *{{ .Model }}) Sequences() (names []string, attributeLinks []interface{}) {
	names = append(names {{ range $key, $column := .Columns }}{{ if $column.Sequence }} , "{{ $column.Name }}" {{ end }} {{ end }})
	attributeLinks = append(attributeLinks {{ range $key, $column := .Columns }}{{ if $column.Sequence }} , &m.{{ $column.ModelName }}{{ end }}{{ end }})
	return
}



//return table name of {{ .Model }}
func (m *{{ .Model }}) TableName() string {
	return "{{.Table}}"
}

`
	return ParseCrudMethodTemplate(t, model, table, columns)
}

// Columns
func getColumns(model string, table string, columns Columns) (bytes.Buffer, error) {
	t := `// Model columns
	func (m *{{ .Model }}) Columns() (names []string, attributeLinks []interface{}) {
		names = append(names {{ range $key, $column := .Columns }}{{ if not ($column.IsPrimaryKey) }} , "{{ $column.Name }}" {{ end }} {{ end }})
		attributeLinks = append(attributeLinks {{ range $key, $column := .Columns }}{{ if not ($column.IsPrimaryKey) }} , &m.{{ $column.ModelName }}{{ end }}{{ end }})
		return
	}
`
	return ParseCrudMethodTemplate(t, model, table, columns)
}

// Column map
func getValues(model string, table string, columns Columns) (bytes.Buffer, error) {
	t := `
`
	return ParseCrudMethodTemplate(t, model, table, columns)
}

// Parse Template
func ParseCrudMethodTemplate(t string, model string, table string, columns Columns) (bytes.Buffer, error) {
	var buf bytes.Buffer
	funcMap := template.FuncMap{
		"inc": func(i int) int {
			return i + 1
		},
		"system": func(column Column) bool {
			return existsInArrayString(column.Name, []string{"updated_at", "created_at", "deleted_at"}) ||
				(column.IsPrimaryKey && column.Sequence != nil)
		},
	}

	tml := template.Must(template.New("").Funcs(funcMap).Parse(t))
	err := tml.Execute(&buf, struct {
		Model   string
		Table   string
		Columns Columns
	}{
		Model:   model,
		Table:   table,
		Columns: columns,
	})

	return buf, err
}

// Get model loader
func getModelLoader(model string, table string, columns Columns) (bytes.Buffer, error) {
	t := `
// Load {{ .Model }}
func (m *{{ .Model }}) Load(d crud.DSLer) (ok bool, err error) {
	ok, err = crud.Load(d, m)
	return
}
`
	return ParseCrudMethodTemplate(t, model, table, columns)
}

// Get model deleter
func getModelDeleter(model string, table string, columns Columns) (bytes.Buffer, error) {
	t := `
// Delete {{ .Model }}
func (m *{{ .Model }}) Delete(d crud.DSLer) error {
	return crud.Delete(d, m)
}
`
	return ParseCrudMethodTemplate(t, model, table, columns)
}

// Get model saver
func getModelSaver(model string, table string, columns Columns) (bytes.Buffer, error) {

	t := `
// Save {{ .Model }}
func (m *{{ .Model }}) Save(d crud.DSLer) error {
	return crud.Save(d, m)
}
`
	return ParseCrudMethodTemplate(t, model, table, columns)
}

// Get model searcher
func getModelSearcher(model string, table string, columns Columns) (bytes.Buffer, error) {
	t := `// Search by filer
func (m *{{ .Model }}) Search (q crud.DSLer, filter godb.SqlFilter) ([]{{ .Model }}, {{ $index := 0 }}{{ range $key, $column := .Columns }}{{ if $column.IsPrimaryKey }}{{ if $index }}, {{ end }}{{ $index = inc $index }}[]{{ $column.ModelType }}{{ end }}{{ end }}, error) {
	nm,_ := m.Columns()
	names, _ := m.PrimaryKey()
	names = append(names, nm...)
	query := fmt.Sprintf("SELECT " + strings.Join(names, ",") + " FROM {{ .Table }} %s", filter.GetWithWhere())
	rows, err := q.Query(query, filter.GetArguments()...)
	{{ range $key, $column := .Columns }}{{ if $column.IsPrimaryKey }}
	entity{{ $column.ModelName }}s := make([]{{ $column.ModelType }}, 0){{ end }}{{ end }}
	if err != nil {
		return nil, {{ $index := 0 }}{{ range $key, $column := .Columns }}{{ if $column.IsPrimaryKey }}{{ if $index }}, {{ end }}{{ $index = inc $index }}entity{{ $column.ModelName }}s{{ end }}{{ end }}, err
	}
	defer rows.Close()
	var result = []{{ .Model }}{}
	for rows.Next() {
		row := &{{ .Model }}{}
		_, sc := row.Columns()
		_, scans := row.PrimaryKey()

		scans = append(scans, sc...)
		err = rows.Scan(scans...)
		if err != nil {
			return result, {{ $index := 0 }}{{ range $key, $column := .Columns }}{{ if $column.IsPrimaryKey }}{{ if $index }}, {{ end }}{{ $index = inc $index }}entity{{ $column.ModelName }}s{{ end }}{{ end }}, err
		}
		{{ range $key, $column := .Columns }}{{ if $column.IsPrimaryKey }}
		entity{{ $column.ModelName }}s = append(entity{{ $column.ModelName }}s, row.{{ $column.ModelName}}){{ end }}{{ end }}
		result = append(result, *row)
	}
	return result, {{ $index := 0 }}{{ range $key, $column := .Columns }}{{ if $column.IsPrimaryKey }}{{ if $index }}, {{ end }}{{ $index = inc $index }}entity{{ $column.ModelName }}s{{ end }}{{ end }}, nil
}
`

	return ParseCrudMethodTemplate(t, model, table, columns)
}

// Create Model File
func CreateModel(schema string, table string, path string) error {
	var tableExists bool
	var imports []string

	columns, err := GetTableColumns(schema, table)
	if err != nil {
		return err
	}

	for _, column := range *columns {
		tableExists = true
		imports = appendUniqueString(imports, column.Import)
	}

	// Check if table not exist or no columns
	if !tableExists {
		return errors.New(fmt.Sprintf("table (%s) is not exists", table))
	}

	// Name of the model
	modelName, err := toCamelCase(table, true)
	if err != nil {
		return err
	}

	//Table name with schema
	tableName := fmt.Sprintf("%s.%s", schema, table)

	//create file
	file, path, err := CreateModelFile(schema, table, path)
	if err != nil {
		return err
	}

	defer file.Close()

	// Get header
	header, err := getModelHeader(imports)
	if err != nil {
		return err
	}

	model, err := getModelStruct(modelName, tableName, *columns)
	if err != nil {
		return err
	}

	cols, err := getColumns(modelName, tableName, *columns)
	if err != nil {
		return err
	}

	vals, err := getValues(modelName, tableName, *columns)
	if err != nil {
		return err
	}

	parser, err := getModelParser(modelName, tableName, *columns)
	if err != nil {
		return err
	}

	loader, err := getModelLoader(modelName, tableName, *columns)
	if err != nil {
		return err
	}

	deleter, err := getModelDeleter(modelName, tableName, *columns)
	if err != nil {
		return err
	}

	saver, err := getModelSaver(modelName, tableName, *columns)
	if err != nil {
		return err
	}

	searcher, err := getModelSearcher(modelName, tableName, *columns)
	if err != nil {
		return err
	}

	_, err = file.Write(header.Bytes())
	if err != nil {
		return err
	}

	_, err = file.Write(model.Bytes())
	if err != nil {
		return err
	}

	_, err = file.Write(cols.Bytes())
	if err != nil {
		return err
	}

	_, err = file.Write(vals.Bytes())
	if err != nil {
		return err
	}

	_, err = file.Write(parser.Bytes())
	if err != nil {
		return err
	}

	_, err = file.Write(loader.Bytes())
	if err != nil {
		return err
	}

	_, err = file.Write(deleter.Bytes())
	if err != nil {
		return err
	}

	_, err = file.Write(saver.Bytes())
	if err != nil {
		return err
	}

	_, err = file.Write(searcher.Bytes())
	if err != nil {
		return err
	}

	cmd := exec.Command("go", "fmt", path)
	err = cmd.Run()
	if err != nil {
		return err
	}

	return nil
}

func toCamelCase(str string, isFirstTitle bool) (string, error) {
	var result string
	reg, _ := regexp.Compile(`[a-z0-9]+(_[a-z0-9]+)*`)
	regs, _ := regexp.Compile(`\s*`)
	matches := reg.FindStringSubmatch(strings.ToLower(str))

	if len(matches) > 0 {
		keys := strings.Split(matches[0], "_")
		var titled string
		if isFirstTitle {
			titled = strings.Join(keys, " ")
		} else {
			titled = strings.Join(keys[1:], " ")
		}
		str := reg.ReplaceAllStringFunc(titled, func(s string) string {
			return strings.Title(s)
		})
		str = regs.ReplaceAllString(str, "")
		if !isFirstTitle {
			result = keys[0] + str
		} else {
			result = str
		}
	} else {
		return "", errors.New("wrong string passed")
	}
	return result, nil
}


func appendUniqueString(slice []string, i string) []string {
	for _, el := range slice {
		if el == i {
			return slice
		}
	}
	return append(slice, i)
}

func existsInArrayString(value string, slice []string) bool {
	for _, item := range slice {
		if item == value {
			return true
		}
	}
	return false
}