package crud

import (
	"database/sql"
	"errors"
	"strconv"
	"strings"
)

//ActiveRecord analog
type Crudable interface {
	Columns() (names []string, attributeLinks []interface{})
	Primarykey() (name string, attributeLink interface{})
	TableName() string
	Validate() (err error)
}

//SQL tx and dbo
type SQLer interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type Crud struct {
}

// SQL load Query
func GetLoadQuery(m Crudable) string {
	columns := getColumnNames(m)
	primary, _ := m.Primarykey()
	table := m.TableName()
	return "SELECT " + columns + " FROM " + table + " WHERE " + primary + " = $1;"
}

func getColumnNames(m Crudable) string {
	names := make([]string, 0)
	primary, _ := m.Primarykey()
	names = append(names, primary)
	nms, _ := m.Columns()
	names = append(names, nms...)
	return strings.Join(names, ", ")
}

func getScans(m Crudable) (values []interface{}) {
	_, attributeLink := m.Primarykey()
	values = append(values, attributeLink)
	_, attributeLinks := m.Columns()
	values = append(values, attributeLinks...)
	return
}

func parse(rows *sql.Rows, m Crudable) (err error) {
	errScan := rows.Scan(getScans(m)...)
	err = errScan
	return
}

// Load model
func Load(dbo SQLer, m Crudable) (find bool, err error) {
	_, id := m.Primarykey()
	if primaryExists(id) {
		var iterator *sql.Rows
		iterator, errQuery := dbo.Query(GetLoadQuery(m), id)
		if errQuery != nil {
			err = errQuery
			return
		}
		defer iterator.Close()

		if iterator.Next() == false {
			return
		}

		err = parse(iterator, m)
		if err == nil {
			find = true
		}
		return
	} else {
		err = errors.New("no primary key specified, nothing for load")
	}
	return
}

// SQL delete Query
func getDeleteQuery(m Crudable) string {
	name, _ := m.Primarykey()
	return "DELETE FROM " + m.TableName() + " WHERE " + name + " = $1;"
}

// Delete method
func Delete(dbo SQLer, m Crudable) error {
	_, id := m.Primarykey()
	_, err := dbo.Exec(getDeleteQuery(m), id)
	return err
}

// SQL upsert Query
func getUpdateQuery(m Crudable) (query string, scans []interface{}) {
	idname, _ := m.Primarykey()
	cols, _ := m.Columns()
	updateCols := ""
	for i, colname := range cols {
		updateCols = updateCols + " " + colname + " = $" + strconv.Itoa(i+2)
		if i < (len(cols) - 1) {
			updateCols = updateCols + ", "
		}
	}

	query = `UPDATE ` + m.TableName() + ` SET ` + updateCols + `
		WHERE ` + idname + ` = $1
		RETURNING ` + getColumnNames(m) + `;`
	scans = getScans(m)
	return
}

func getSaveQuery(m Crudable) (query string, scans []interface{}) {
	names, scans := m.Columns()
	columns := strings.Join(names, ",")
	params := ""
	for i, _ := range names {
		params = params + " $" + strconv.Itoa(i+1)
		if i < (len(names) - 1) {
			params = params + ", "
		}
	}

	query = `INSERT INTO ` + m.TableName() + ` (` + columns + `) VALUES (` + params + `)
	RETURNING ` + getColumnNames(m) + `;`

	return
}

func getSaveQueryWithPrimary(m Crudable) (query string, insertions []interface{}) {
	columns := getColumnNames(m)
	insertions = getScans(m)
	params := ""
	for i, _ := range insertions {
		params = params + " $" + strconv.Itoa(i+1)
		if i < (len(insertions) - 1) {
			params = params + ", "
		}
	}

	query = `INSERT INTO ` + m.TableName() + ` (` + columns + `) VALUES (` + params + `)
	RETURNING ` + columns + `;`
	return
}

func getUpdateQueryWithPrimary(m Crudable) (query string, insertions []interface{}) {
	idname, primary := m.Primarykey()
	cols, _ := m.Columns()
	updateCols := " idname  = $" + strconv.Itoa(2)
	for i, colname := range cols {
		updateCols = updateCols + " " + colname + " = $" + strconv.Itoa(i+3)
		if i < (len(cols) - 1) {
			updateCols = updateCols + ", "
		}
	}

	query = `UPDATE ` + m.TableName() + ` SET ` + updateCols + `
		WHERE ` + idname + ` = $1
		RETURNING ` + getColumnNames(m) + `;`

	insertions = make([]interface{}, 0)
	insertions = append(insertions, primary, getScans(m))
	return
}

//Model saver method
func Save(dbo SQLer, m Crudable) (err error) {
	_, id := m.Primarykey()
	vErr := m.Validate()
	if vErr == nil {
		var qErr error
		if primaryExists(id) {
			query, insertions := getUpdateQuery(m)
			qErr = dbo.QueryRow(query, insertions...).Scan(getScans(m)...)
		} else {
			query, insertions := getSaveQuery(m)
			qErr = dbo.QueryRow(query, insertions...).Scan(getScans(m)...)
		}
		if qErr != nil {
			err = qErr
		}
	} else {
		err = vErr
	}
	return
}

//Model saver method
func SaveWithPrimary(dbo SQLer, m Crudable) (err error) {
	_, id := m.Primarykey()
	vErr := m.Validate()
	if vErr == nil {
		if primaryExists(id) {
			ok, err := Load(dbo, m)
			if err == nil {
				var qErr error
				if ok {
					query, insertions := getUpdateQueryWithPrimary(m)
					qErr = dbo.QueryRow(query, insertions...).Scan(getScans(m)...)
				} else {
					query, insertions := getSaveQueryWithPrimary(m)
					qErr = dbo.QueryRow(query, insertions...).Scan(getScans(m)...)
				}
				if qErr != nil {
					err = qErr
				}
			}
		}
	} else {
		err = vErr
	}
	return
}

func primaryExists(input interface{}) (ok bool) {
	if input == nil {
		return
	}
	switch input.(type) {
	case *int:
		x := *input.(*int)
		if x != 0 {
			ok = true
		}
	case *int64:
		x := *input.(*int64)
		if x != 0 {
			ok = true
		}
	case *int32:
		x := *input.(*int32)
		if x != 0 {
			ok = true
		}
	}
	return
}
