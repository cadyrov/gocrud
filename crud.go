package crud

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/cadyrov/govalidation"
	"strconv"
	"strings"
)

//ActiveRecord analog
type Cruder interface {
	Columns() (names []string, attributeLinks []interface{})
	PrimaryKey() (names []string, attributeLinks []interface{})
	Sequences() (names []string, attributeLinks []interface{})
	TableName() string
	Validate() (err error)
}

//SQL tx and dbo
type DSLer interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type Crud struct {
}

// SQL load Query
func GetLoadQuery(m Cruder) string {
	columns := columnNames(m)
	table := m.TableName()
	sql, _ := getSqlPrimary(m, 0)
	return "SELECT " + columns + " FROM " + table + " WHERE " + sql + " ;"
}

func getSqlPrimary(m Cruder, cnt int) (sql string, count int) {
	count = cnt
	sql = ""
	nms, _ := m.PrimaryKey()
	for key, value := range nms {
		nm := value
		count++
		if key == 0 {
			sql += " " + nm + " = " + fmt.Sprintf("$%v", count) + " "
		} else {
			sql += " and " + nm + " = " + fmt.Sprintf("$%v", count) + " "
		}
	}
	return
}

func columnNames(m Cruder) string {
	names := make([]string, 0)
	primary, _ := m.PrimaryKey()
	names = append(names, primary...)
	nms, _ := m.Columns()
	names = append(names, nms...)
	return strings.Join(names, ", ")
}

func scans(m Cruder) (values []interface{}) {
	_, attributeLink := m.PrimaryKey()
	values = append(values, attributeLink...)
	_, attributeLinks := m.Columns()
	values = append(values, attributeLinks...)
	return
}

func insertionColumns(m Cruder) (names []string, attributeLinks []interface{}) {
	prNames, prLinks := m.PrimaryKey()
	for key, value := range prNames {
		if !inSequense(m, value) {
			nm := value
			attrLink := prLinks[key]
			names = append(names, nm)
			attributeLinks = append(attributeLinks, attrLink)
		}
	}
	stNames, stLinks := m.Columns()
	names = append(names, stNames...)
	attributeLinks = append(attributeLinks, stLinks...)
	return
}

func inSequense(m Cruder, name string) bool {
	names, _ := m.Sequences()
	for _, value := range names {
		if value == name {
			return true
		}
	}
	return false
}

func parse(rows *sql.Rows, m Cruder) (err error) {
	errScan := rows.Scan(scans(m)...)
	err = errScan
	return
}

// Load model
func Load(dbo DSLer, m Cruder) (find bool, err error) {
	_, idlinks := m.PrimaryKey()
	if primaryExists(idlinks) {
		var iterator *sql.Rows
		iterator, errQuery := dbo.Query(GetLoadQuery(m), idlinks...)
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
func getDeleteQuery(m Cruder) string {
	sql, _ := getSqlPrimary(m, 0)
	return "DELETE FROM " + m.TableName() + " WHERE " + sql + " ;"
}

// Delete method
func Delete(dbo DSLer, m Cruder) error {
	_, idlinks := m.PrimaryKey()
	_, err := dbo.Exec(getDeleteQuery(m), idlinks...)
	return err
}

// SQL upsert Query
func getUpdateQuery(m Cruder) (query string, insertions []interface{}) {
	_, attr := m.PrimaryKey()
	insertions = append(insertions, attr...)
	cols, ins := insertionColumns(m)
	insertions = append(insertions, ins...)
	sqlPrm, iStrt := getSqlPrimary(m, 0)
	updateCols := ""
	for i, colname := range cols {
		updateCols = updateCols + " " + colname + " = $" + strconv.Itoa(i+1+iStrt)
		if i < (len(cols) - 1) {
			updateCols = updateCols + ", "
		}
	}

	query = `UPDATE ` + m.TableName() + ` SET ` + updateCols + `
		WHERE ` + sqlPrm + ` 
		RETURNING ` + columnNames(m) + `;`
	return
}

func getSaveQuery(m Cruder) (query string, insertions []interface{}) {
	names, insertions := insertionColumns(m)
	columns := strings.Join(names, ",")
	params := ""
	for i, _ := range names {
		params = params + " $" + strconv.Itoa(i+1)
		if i < (len(names) - 1) {
			params = params + ", "
		}
	}

	query = `INSERT INTO ` + m.TableName() + ` (` + columns + `) VALUES (` + params + `)
	RETURNING ` + columnNames(m) + `;`

	return
}

//Model saver method
func Save(ds DSLer, m Cruder) (err error) {
	if isUpdate(m) {
		err = Update(ds,m)
	} else {
		err = Create(ds,m)
	}
	return
}

func Create(ds DSLer, m Cruder) (err error) {
	if err = m.Validate(); err == nil {
		query, insertions := getSaveQuery(m)
		err = ds.QueryRow(query, insertions...).Scan(scans(m)...)
	}
	return
}

func Update(ds DSLer, m Cruder) (err error) {
	if err = m.Validate(); err == nil {
		query, insertions := getUpdateQuery(m)
		err = ds.QueryRow(query, insertions...).Scan(scans(m)...)
	}
	return
}


func isUpdate(m Cruder) (ok bool) {
	_, attrLink := m.Sequences()
	if len(attrLink) == 0 {
		return
	}
	for _, value := range attrLink {
		err := validation.Validate(value, validation.Required)
		if err != nil {
			ok = false
			return
		}
	}
	return true
}

func primaryExists(input []interface{}) (ok bool) {
	if input == nil {
		return
	}
	ok = true
	for _, value := range input {
		err := validation.Validate(value, validation.Required)
		if err != nil {
			ok = false
			return
		}
	}
	return
}
