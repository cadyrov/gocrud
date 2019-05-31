package crud

import (
	"database/sql"
	"errors"
	"strconv"
	"strings"

	"github.com/dimonrus/godb"
)

type Crudable interface {
	Columns() (names []string, attributeLinks []interface{})
	Primarykey() (name string, attributeLink interface{})
	TableName() string
	Validate() (err error)
}

type Crud struct {
}

// SQL load Query
func (crud *Crud) GetLoadQuery(m Crudable) string {
	columns := crud.getColumnNames(m)
	primary, _ := m.Primarykey()
	table := m.TableName()
	return "SELECT " + columns + " FROM " + table + " WHERE " + primary + " = $1;"
}

func (crud *Crud) getColumnNames(m Crudable) string {
	names := make([]string, 0)
	primary, _ := m.Primarykey()
	names = append(names, primary)
	nms, _ := m.Columns()
	names = append(names, nms...)
	return strings.Join(names, ", ")
}

func (crud *Crud) getScans(m Crudable) (values []interface{}) {
	_, attributeLink := m.Primarykey()
	values = append(values, attributeLink)
	_, attributeLinks := m.Columns()
	values = append(values, attributeLinks...)
	return
}

func (crud *Crud) parse(rows *sql.Rows, m Crudable) (err error) {
	err = rows.Scan(crud.getScans(m)...)
	return
}

// Load model
func (crud *Crud) Load(dbo *godb.DBO, m Crudable) (find bool, err error) {
	_, id := m.Primarykey()
	if crud.primaryExists(id) {
		var iterator *sql.Rows
		iterator, err = dbo.Query(crud.GetLoadQuery(m), id)
		if err != nil {
			return
		}
		defer iterator.Close()

		if iterator.Next() == false {
			return
		}

		err = crud.parse(iterator, m)
		if err == nil {
			find = true
		}
		return
	} else {
		err = errors.New("no primary key specified, nothing for load")
	}
	return
}

// Load model
func (crud *Crud) LoadTx(tx *godb.SqlTx, m Crudable) (find bool, err error) {
	_, id := m.Primarykey()
	if crud.primaryExists(id) {
		var iterator *sql.Rows
		iterator, err = tx.Query(crud.GetLoadQuery(m), id)
		if err != nil {
			return
		}
		defer iterator.Close()

		if iterator.Next() == false {
			return
		}

		err = crud.parse(iterator, m)
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
func (crud *Crud) getDeleteQuery(m Crudable) string {
	name, _ := m.Primarykey()
	return "DELETE FROM " + m.TableName() + " WHERE " + name + " = $1;"
}

// Delete method
func (crud *Crud) Delete(dbo *godb.DBO, m Crudable) error {
	_, id := m.Primarykey()
	_, err := dbo.Exec(crud.getDeleteQuery(m), id)
	return err
}

//Delete with transactions
func (crud *Crud) DeleteTx(tx *godb.SqlTx, m Crudable) error {
	_, id := m.Primarykey()
	_, err := tx.Exec(crud.getDeleteQuery(m), id)
	return err
}

// SQL upsert Query
func (crud *Crud) getUpdateQuery(m Crudable) (query string, scans []interface{}) {
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
		RETURNING ` + crud.getColumnNames(m) + `;`
	scans = crud.getScans(m)
	return
}

func (crud *Crud) getSaveQuery(m Crudable) (query string, scans []interface{}) {
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
	RETURNING ` + crud.getColumnNames(m) + `;`

	return
}

func (crud *Crud) getSaveQueryWithPrimary(m Crudable) (query string, insertions []interface{}) {
	columns := crud.getColumnNames(m)
	insertions = crud.getScans(m)
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

func (crud *Crud) getUpdateQueryWithPrimary(m Crudable) (query string, insertions []interface{}) {
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
		RETURNING ` + crud.getColumnNames(m) + `;`

	insertions = make([]interface{}, 0)
	insertions = append(insertions, primary, crud.getScans(m))
	return
}

//Model saver method
func (crud *Crud) Save(dbo *godb.DBO, m Crudable) (err error) {
	_, id := m.Primarykey()
	err = m.Validate()
	if err == nil {
		if crud.primaryExists(id) {
			query, insertions := crud.getUpdateQuery(m)
			err = dbo.QueryRow(query, insertions...).Scan(crud.getScans(m)...)
		} else {
			query, insertions := crud.getSaveQuery(m)
			err = dbo.QueryRow(query, insertions...).Scan(crud.getScans(m)...)
		}
	}
	return
}

//Model saver method with transactions
func (crud *Crud) SaveTx(tx *godb.SqlTx, m Crudable) (err error) {
	_, id := m.Primarykey()

	err = m.Validate()
	if err == nil {
		if crud.primaryExists(id) {
			query, insertions := crud.getUpdateQuery(m)
			err = tx.QueryRow(query, insertions...).Scan(crud.getScans(m)...)
		} else {
			query, insertions := crud.getSaveQuery(m)
			err = tx.QueryRow(query, insertions...).Scan(crud.getScans(m)...)
		}
	}
	return
}

//Model saver method
func (crud *Crud) SaveWithPrimary(dbo *godb.DBO, m Crudable) (err error) {
	_, id := m.Primarykey()
	err = m.Validate()
	if err == nil {
		if crud.primaryExists(id) {
			ok, err := crud.Load(dbo, m)
			if err == nil {
				if ok {
					query, insertions := crud.getUpdateQueryWithPrimary(m)
					err = dbo.QueryRow(query, insertions...).Scan(crud.getScans(m)...)
				} else {
					query, insertions := crud.getSaveQueryWithPrimary(m)
					err = dbo.QueryRow(query, insertions...).Scan(crud.getScans(m)...)
				}
			}
		}
	}
	return
}

//Model saver method
func (crud *Crud) SaveWithPrimaryTx(tx *godb.SqlTx, m Crudable) (err error) {
	_, id := m.Primarykey()
	err = m.Validate()
	if err == nil {
		if crud.primaryExists(id) {
			ok, err := crud.LoadTx(tx, m)
			if err == nil {
				if ok {
					query, insertions := crud.getUpdateQueryWithPrimary(m)
					err = tx.QueryRow(query, insertions...).Scan(crud.getScans(m)...)
				} else {
					query, insertions := crud.getSaveQueryWithPrimary(m)
					err = tx.QueryRow(query, insertions...).Scan(crud.getScans(m)...)
				}
			}
		}
	}
	return
}

func (crud *Crud) primaryExists(input interface{}) (ok bool) {
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
