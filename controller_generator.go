package crud

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"text/template"
)

func MakeController(path string, modelPath string, modelName string) (err error) {
	upperModel := strings.Title(modelName)
	// create file
	file, _, err := CreateControllerFile(path, modelName)
	if err != nil {
		return
	}
	defer file.Close()

	// header
	buf, err := controllerHeader(modelPath)
	if err != nil {
		return
	}
	if _, errs := file.Write(buf.Bytes()); errs != nil {
		err = errs
		return
	}
	// forms
	buf, err = controllerForm(upperModel)
	if err != nil {
		return
	}
	if _, errs := file.Write(buf.Bytes()); errs != nil {
		err = errs
		return
	}

	// returnedSlice
	buf, err = controllerReturnedSlice(upperModel)
	if err != nil {
		return
	}
	if _, errs := file.Write(buf.Bytes()); errs != nil {
		err = errs
		return
	}
	// create
	buf, err = controllerCreate(upperModel)
	if err != nil {
		return
	}
	if _, errs := file.Write(buf.Bytes()); errs != nil {
		err = errs
		return
	}

	// read
	buf, err = controllerRead(upperModel)
	if err != nil {
		return
	}
	if _, errs := file.Write(buf.Bytes()); errs != nil {
		err = errs
		return
	}

	// find
	buf, err = controllerFind(upperModel)
	if err != nil {
		return
	}
	if _, errs := file.Write(buf.Bytes()); errs != nil {
		err = errs
		return
	}

	// udpate
	buf, err = controllerUpdate(upperModel)
	if err != nil {
		return
	}
	if _, errs := file.Write(buf.Bytes()); errs != nil {
		err = errs
		return
	}
	// delete
	buf, err = controllerDelete(upperModel)
	if err != nil {
		return
	}
	if _, errs := file.Write(buf.Bytes()); errs != nil {
		err = errs
		return
	}
	// validate and services
	buf, err = controllerValidate(upperModel)
	if err != nil {
		return
	}
	if _, errs := file.Write(buf.Bytes()); errs != nil {
		err = errs
		return
	}
	// write to file
	return
}

// Create file in os
func CreateControllerFile(path string, fileName string) (*os.File, string, error) {
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

func controllerHeader(modelPath string) (buf bytes.Buffer, err error) {
	imports := []string{}
	baseImports := []string{
		`"github.com/dimonrus/porterr"`,
		`"fmt"`,
		`"` + modelPath + `"`,
	}
	imports = append(imports, baseImports...)
	t := `
package core
import (
	{{ range $key, $import := .Imports }}{{ $import }}
	{{ end }})
`

	tml := template.Must(template.New("").Parse(t))
	err = tml.Execute(&buf, struct {
		Imports []string
	}{
		Imports: imports,
	})
	return
}

func controllerForm(modelName string) (buf bytes.Buffer, err error) {
	t := `
type {{.Import}}Form struct {
	*models.{{.Import}}
}

type {{.Import}}SearchForm struct {
	Id int64 
	Name string
}
	`
	tml := template.Must(template.New("").Parse(t))
	err = tml.Execute(&buf, struct {
		Import string
	}{
		Import: modelName,
	})
	return
}

func controllerReturnedSlice(modelName string) (buf bytes.Buffer, err error) {
	t := `
type {{.Import}}ResultSet []{{.Import}}Form
	`
	tml := template.Must(template.New("").Parse(t))
	err = tml.Execute(&buf, struct {
		Import string
	}{
		Import: modelName,
	})
	return
}

func controllerRead(modelName string) (buf bytes.Buffer, err error) {
	t := `
//load {{.Import}}
func (f *{{.Import}}Form) Load() (result *{{.Import}}Form, e porterr.IError) {
	result = f
	db := base.App.GetBaseDb()
	if _, err := f.{{.Import}}.Load(db); err != nil {
		e = porterr.New(porterr.PortErrorDatabaseQuery, "Load {{.Import}} error: " + err.Error())
		return
	}
	return
}
	`
	tml := template.Must(template.New("").Parse(t))
	err = tml.Execute(&buf, struct {
		Import string
	}{
		Import: modelName,
	})
	return
}

func controllerCreate(modelName string) (buf bytes.Buffer, err error) {
	t := `
//create {{.Import}}
func (f *{{.Import}}Form) Create() (result *{{.Import}}Form, e porterr.IError) {
	db := base.App.GetBaseDb()
	result = f
	tx, err := db.Begin()
	if err != nil {
		e = porterr.New(porterr.PortErrorDatabaseQuery, "Create {{.Import}} error: " + err.Error())
		return
	}
	if e = f.save(tx); e == nil {
		tx.Commit()
		return
	}
	tx.Rollback()
	return
}
	`
	tml := template.Must(template.New("").Parse(t))
	err = tml.Execute(&buf, struct {
		Import string
	}{
		Import: modelName,
	})
	return
}

func controllerFind(modelName string) (buf bytes.Buffer, err error) {
	t := `
//find {{.Import}}
func (f *{{.Import}}SearchForm) Find() (result {{.Import}}ResultSet, e porterr.IError) {
	db := base.App.GetBaseDb()
	filter := godb.SqlFilter{}
	filter.AddFiledFilter("name", "=", f.Name)
	filter.AddInFilter("id", int64ToIArr([]int64{f.Id}))
	md := (&models.{{.Import}}{})
	res, _, err := md.Search(db, filter)
	if err != nil {
		e = porterr.New(porterr.PortErrorDatabaseQuery, "Find {{.Import}} error: " + err.Error())
		return
	}
	for key := range res {
		md := res[key]
		result = append(result, {{.Import}}Form{&md})
	}
	return
}
	`
	tml := template.Must(template.New("").Parse(t))
	err = tml.Execute(&buf, struct {
		Import string
	}{
		Import: modelName,
	})
	return
}

func controllerUpdate(modelName string) (buf bytes.Buffer, err error) {
	t := `
//update {{.Import}}
func (f *{{.Import}}Form) Update() (result *{{.Import}}Form, e porterr.IError) {
	db := base.App.GetBaseDb()
	result = f
	tx, err := db.Begin()
	if err != nil {
		e = porterr.New(porterr.PortErrorDatabaseQuery, "Update {{.Import}} error: " + err.Error())
		return
	}
	if e = f.primaryExistsError(tx); e != nil {
		return
	}
	if e = f.save(tx); e == nil {
		tx.Commit()
		return
	}
	tx.Rollback()
	return
}
	`
	tml := template.Must(template.New("").Parse(t))
	err = tml.Execute(&buf, struct {
		Import string
	}{
		Import: modelName,
	})
	return
}

func controllerDelete(modelName string) (buf bytes.Buffer, err error) {
	t := `
//remove {{.Import}}
func (f *{{.Import}}Form) Delete() (iError rest.IError) {
	db := base.App.GetBaseDb()
	tx, err := db.Begin()
	if err != nil {
		e = porterr.New(porterr.PortErrorDatabaseQuery, "Delete {{.Import}} error: " + err.Error())
		return
	}
	if e = f.primaryExistsError(tx); e != nil {
		return
	}
	if err = f.{{.Import}}.Delete(tx); err != nil {
		tx.Rollback()
		e = porterr.New(porterr.PortErrorDatabaseQuery, "Delete {{.Import}} error: " + err.Error())
		return
	}
	tx.Commit()
	return
}
	`
	tml := template.Must(template.New("").Parse(t))
	err = tml.Execute(&buf, struct {
		Import string
	}{
		Import: modelName,
	})
	return
}

func controllerValidate(modelName string) (buf bytes.Buffer, err error) {
	t := `
func (f *{{.Import}}Form) primaryExistsError(ds crud.DSLer) (e rest.IError){
	md := *f.{{.Import}}
	if ok, err := (&md).Load(ds); !ok {
		_, pk := (&md).PrimaryKey()
		id := fmt.Sprintf("%s", pk)
		if err != nil {
			e = porterr.New(porterr.PortErrorDatabaseQuery, "Find {{.Import}}  error: " + err.Error())
			return
		}
		e = porterr.New(porterr.PortErrorRequest, "Record {{.Import}} with id " + id + " not found ").HTTP(http.StatusBadRequest)
		return
	}
	return
}

func (f *{{.Import}}Form) save(ds crud.DSLer) (e rest.IError){
	if e = f.saveValidate(ds); iError != nil {
		return
	}
	err := f.Save(ds)
	if err != nil {
		e = porterr.New(porterr.PortErrorDatabaseQuery, "Record {{.Import}} operation save error: " + err.Error())
		return
	}
	return
}

func (f *{{.Import}}Form) saveValidate(ds crud.DSLer) (e rest.IError) {
	return
}
	`
	tml := template.Must(template.New("").Parse(t))
	err = tml.Execute(&buf, struct {
		Import string
	}{
		Import: modelName,
	})
	return
}
