package crud

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"text/template"
)

func MakeController(path string, modelPath string,  modelName string) (err error){
	upperModel := strings.Title(modelName)
	// create file
	file, pth, err := CreateControllerFile(path, modelName)
	if err != nil {
		return
	}
	defer file.Close()

	// header
	buf, err := controllerHeader(modelPath)
	if err != nil {
		return
	}
	if _, errs := file.Write(buf.Bytes()); errs != nil{
		err = errs
		return
	}
	// forms
	buf, err = controllerForm(upperModel)
	if err != nil {
		return
	}
	if _, errs := file.Write(buf.Bytes()); errs != nil{
		err = errs
		return
	}

	// returnedSlice
	buf, err = controllerReturnedSlice(upperModel)
	if err != nil {
		return
	}
	if _, errs := file.Write(buf.Bytes()); errs != nil{
		err = errs
		return
	}
	// create
	buf, err = controllerCreate(upperModel)
	if err != nil {
		return
	}
	if _, errs := file.Write(buf.Bytes()); errs != nil{
		err = errs
		return
	}

	// read
	buf, err = controllerRead(upperModel)
	if err != nil {
		return
	}
	if _, errs := file.Write(buf.Bytes()); errs != nil{
		err = errs
		return
	}

	// find
	buf, err = controllerFind(upperModel)
	if err != nil {
		return
	}
	if _, errs := file.Write(buf.Bytes()); errs != nil{
		err = errs
		return
	}

	// udpate
	buf, err = controllerUpdate(upperModel)
	if err != nil {
		return
	}
	if _, errs := file.Write(buf.Bytes()); errs != nil{
		err = errs
		return
	}
	// delete
	buf, err = controllerDelete(upperModel)
	if err != nil {
		return
	}
	if _, errs := file.Write(buf.Bytes()); errs != nil{
		err = errs
		return
	}
	// validate and services
	buf, err = controllerValidate(upperModel)
	if err != nil {
		return
	}
	if _, errs := file.Write(buf.Bytes()); errs != nil{
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
		`"github.com/dimonrus/rest"`,
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
	func (f *{{.Import}}Form) Get() (result *{{.Import}}Form, iError rest.IError) {
		db := base.App.GetBaseDb()
		if _, err := f.Load(db); err != nil {
			iError = RestIError(err, "", http.StatusInternalServerError)
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
	func (f *{{.Import}}Form) Create() (result *{{.Import}}Form, iError rest.IError) {
		db := base.App.GetBaseDb()
		result = f
		tx, err := db.Begin()
		if err != nil {
			iError = RestIError(err, "", http.StatusInternalServerError)
			return
		}
		if iError = f.save(tx); iError == nil {
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
	func (f *{{.Import}}SearchForm) Find() (result {{.Import}}ResultSet, iError rest.IError) {
		db := base.App.GetBaseDb()
		filter := godb.SqlFilter{}
		filter.AddFiledFilter("name", "=", f.Name)
		filter.AddInFilter("id", int64ToIArr([]int64{f.Id}))
		md := (&models.{{.Import}}{})
		res, _, err := md.Search(db, filter)
		if err != nil {
			iError = RestIError(err, "", http.StatusInternalServerError)
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
	func (f *{{.Import}}Form) Update() (result *{{.Import}}Form, iError rest.IError) {
		db := base.App.GetBaseDb()
		result = f
		tx, err := db.Begin()
		if err != nil {
			iError = RestIError(err, "", http.StatusInternalServerError)
			return
		}
		if iError = f.primaryExistsError(tx); iError != nil {
			return
		}
		if iError = f.save(tx); iError == nil {
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
	func (f *{{.Import}}Form) Remove() (iError rest.IError) {
		db := base.App.GetBaseDb()
		tx, err := db.Begin()
		if err != nil {
			iError = RestIError(err, "", http.StatusInternalServerError)
			return
		}
		if iError = f.primaryExistsError(tx); iError != nil {
			return
		}
		if err = f.Delete(tx); err != nil {
			tx.Rollback()
			iError = RestIError(err, "", http.StatusInternalServerError)
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
	func (f *{{.Import}}Form) primaryExistsError(ds crud.DSLer) (iError rest.IError){
		md := *f.{{.Import}}
		model := &md
		if ok, err := model.Load(ds); !ok {
			_, pk := model.PrimaryKey()
			id := fmt.Sprintf("%s", pk)
			if err != nil {
				iError = RestIError(err, "", http.StatusInternalServerError)
				return
			}
			iError = RestIError(nil, "Record with id " + id + " not found", http.StatusBadRequest)
			return
		}
		return
	}

	func (f *{{.Import}}Form) save(ds crud.DSLer) (iError rest.IError){
		if iError = f.saveValidate(ds); iError != nil {
			return
		}
		err := f.Save(ds)
		if err != nil {
			iError = RestIError(err, "", http.StatusInternalServerError)
			return
		}
		return
	}

	func (f *{{.Import}}Form) saveValidate(ds crud.DSLer) (iError rest.IError) {
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
