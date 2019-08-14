package crud

import (
	validation "github.com/cadyrov/govalidation"
	"github.com/dimonrus/rest"
	"net/http"
)

type Modeler interface {
	Validate() (err error)
	Load(dbo crud.DSLer) (bool, error)
}

type SearchMeta struct {
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
	Total  int `json:"total"`
}

func validateOnExisits(m Modeler, db crud.DSLer, nameStruct string, failMessage string) rest.IError {
	ok, err := m.Load(db)
	if err != nil {
		return RestIError(err, nameStruct, http.StatusInternalServerError)
	}
	if !ok {
		return RestIError(nil, failMessage, http.StatusNotFound)
	}
	return nil
}

func toIArray(interfaces ...interface{}) []interface{} {
	arr := make([]interface{}, 0)
	return append(arr, interfaces...)
}

func int64ToIArr(in []int64) []interface{} {
	sl := make([]interface{}, 0, cap(in))
	for _, val := range in {
		sl = append(sl, val)
	}
	return sl
}

func intToIArr(in []int) []interface{} {
	sl := make([]interface{}, 0, cap(in))
	for _, val := range in {
		sl = append(sl, val)
	}
	return sl
}

func strToIArr(in []string) []interface{} {
	sl := make([]interface{}, 0, cap(in))
	for _, val := range in {
		sl = append(sl, val)
	}
	return sl
}

func RestIError(err error, message string, httpCode int) (iEr rest.IError) {
	if err == nil {
		iEr = rest.NewRestError(message, httpCode)
	} else if exterr, ok := err.(validation.Errors); ok {
		iEr = rest.NewRestError(message, httpCode)
		for key, value := range exterr {
			koy := key
			val := value.ExternalError().Error()
			code := value.GetCode()
			iEr = iEr.AppendDetail(val, &koy, &code)
		}
	} else {
		iEr = rest.NewRestError(message+err.Error(), httpCode)
	}
	return
}

func inSlice(slice []int64, aim int64) bool {
	for _, value := range slice {
		if value == aim {
			return true
		}
	}
	return false
}
