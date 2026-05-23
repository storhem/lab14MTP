//go:build rust

// Package validator — cgo-интеграция с Rust-библиотекой валидации.
// Собирать с флагом: go build -tags rust
package validator

/*
#cgo LDFLAGS: -L${SRCDIR}/../../../validator/target/release -lvacancy_validator
#include <stdlib.h>
#include "../../validator/vacancy_validator.h"
*/
import "C"
import (
	"errors"
	"unsafe"
)

type ValidationResult struct {
	Valid  bool
	Errors []string
}

func Validate(name string, salaryFrom, salaryTo int, areaID string) ValidationResult {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	cAreaID := C.CString(areaID)
	defer C.free(unsafe.Pointer(cAreaID))

	result := C.validate_vacancy(cName, C.int(salaryFrom), C.int(salaryTo), cAreaID)
	defer C.free_validation_result(result)

	isValid := bool(result.is_valid)
	errs := make([]string, 0, int(result.error_count))
	for i := 0; i < int(result.error_count); i++ {
		errPtr := *(**C.char)(unsafe.Pointer(
			uintptr(unsafe.Pointer(result.errors)) + uintptr(i)*unsafe.Sizeof(uintptr(0)),
		))
		errs = append(errs, C.GoString(errPtr))
	}
	return ValidationResult{Valid: isValid, Errors: errs}
}

func ValidateError(name string, salaryFrom, salaryTo int, areaID string) error {
	res := Validate(name, salaryFrom, salaryTo, areaID)
	if res.Valid {
		return nil
	}
	msg := ""
	for _, e := range res.Errors {
		msg += e + "; "
	}
	return errors.New(msg)
}
