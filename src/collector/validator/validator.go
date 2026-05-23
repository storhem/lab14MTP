//go:build !rust

// Package validator валидирует данные о вакансиях.
// При сборке с тегом -tags rust используется Rust-библиотека через cgo.
// По умолчанию используется чистая Go-реализация.
package validator

import "errors"

// ValidationResult — результат валидации вакансии.
type ValidationResult struct {
	Valid  bool
	Errors []string
}

// Validate проверяет корректность данных вакансии.
func Validate(name string, salaryFrom, salaryTo int, areaID string) ValidationResult {
	var errs []string

	if len(name) < 3 {
		errs = append(errs, "name too short (min 3 chars)")
	}
	if salaryFrom < 0 {
		errs = append(errs, "salary_from cannot be negative")
	}
	if salaryTo > 0 && salaryFrom > salaryTo {
		errs = append(errs, "salary_from > salary_to")
	}
	if salaryTo > 10_000_000 {
		errs = append(errs, "salary_to > 10M RUB (unrealistic)")
	}
	if areaID == "" {
		errs = append(errs, "area_id is required")
	}

	return ValidationResult{Valid: len(errs) == 0, Errors: errs}
}

// ValidateError собирает ошибки в error-тип.
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
