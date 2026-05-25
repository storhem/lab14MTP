package collector_test

import (
	"testing"

	"lab14/collector/validator"
)

func TestValidate_ValidVacancy(t *testing.T) {
	res := validator.Validate("Go Developer", 150000, 250000, "1")
	if !res.Valid {
		t.Errorf("expected valid, got errors: %v", res.Errors)
	}
}

func TestValidate_ShortName(t *testing.T) {
	res := validator.Validate("Go", 100000, 200000, "1")
	if res.Valid {
		t.Error("expected invalid: name too short")
	}
	if len(res.Errors) == 0 {
		t.Error("expected at least one error")
	}
}

func TestValidate_NegativeSalary(t *testing.T) {
	res := validator.Validate("Backend Developer", -1000, 200000, "1")
	if res.Valid {
		t.Error("expected invalid: negative salary")
	}
}

func TestValidate_SalaryFromGtTo(t *testing.T) {
	res := validator.Validate("Senior Dev", 300000, 100000, "1")
	if res.Valid {
		t.Error("expected invalid: salary_from > salary_to")
	}
}

func TestValidate_UnrealisticSalary(t *testing.T) {
	res := validator.Validate("CEO Assistant", 0, 50000000, "1")
	if res.Valid {
		t.Error("expected invalid: salary_to > 10M")
	}
}

func TestValidate_NoArea(t *testing.T) {
	res := validator.Validate("Developer", 100000, 200000, "")
	if res.Valid {
		t.Error("expected invalid: empty area_id")
	}
}

func TestValidate_ZeroSalary(t *testing.T) {
	// Нулевая зарплата допустима (не указана)
	res := validator.Validate("Developer", 0, 0, "1")
	if !res.Valid {
		t.Errorf("zero salary should be valid, got: %v", res.Errors)
	}
}
