use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::os::raw::c_int;

/// Результат валидации (C ABI для cgo-интеграции с Go).
#[repr(C)]
pub struct CValidationResult {
    pub is_valid: bool,
    pub errors: *mut *mut c_char,
    pub error_count: c_int,
}

/// Правила валидации вакансии.
fn validate_vacancy_inner(name: &str, salary_from: i32, salary_to: i32, area_id: &str) -> Vec<String> {
    let mut errors = Vec::new();

    if name.len() < 3 {
        errors.push("name too short (min 3 chars)".to_string());
    }
    if name.len() > 500 {
        errors.push("name too long (max 500 chars)".to_string());
    }
    if salary_from < 0 {
        errors.push("salary_from cannot be negative".to_string());
    }
    if salary_to > 0 && salary_from > salary_to {
        errors.push(format!(
            "salary_from ({}) > salary_to ({})",
            salary_from, salary_to
        ));
    }
    if salary_to > 10_000_000 {
        errors.push(format!(
            "salary_to {} > 10M RUB (unrealistic)",
            salary_to
        ));
    }
    if area_id.is_empty() {
        errors.push("area_id is required".to_string());
    }

    errors
}

/// C-экспортируемая функция для cgo (Go-интеграция).
#[no_mangle]
pub extern "C" fn validate_vacancy(
    name: *const c_char,
    salary_from: c_int,
    salary_to: c_int,
    area_id: *const c_char,
) -> CValidationResult {
    let name_str = unsafe {
        CStr::from_ptr(name).to_str().unwrap_or("")
    };
    let area_str = unsafe {
        CStr::from_ptr(area_id).to_str().unwrap_or("")
    };

    let errors = validate_vacancy_inner(name_str, salary_from, salary_to, area_str);
    let is_valid = errors.is_empty();

    let mut c_errors: Vec<*mut c_char> = errors
        .into_iter()
        .map(|e| CString::new(e).unwrap().into_raw())
        .collect();

    let error_count = c_errors.len() as c_int;
    let errors_ptr = if c_errors.is_empty() {
        std::ptr::null_mut()
    } else {
        let ptr = c_errors.as_mut_ptr();
        std::mem::forget(c_errors);
        ptr
    };

    CValidationResult {
        is_valid,
        errors: errors_ptr,
        error_count,
    }
}

/// Освобождает память результата валидации.
#[no_mangle]
pub extern "C" fn free_validation_result(result: CValidationResult) {
    if !result.errors.is_null() && result.error_count > 0 {
        unsafe {
            let errors = Vec::from_raw_parts(
                result.errors,
                result.error_count as usize,
                result.error_count as usize,
            );
            for ptr in errors {
                if !ptr.is_null() {
                    drop(CString::from_raw(ptr));
                }
            }
        }
    }
}

/// PyO3-модуль для Python-интеграции.
#[cfg(feature = "python")]
mod python {
    use pyo3::prelude::*;
    use super::validate_vacancy_inner;

    #[pyfunction]
    fn validate(name: &str, salary_from: i32, salary_to: i32, area_id: &str) -> PyResult<(bool, Vec<String>)> {
        let errors = validate_vacancy_inner(name, salary_from, salary_to, area_id);
        Ok((errors.is_empty(), errors))
    }

    #[pymodule]
    fn vacancy_validator(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add_function(wrap_pyfunction!(validate, m)?)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_vacancy() {
        let errors = validate_vacancy_inner("Go Developer", 150000, 250000, "1");
        assert!(errors.is_empty(), "expected no errors, got {:?}", errors);
    }

    #[test]
    fn test_short_name() {
        let errors = validate_vacancy_inner("Go", 100000, 200000, "1");
        assert!(!errors.is_empty());
        assert!(errors[0].contains("name too short"));
    }

    #[test]
    fn test_negative_salary() {
        let errors = validate_vacancy_inner("Developer", -1000, 200000, "1");
        assert!(errors.iter().any(|e| e.contains("negative")));
    }

    #[test]
    fn test_salary_from_gt_to() {
        let errors = validate_vacancy_inner("Developer", 300000, 100000, "1");
        assert!(errors.iter().any(|e| e.contains("salary_from")));
    }

    #[test]
    fn test_unrealistic_salary() {
        let errors = validate_vacancy_inner("CEO", 0, 50_000_000, "1");
        assert!(errors.iter().any(|e| e.contains("10M")));
    }

    #[test]
    fn test_empty_area() {
        let errors = validate_vacancy_inner("Developer", 100000, 200000, "");
        assert!(errors.iter().any(|e| e.contains("area_id")));
    }
}
