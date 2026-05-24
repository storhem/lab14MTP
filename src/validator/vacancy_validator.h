#ifndef VACANCY_VALIDATOR_H
#define VACANCY_VALIDATOR_H

#include <stdbool.h>

typedef struct {
    bool is_valid;
    char** errors;
    int error_count;
} CValidationResult;

CValidationResult validate_vacancy(
    const char* name,
    int salary_from,
    int salary_to,
    const char* area_id
);

void free_validation_result(CValidationResult result);

#endif /* VACANCY_VALIDATOR_H */
