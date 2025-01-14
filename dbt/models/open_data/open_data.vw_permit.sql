-- Copy of default.vw_pin_appeal that feeds the "Appeals" open data asset.

/* The following columns are not included in the open data asset, or are
currently hidden:
    date_updated
    filing_type
    notes
*/

SELECT
    CONCAT(pin, permit_number) AS row_id,
    pin,
    permit_number,
    local_permit_number,
    date_issued,
    date_submitted,
    estimated_date_of_completion,
    assessment_year,
    recheck_year,
    status,
    assessable,
    amount,
    address_full,
    address_street_dir,
    address_street_number,
    address_street_name,
    address_suffix_1,
    address_suffix_2,
    applicant_name,
    job_code_primary,
    job_code_secondary,
    work_description,
    improvement_code_1,
    improvement_code_2,
    improvement_code_3,
    improvement_code_4
FROM {{ ref('default.vw_pin_permit') }}
