SELECT
    permit.parid AS pin,
    SUBSTRING(permit.parid, 1, 10) AS pin10,
    permit.num AS permit_number,
    permit.user28 AS local_permit_number,
    permit.permdt AS issue_date,
    SUBSTRING(permit.permdt, 1, 4) AS issue_year,
    permit.amount,
    permit.note2 AS address,
    permit.user21 AS contact_name,
    permit.user43 AS work_description,
    permit.user29  -- TODO: What is this field?
FROM {{ source('iasworld', 'permit') }} AS permit
WHERE permit.cur = 'Y' AND permit.deactivat IS NULL
