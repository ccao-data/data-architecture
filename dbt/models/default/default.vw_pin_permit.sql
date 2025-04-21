-- Filter for only active permit rows
WITH active_permits AS (
    SELECT *
    FROM {{ source('iasworld', 'permit') }}
    WHERE cur = 'Y' AND deactivat IS NULL
),

-- Extract detailed address information so that we can combine address fields
-- appropriately. This is useful because adradd is occasionally used instead
-- of adrdir for the street directional, which might be an error but is
-- the reality of the data as they currently stand
permit_addresses AS (
    SELECT
        iasw_id,
        adrno AS address_street_number,
        COALESCE(
            adrdir,
            CASE WHEN LENGTH(adradd) = 1 THEN adradd END
        ) AS address_street_dir,
        adrstr AS address_street_name,
        adrsuf AS address_suffix_1,
        adrsuf2 AS address_suffix_2
    FROM active_permits
)

SELECT
    permit.parid AS pin,
    SUBSTR(permit.permdt, 1, 4) AS year,
    permit.num AS permit_number,
    permit.user28 AS local_permit_number,
    permit.permdt AS date_issued,
    permit.certdate AS date_submitted,
    COALESCE(permit.udate3, permit.wen) AS date_updated,
    permit.udate2 AS estimated_date_of_completion,
    permit.user31 AS assessment_year,
    CAST(NULLIF(REGEXP_REPLACE(permit.user11, '([^0-9])', ''), '') AS INT)
        AS recheck_year,
    permit.flag AS status,
    permit.user18 AS assessable,
    permit.amount,
    vpu.township,
    NULLIF(ARRAY_JOIN(vpu.tax_municipality_name, ', '), '') AS municipality,
    -- When note2 is filled out and present, it represents the full
    -- concatenated street address. When not present, we need to
    -- reconstruct it from the detailed address fields
    COALESCE(
        -- Replace double commas that are present in the note2 field. We need
        -- to get rid of two types of double commas: trailing double commas,
        -- which need to be removed, and double commas inside an address
        -- string, which should be replaced with a single comma
        REPLACE(
            REGEXP_REPLACE(permit.note2, ',,$'),
            ',,',
            ', '
        ),
        CONCAT_WS(
            ' ',
            CAST(address.address_street_number AS VARCHAR),
            address.address_street_dir,
            address.address_street_name,
            address.address_suffix_1,
            address.address_suffix_2
        )
    ) AS address_full,
    address.address_street_dir,
    address.address_street_number,
    address.address_street_name,
    address.address_suffix_1,
    address.address_suffix_2,
    permit.user21 AS applicant_name,
    permit.why AS job_code_primary,
    permit.user42 AS job_code_secondary,
    permit.user43 AS work_description,
    permit.user6 AS improvement_code_1,
    permit.user7 AS improvement_code_2,
    permit.user8 AS improvement_code_3,
    permit.user9 AS improvement_code_4,
    permit.id1 AS filing_type,
    permit.note3 AS notes
FROM active_permits AS permit
-- The iasw_id is the unique identifier for each permit in iasWorld,
-- so we can use it as a basis for joining permits to themselves
INNER JOIN permit_addresses AS address
    ON permit.iasw_id = address.iasw_id
LEFT JOIN {{ ref('default.vw_pin_universe') }} AS vpu
    ON permit.parid = vpu.pin
    AND SUBSTR(permit.permdt, 1, 4) = vpu.year
