-- Copy of default.vw_pin_appeal that feeds the "Appeals" open data asset.
-- Some columns from the feeder view may not be present in this view.

SELECT
    feeder.pin,
    feeder.class,
    feeder.township_code,
    feeder.mailed_bldg,
    feeder.mailed_land,
    feeder.mailed_tot,
    feeder.certified_bldg,
    feeder.certified_land,
    feeder.certified_tot,
    feeder.case_no,
    feeder.appeal_type,
    feeder.change,
    feeder.reason_code1,
    feeder.reason_desc1,
    feeder.reason_code2,
    feeder.reason_desc2,
    feeder.reason_code3,
    feeder.reason_desc3,
    feeder.agent_code,
    feeder.agent_name,
    feeder.status,
    {{ open_data_columns(row_id_cols=['pin', 'year', 'case_no']) }}
FROM {{ ref('default.vw_pin_appeal') }} AS feeder
FULL OUTER JOIN
    (
        /*
        row_id's in htpar consist of PINs and appeal case numbers (as well as
        years) and can show up multiple times, sometimes being deactivated and
        sometimes not. We aggregate them and to figure out which case/PIN
        combos have been entirely deactivated.
        */
        WITH
        entire_case AS (
            SELECT
                addndat.parid || addndat.taxyr || addndat.caseno AS row_id,
                addndat.taxyr AS year,
                TRUE AS deleted,
                AVG(
                    CASE
                        WHEN addndat.deactivat IS NOT NULL THEN 1 ELSE 0
                    END
                ) AS deactivat
            FROM {{ source("iasworld", "htpar") }} AS addndat
            LEFT JOIN
                {{ source("iasworld", "pardat") }} AS pdat
                ON addndat.parid = pdat.parid
                AND addndat.taxyr = pdat.taxyr
            WHERE addndat.caseno IS NOT NULL OR pdat.deactivat IS NOT NULL
            GROUP BY
                addndat.parid || addndat.taxyr || addndat.caseno,
                addndat.taxyr
        )

        SELECT
            row_id,
            year,
            deleted AS ":deleted" -- noqa: RF05
        FROM entire_case
        WHERE deactivat = 1
    ) AS deleted_rows
    ON feeder.pin || feeder.year || feeder.case_no
    = deleted_rows.row_id
