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
{{ open_data_join_rows_to_delete(addn_table="htpar") }}
