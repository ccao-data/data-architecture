-- View containing values from each stage of assessment by PIN and year
-- in wide format

-- Stage name constants
{% set stage_name_pre_mailed = 'PRE-MAILED' %}
{% set stage_name_mailed = 'MAILED' %}
{% set stage_name_pre_certified = 'ASSESSOR PRE-CERTIFIED' %}
{% set stage_name_certified = 'ASSESSOR CERTIFIED' %}
{% set stage_name_board = 'BOARD CERTIFIED' %}

-- Get a list of completed stages for all PINs in all years. This will allow us
-- to disambiguate pre-mailed values from pre-certified values based on which
-- stages are present for which PINs (i.e. if CCAOVALUE is present, the value
-- cannot be pre-mail)
WITH stages AS (
    SELECT
        parid,
        taxyr,
        ARRAY_AGG(procname) AS procnames
    FROM {{ source('iasworld', 'asmt_all') }}
    WHERE procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')
        AND rolltype != 'RR'
        AND deactivat IS NULL
        AND valclass IS NULL
    GROUP BY parid, taxyr
),

-- CCAO mailed, CCAO final, and BOR final values for each PIN by year.
-- We use ARBITRARY functions here for two reasons: 1) To flatten three stages
-- of assessment into one row, and 2) to deduplicate PINs with multiple rows for
-- a given stage/pin/year combination. Values are always the same within these
-- duplicates.
stage_values AS (
    SELECT
        asmt.parid AS pin,
        asmt.taxyr AS year,
        -- Pre-mailed values
        ARBITRARY(
            CASE
                WHEN
                    {{ pre_mailed_filters('asmt') }}
                    THEN REGEXP_REPLACE(asmt.class, '[^[:alnum:]]', '')
            END
        ) AS pre_mailed_class,
        ARBITRARY(
            CASE
                WHEN
                    {{ pre_mailed_filters('asmt') }} AND asmt.taxyr < '2020'
                    THEN asmt.ovrvalasm2
                WHEN
                    {{ pre_mailed_filters('asmt') }} AND asmt.taxyr >= '2020'
                    THEN asmt.valasm2
            END
        ) AS pre_mailed_bldg,
        ARBITRARY(
            CASE
                WHEN
                    {{ pre_mailed_filters('asmt') }} AND asmt.taxyr < '2020'
                    THEN asmt.ovrvalasm1
                WHEN
                    {{ pre_mailed_filters('asmt') }} AND asmt.taxyr >= '2020'
                    THEN asmt.valasm1
            END
        ) AS pre_mailed_land,
        ARBITRARY(
            CASE
                WHEN
                    {{ pre_mailed_filters('asmt') }} AND asmt.taxyr < '2020'
                    THEN asmt.ovrvalasm3
                WHEN
                    {{ pre_mailed_filters('asmt') }} AND asmt.taxyr >= '2020'
                    THEN asmt.valasm3
            END
        ) AS pre_mailed_tot,
        -- Pre-mailed market values
        ARBITRARY(
            CASE
                WHEN
                    {{ pre_mailed_filters('asmt') }} AND asmt.taxyr < '2020'
                    THEN NULL
                WHEN
                    {{ pre_mailed_filters('asmt') }} AND asmt.taxyr >= '2020'
                    THEN asmt.valapr2
            END
        ) AS pre_mailed_bldg_mv,
        ARBITRARY(
            CASE
                WHEN
                    {{ pre_mailed_filters('asmt') }} AND asmt.taxyr < '2020'
                    THEN NULL
                WHEN
                    {{ pre_mailed_filters('asmt') }} AND asmt.taxyr >= '2020'
                    THEN asmt.valapr1
            END
        ) AS pre_mailed_land_mv,
        ARBITRARY(
            CASE
                WHEN
                    {{ pre_mailed_filters('asmt') }} AND asmt.taxyr < '2020'
                    THEN NULL
                WHEN
                    {{ pre_mailed_filters('asmt') }} AND asmt.taxyr >= '2020'
                    THEN asmt.valapr3
            END
        ) AS pre_mailed_tot_mv,
        -- Mailed values
        ARBITRARY(
            CASE
                WHEN
                    asmt.procname = 'CCAOVALUE'
                    THEN REGEXP_REPLACE(asmt.class, '[^[:alnum:]]', '')
            END
        ) AS mailed_class,
        ARBITRARY(
            CASE
                WHEN
                    asmt.procname = 'CCAOVALUE' AND asmt.taxyr < '2020'
                    THEN asmt.ovrvalasm2
                WHEN
                    asmt.procname = 'CCAOVALUE' AND asmt.taxyr >= '2020'
                    THEN asmt.valasm2
            END
        ) AS mailed_bldg,
        ARBITRARY(
            CASE
                WHEN
                    asmt.procname = 'CCAOVALUE' AND asmt.taxyr < '2020'
                    THEN asmt.ovrvalasm1
                WHEN
                    asmt.procname = 'CCAOVALUE' AND asmt.taxyr >= '2020'
                    THEN asmt.valasm1
            END
        ) AS mailed_land,
        ARBITRARY(
            CASE
                WHEN
                    asmt.procname = 'CCAOVALUE' AND asmt.taxyr < '2020'
                    THEN asmt.ovrvalasm3
                WHEN
                    asmt.procname = 'CCAOVALUE' AND asmt.taxyr >= '2020'
                    THEN asmt.valasm3
            END
        ) AS mailed_tot,
        -- Mailed market values
        ARBITRARY(
            CASE
                WHEN
                    asmt.procname = 'CCAOVALUE' AND asmt.taxyr < '2020'
                    THEN NULL
                WHEN
                    asmt.procname = 'CCAOVALUE' AND asmt.taxyr >= '2020'
                    THEN asmt.valapr2
            END
        ) AS mailed_bldg_mv,
        ARBITRARY(
            CASE
                WHEN
                    asmt.procname = 'CCAOVALUE' AND asmt.taxyr < '2020'
                    THEN NULL
                WHEN
                    asmt.procname = 'CCAOVALUE' AND asmt.taxyr >= '2020'
                    THEN asmt.valapr1
            END
        ) AS mailed_land_mv,
        ARBITRARY(
            CASE
                WHEN
                    asmt.procname = 'CCAOVALUE' AND asmt.taxyr < '2020'
                    THEN NULL
                WHEN
                    asmt.procname = 'CCAOVALUE' AND asmt.taxyr >= '2020'
                    THEN asmt.valapr3
            END
        ) AS mailed_tot_mv,
        -- Assessor pre-certified values
        ARBITRARY(
            CASE
                WHEN
                    {{ pre_certified_filters('asmt') }}
                    THEN REGEXP_REPLACE(asmt.class, '[^[:alnum:]]', '')
            END
        ) AS pre_certified_class,
        ARBITRARY(
            CASE
                WHEN
                    {{ pre_certified_filters('asmt') }} AND asmt.taxyr < '2020'
                    THEN asmt.ovrvalasm2
                WHEN
                    {{ pre_certified_filters('asmt') }} AND asmt.taxyr >= '2020'
                    THEN asmt.valasm2
            END
        ) AS pre_certified_bldg,
        ARBITRARY(
            CASE
                WHEN
                    {{ pre_certified_filters('asmt') }} AND asmt.taxyr < '2020'
                    THEN asmt.ovrvalasm1
                WHEN
                    {{ pre_certified_filters('asmt') }} AND asmt.taxyr >= '2020'
                    THEN asmt.valasm1
            END
        ) AS pre_certified_land,
        ARBITRARY(
            CASE
                WHEN
                    {{ pre_certified_filters('asmt') }} AND asmt.taxyr < '2020'
                    THEN asmt.ovrvalasm3
                WHEN
                    {{ pre_certified_filters('asmt') }} AND asmt.taxyr >= '2020'
                    THEN asmt.valasm3
            END
        ) AS pre_certified_tot,
        -- Assessor pre-certified market values
        ARBITRARY(
            CASE
                WHEN
                    {{ pre_certified_filters('asmt') }} AND asmt.taxyr < '2020'
                    THEN NULL
                WHEN
                    {{ pre_certified_filters('asmt') }} AND asmt.taxyr >= '2020'
                    THEN asmt.valapr2
            END
        ) AS pre_certified_bldg_mv,
        ARBITRARY(
            CASE
                WHEN
                    {{ pre_certified_filters('asmt') }} AND asmt.taxyr < '2020'
                    THEN NULL
                WHEN
                    {{ pre_certified_filters('asmt') }} AND asmt.taxyr >= '2020'
                    THEN asmt.valapr1
            END
        ) AS pre_certified_land_mv,
        ARBITRARY(
            CASE
                WHEN
                    {{ pre_certified_filters('asmt') }} AND asmt.taxyr < '2020'
                    THEN NULL
                WHEN
                    {{ pre_certified_filters('asmt') }} AND asmt.taxyr >= '2020'
                    THEN asmt.valapr3
            END
        ) AS pre_certified_tot_mv,
        -- Assessor certified values
        ARBITRARY(
            CASE
                WHEN
                    asmt.procname = 'CCAOFINAL'
                    THEN REGEXP_REPLACE(asmt.class, '[^[:alnum:]]', '')
            END
        ) AS certified_class,
        ARBITRARY(
            CASE
                WHEN
                    asmt.procname = 'CCAOFINAL' AND asmt.taxyr < '2020'
                    THEN asmt.ovrvalasm2
                WHEN
                    asmt.procname = 'CCAOFINAL' AND asmt.taxyr >= '2020'
                    THEN asmt.valasm2
            END
        ) AS certified_bldg,
        ARBITRARY(
            CASE
                WHEN
                    asmt.procname = 'CCAOFINAL' AND asmt.taxyr < '2020'
                    THEN asmt.ovrvalasm1
                WHEN
                    asmt.procname = 'CCAOFINAL' AND asmt.taxyr >= '2020'
                    THEN asmt.valasm1
            END
        ) AS certified_land,
        ARBITRARY(
            CASE
                WHEN
                    asmt.procname = 'CCAOFINAL' AND asmt.taxyr < '2020'
                    THEN asmt.ovrvalasm3
                WHEN
                    asmt.procname = 'CCAOFINAL' AND asmt.taxyr >= '2020'
                    THEN asmt.valasm3
            END
        ) AS certified_tot,
        -- Assessor certified market values
        ARBITRARY(
            CASE
                WHEN
                    asmt.procname = 'CCAOFINAL' AND asmt.taxyr < '2020'
                    THEN NULL
                WHEN
                    asmt.procname = 'CCAOFINAL' AND asmt.taxyr >= '2020'
                    THEN asmt.valapr2
            END
        ) AS certified_bldg_mv,
        ARBITRARY(
            CASE
                WHEN
                    asmt.procname = 'CCAOFINAL' AND asmt.taxyr < '2020'
                    THEN NULL
                WHEN
                    asmt.procname = 'CCAOFINAL' AND asmt.taxyr >= '2020'
                    THEN asmt.valapr1
            END
        ) AS certified_land_mv,
        ARBITRARY(
            CASE
                WHEN
                    asmt.procname = 'CCAOFINAL' AND asmt.taxyr < '2020'
                    THEN NULL
                WHEN
                    asmt.procname = 'CCAOFINAL' AND asmt.taxyr >= '2020'
                    THEN asmt.valapr3
            END
        ) AS certified_tot_mv,
        -- Board certified values
        ARBITRARY(
            CASE
                WHEN
                    asmt.procname = 'BORVALUE'
                    THEN REGEXP_REPLACE(asmt.class, '[^[:alnum:]]', '')
            END
        ) AS board_class,
        ARBITRARY(
            CASE
                WHEN
                    asmt.procname = 'BORVALUE' AND asmt.taxyr < '2020'
                    THEN asmt.ovrvalasm2
                WHEN
                    asmt.procname = 'BORVALUE' AND asmt.taxyr >= '2020'
                    THEN asmt.valasm2
            END
        ) AS board_bldg,
        ARBITRARY(
            CASE
                WHEN
                    asmt.procname = 'BORVALUE' AND asmt.taxyr < '2020'
                    THEN asmt.ovrvalasm1
                WHEN
                    asmt.procname = 'BORVALUE' AND asmt.taxyr >= '2020'
                    THEN asmt.valasm1
            END
        ) AS board_land,
        ARBITRARY(
            CASE
                WHEN
                    asmt.procname = 'BORVALUE' AND asmt.taxyr < '2020'
                    THEN asmt.ovrvalasm3
                WHEN
                    asmt.procname = 'BORVALUE' AND asmt.taxyr >= '2020'
                    THEN asmt.valasm3
            END
        ) AS board_tot,
        -- Board certified market values
        ARBITRARY(
            CASE
                WHEN
                    asmt.procname = 'BORVALUE' AND asmt.taxyr < '2020'
                    THEN NULL
                WHEN
                    asmt.procname = 'BORVALUE' AND asmt.taxyr >= '2020'
                    THEN asmt.valapr2
            END
        ) AS board_bldg_mv,
        ARBITRARY(
            CASE
                WHEN
                    asmt.procname = 'BORVALUE' AND asmt.taxyr < '2020'
                    THEN NULL
                WHEN
                    asmt.procname = 'BORVALUE' AND asmt.taxyr >= '2020'
                    THEN asmt.valapr1
            END
        ) AS board_land_mv,
        ARBITRARY(
            CASE
                WHEN
                    asmt.procname = 'BORVALUE' AND asmt.taxyr < '2020'
                    THEN NULL
                WHEN
                    asmt.procname = 'BORVALUE' AND asmt.taxyr >= '2020'
                    THEN asmt.valapr3
            END
        ) AS board_tot_mv
    FROM {{ source('iasworld', 'asmt_all') }} AS asmt
    LEFT JOIN stages
        ON asmt.parid = stages.parid
        AND asmt.taxyr = stages.taxyr
    WHERE (
        -- Check for two possible situations: Either the record is stamped
        -- with a procname corresponding to a stage, in which case the record
        -- corresponds to a final value for a stage, or the procname is null
        -- and other fields indicate that it is a provisional value for an
        -- upcoming stage
        asmt.procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')
        OR (
            asmt.procname IS NULL
            AND asmt.cur = 'Y'
            AND (
                -- If the PIN has no stages but its year is not the current
                -- assessment year, it is likely a data error from a prior
                -- year that we don't want to include in our results. In
                -- contrast, if the PIN is in the current year but has no
                -- stages, it is most likely a provisional value for a PIN
                -- that has not mailed yet
                CARDINALITY(stages.procnames) != 0
                OR asmt.taxyr = DATE_FORMAT(NOW(), '%Y')
            )
        )
    )
    AND asmt.rolltype != 'RR'
    AND asmt.deactivat IS NULL
    AND asmt.valclass IS NULL
    GROUP BY asmt.parid, asmt.taxyr
),

clean_values AS (
    SELECT
        stage_values.*,
        -- Current stage indicator
        CASE
            WHEN
                stage_values.board_tot IS NOT NULL
                THEN '{{ stage_name_board }}'
            WHEN
                stage_values.certified_tot IS NOT NULL
                THEN '{{ stage_name_certified }}'
            WHEN
                stage_values.pre_certified_tot IS NOT NULL
                THEN '{{ stage_name_pre_certified }}'
            WHEN
                stage_values.mailed_tot IS NOT NULL
                THEN '{{ stage_name_mailed }}'
            WHEN
                stage_values.pre_mailed_tot IS NOT NULL
                THEN '{{ stage_name_pre_mailed }}'
        END AS stage_name,
        CASE
            WHEN stage_values.board_tot IS NOT NULL THEN 3
            WHEN stage_values.certified_tot IS NOT NULL THEN 2
            WHEN stage_values.pre_certified_tot IS NOT NULL THEN 1.5
            WHEN stage_values.mailed_tot IS NOT NULL THEN 1
            WHEN stage_values.pre_mailed_tot IS NOT NULL THEN 0
        END AS stage_num
    FROM stage_values
),

change_reasons AS (
    SELECT
        aprval.parid AS pin,
        aprval.taxyr AS year,
        aprval.reascd,
        CASE
            WHEN
                {{ pre_mailed_filters('aprval') }}
                THEN '{{ stage_name_pre_mailed }}'
            WHEN aprval.procname = 'CCAOVALUE' THEN '{{ stage_name_mailed }}'
            WHEN
                {{ pre_certified_filters('aprval') }}
                THEN '{{ stage_name_pre_certified }}'
            WHEN aprval.procname = 'CCAOFINAL' THEN '{{ stage_name_certified }}'
            WHEN aprval.procname = 'BORVALUE' THEN '{{ stage_name_board }}'
        END AS stage_name
    FROM {{ source('iasworld', 'aprval') }} AS aprval
    LEFT JOIN stages
        ON aprval.parid = stages.parid
        AND aprval.taxyr = stages.taxyr
    WHERE aprval.reascd IS NOT NULL
        AND (
            aprval.procname IS NULL
            OR aprval.procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')
        )
)

SELECT
    vals.*,
    descr.description AS change_reason
FROM clean_values AS vals
LEFT JOIN change_reasons AS reasons
    ON vals.pin = reasons.pin
    AND vals.year = reasons.year
    AND vals.stage_name = reasons.stage_name
LEFT JOIN {{ ref('ccao.aprval_reascd') }} AS descr
    ON reasons.reascd = descr.reascd
