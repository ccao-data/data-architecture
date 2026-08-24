{%- set whitespace_cols = [
    "own1", "own2", "addr1", "addr2", "addr3", "addr4", "addr5",
    "adrpre", "adrdir", "adrstr", "adrsuf", "unitdesc", "unitno",
    "cityname", "statecode", "zip1", "zip2", "user27"
] -%}

{%- set whitespace_parts = [] -%}
{%- for col in whitespace_cols -%}
    {%- do whitespace_parts.append(
        "(" ~ col ~ " LIKE '% ' OR " ~ col ~ " LIKE ' %')"
    ) -%}
{%- endfor -%}

{%- set tests = [
    {
        "name": "iasworld_owndat_cur_in_accepted_values",
        "description": 'cur should be "Y" or "D"',
        "category": "incorrect_values",
        "condition": "cur IN ('Y', 'D')",
        "additional_select_columns": ["cur"]
    },
    {
        "name": "iasworld_owndat_parid_in_pardat_parid",
        "description": "parid should be in pardat",
        "category": "parid",
        "condition": "pardat_parid IS NOT NULL"
    },
    {
        "name": "iasworld_owndat_parid_not_null",
        "description": "parid should not be null",
        "category": "incorrect_values",
        "condition": "parid IS NOT NULL"
    },
    {
        "name": "iasworld_owndat_seq_all_sequential_exist",
        "description": "seq should be sequential",
        "category": "incorrect_values",
        "condition": "seq = prev_seq + 1",
        "additional_select_columns": ["seq", "prev_seq"]
    },
    {
        "name": "iasworld_owndat_address_columns_no_extra_whitespace",
        "description": "own1, own2, addr1-5, and other address columns should have no leading or trailing whitespace",
        "category": "column_values",
        "condition": "NOT (" ~ whitespace_parts | join(" OR ") ~ ")",
        "additional_select_columns": whitespace_cols
    },
    {
        "name": "iasworld_owndat_unique_by_parid_taxyr",
        "description": "owndat should be unique by parid and taxyr",
        "category": "duplicate_rows",
        "condition": "num_duplicates = 1",
        "additional_select_columns": ["num_duplicates"]
    }
] -%}

{%- set base_query %}
    SELECT
        -- Identifying columns
        owndat.parid,
        owndat.taxyr,
        CAST(NULL AS INTEGER) AS card,
        CAST(NULL AS INTEGER) AS lline,
        legdat.user1 AS township_code,
        CAST(NULL AS VARCHAR) AS class,
        owndat.who,
        owndat.wen,
        -- Columns to test
        owndat.cur,
        pardat_parids.parid AS pardat_parid,
        owndat.seq,
        LAG(owndat.seq)
            OVER (PARTITION BY owndat.parid, owndat.taxyr ORDER BY owndat.seq)
            AS prev_seq,
        owndat.own1,
        owndat.own2,
        owndat.addr1,
        owndat.addr2,
        owndat.addr3,
        owndat.addr4,
        owndat.addr5,
        owndat.adrpre,
        owndat.adrdir,
        owndat.adrstr,
        owndat.adrsuf,
        owndat.unitdesc,
        owndat.unitno,
        owndat.cityname,
        owndat.statecode,
        owndat.zip1,
        owndat.zip2,
        owndat.user27,
        COUNT(*)
            OVER (PARTITION BY owndat.parid, owndat.taxyr)
            AS num_duplicates
    FROM iasworld.owndat AS owndat
    LEFT JOIN iasworld.legdat AS legdat
        ON owndat.parid = legdat.parid
        AND owndat.taxyr = legdat.taxyr
        AND legdat.cur = 'Y'
        AND legdat.deactivat IS NULL
    LEFT JOIN (
        SELECT DISTINCT parid
        FROM iasworld.pardat
    ) AS pardat_parids
        ON owndat.parid = pardat_parids.parid
    WHERE owndat.cur = 'Y'
        AND owndat.deactivat IS NULL
{% endset %}

{{ generate_iasworld_qc_test_view(base_query, tests) }}
