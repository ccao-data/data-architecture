# Design doc: Reporting source-of-truth (SoT) views

This doc describes the design of a new set of reporting SQL views that aim
to make the CCAO's reporting more consistent, durable, debuggable, and useful.

## Motivation

The Data Department gets asked to fill many ad-hoc reporting requests for
various aggregate stats, usually partitioned by geography. This has resulted
in a buildup of ad-hoc reporting views and tables over time. These ad-hoc views
are difficult to debug and trust, because each of them is complicated in a
slightly different way.

Rather than continue to create such ad-hoc views, we should consolidate our
reporting efforts into a few larger views that cover the majority of CCAO
reporting needs. These new views will cover different groupings, geographies,
and valuation stages, and will use a consistent methodology and schema.
They will serve as the new "source-of-truth" for both internal and external
CCAO reporting.

## Requirements

Each of the following requirements has two components: A motivation ("why do
do we want to do this?") plus a list of requirements ("how will we do this?").

### Durable / long-lived

#### Motivation

Since its inception in 2018, the CCAO's Data Department has created, used, and
deprecated many reporting assets (views, tables, and extracts). This iteration
was necessary due to changing systems and reporting requirements/capabilities.
However, this constant change creates two problems:

1. It's hard to know which reporting asset was used to create past output, as
  the asset may be deprecated/gone.
2. It's hard to know which asset to use for *current* reporting/diagnostic
  needs, as the available assets are often changing.

Now that the pace of institutional change has slowed a bit, it's possible to
create a more permanent set of reporting views that would help solve these
problems.

#### Requirements

We should be able to:

- Reference columns or tables by name without fear of them changing or
  disappearing in a few years
- Store the data in a persistent and/or fully reproducible way, such that
  there is a near-zero chance of data loss
- Build long-term applications and extracts on top of these views (e.g. in
  Open Data or Tableau)

### Consistent methods over time

#### Motivation

CCAO reporting has used inconsistent methods over time. For example, ratio
studies performed in different years, or by different groups, have not always
used identical methods for excluding non-market sales. As a result, comparing
published ratio statistics across years is very difficult, if not impossible.
Even aggregate statistics like counts can change over time based on what is
or is not included in the underlying data.

The goal of the new views would to be apply a single set of consistent filters,
methods, and SOPs, such that the resulting statistics are comparable across
time.

#### Requirements

We should be able to:

- Apply the same method or set of methods for generating aggregate stats
  across all years and groups
- Compare statistics over time within a group/geography, or compare different
  groups to each other in a given time period
- Manage the lifecycle of methods, such that an underlying change in a method
  requires a method version change, and the previous method persists for a
  set period of time, or indefinitely

### Cover common needs, but with some flexibility

#### Motivation

The vast majority of Data Department reporting needs/asks are some version of,
"Fetch an aggregate statistic by group and geography." Currently, these needs
are met on an ad-hoc basis by individual assets, but we can likely create
larger views that cover *most* possible geographies, groups, and aggregate
statistics.

#### Requirements

New reporting tables should:

- Cover the majority of current expected asks/uses
- Allow "live" reporting of statistics as stages are mailed/finalized
- Be flexible enough to add new columns (aggregate stats) or groups, without
  changing the other columns/groups
- Serve as a "universe" per group and geography, such that each member is
  represented in the view, even if it has no statistics

### Centralize, document, and test

#### Motivation

Existing reporting views were created on an ad-hoc basis to meet the needs of
the time. As a result, it's difficult to know if they are perfectly accurate,
given the necessary complexity of their underlying SQL. Their complexity and
lack of shared schema also means they're difficult to work on for newcomers.

Consolidating reporting into larger views would let us focus our debugging and
documentation efforts into just a few areas, rather than a dispersed set of
assets.

#### Requirements

New reporting views should:

- Have strict tests for:
  - Completeness
  - Errors
  - Representativeness (each geography has a row for each possible value)
- Have tests run daily, regardless of build status
- Subsume the duties of current reporting tables/views (eventually, over time)

## Design

The main output of this project would be *new reporting tables* in our
AWS Athena data lakehouse.

Each table would have roughly the same schema/primary key, but a different set
of statistics. The Data Department spent some time scoping the overall table
schema in-person and came up with the following:

| method | year | geography_type | geography_id | geography_data_year | group_type     | group_id | stage_name | stage_id | additional_cols... |
|--------|------|----------------|--------------|---------------------|----------------|----------|------------|----------|--------------------|
| v1     | 2023 | municipality   | Chicago      | 2022                | class          | 204      | board      | 3        | ...                |
| v1     | 2010 | triad          | 1            | 2010                | modeling_group | SF       | mailed     | 1        | ...                |

### Primary key columns

- **method (string)** - The method version number used for lifecycle
  management. Hopefully set to `v1` forever, but more likely to increment for
  each breaking change
- **year (string)** - Tax year. Should derive existence from `iasworld.pardat`
- **geography_type (string)** - The type of geography the stat is grouped by,
  e.g. municipality, triad, township, etc. Should encompass all geographies
  currently in our PIN universe
- **geography_id (string)** - The identifier for each unit of geography,
  e.g. for municipality this would be `Chicago`, for township this might be
  "Thornton"
- **geography_data_year (string)** - The year of the file the geography
  definition is sourced from e.g. a 2020 Census boundary would be `2020`
- **group_type (string)** - The sub-group for aggregation within each geometry
  e.g. class, modeling group, etc.
- **group_id (string)** - The sub-group identifier e.g. for class it would be
  `202`, `203`, etc.
- **(optional) stage_name (string)** - Name of the assessment stage used for
  grouping, one of `mailed`, `certified`, or `board`
- **(optional) stage_id (string)** - Numeric identifier of the assessment
  stage, one of `1` (mailed), `2` (certified), `3` (board)

#### Enumerated geography_type

The `geography_type` column above should span the following values. Where both
names and codes are available, we should prefer codes e.g. `1` instead of
`City` for triad.

- `county` (no grouping, all of Cook County)
- `triad`
- `township`
- `nbhd` (Assessor codes)
- `tax_code`
- `zip_code`
- `community_area` (if available)
- `census_place`
- `census_tract`
- `census_congressional_district`
- `census_zcta`
- `cook_board_of_review_district`
- `cook_commissioner_district`
- `cook_judicial_district`
- `ward_num` (if available)
- `police_district` (if available)
- `school_elementary_district`
- `school_secondary_district`
- `school_unified_district`
- `municipality`
- `tax_park_district`
- `tax_library_district`
- `tax_fire_protection_district`
- `tax_community_college_district`
- `tax_sanitation_district`
- `tax_special_service_area`
- `tax_tif_district`
- `central_business_district`

#### Enumerated group_type

The `group_type` column above should span the following values.

- `no_group` (geography breakout only)
- `class` (minor class, 3-digit code)
- `major_class` (first digit of class code, excluding letter)
- `modeling_group` (SF, MF, CONDO, LAND, NON-RES)
- `res_comm` (residential or commercial)
- `av_quintile`

### Tables

Some of the tables would be grouped by *assessment stage*. This means
there would be one row of aggregate statistics per geography, per group, per
stage e.g. Schaumburg class 202 mailed value statistics. Tables divided by
stage would have their stage groups populated as each stage becomes available.
They would also include an indicator of the *percentage of the stage complete*.

The new table schema would be used for four new tables, each corresponding to
a **topic area**. The columns in each table would depend on the topic area. The
topic areas and columns are:

#### Sales

Not grouped by stage. The table includes statistics about property sales. Spans
2014 to present. Dependent on MyDec and `iasworld.sales` feed updates.

- `pin_n_tot`
- `sale_year`
- `sale_n_tot`
- `sale_n_valid_ccao` (CCAO sales validation codes)
- `sale_n_invalid_ccao`
- `sale_n_valid_idor` (IDOR sales validation codes)
- `sale_n_invalid_idor`
- `sale_fmv_min`
- `sale_fmv_q10`
- `sale_fmv_q25`
- `sale_fmv_median`
- `sale_fmv_q75`
- `sale_fmv_q90`
- `sale_fmv_max`
- `sale_fmv_mean`
- `sale_fmv_sum`
- `sale_fmv_delta_median`
- `sale_fmv_delta_mean`
- `sale_fmv_delta_sum`
- `sale_fmv_per_sqft_min`
- `sale_fmv_per_sqft_q10`
- `sale_fmv_per_sqft_q25`
- `sale_fmv_per_sqft_median`
- `sale_fmv_per_sqft_q75`
- `sale_fmv_per_sqft_q90`
- `sale_fmv_per_sqft_max`
- `sale_fmv_per_sqft_mean`
- `sale_fmv_per_sqft_sum`
- `sale_fmv_per_sqft_delta_median`
- `sale_fmv_per_sqft_delta_mean`
- `sale_fmv_per_sqft_delta_sum`
- `sale_char_bldg_sf_median`
- `sale_char_land_sf_median`
- `sale_char_yrblt_median`
- `sale_class_mode`
- `sale_av_prior_mailed`
- `sale_av_prior_certified`
- `sale_av_prior_board`

#### Tax and exemptions

Not grouped by stage. Includes statistics about tax bills and exemptions. Spans
2006 to two years prior to present. Dependent on PTAXSIM data and forthcoming
iasWorld exemptions and tax bill data.

- `pin_n_tot`
- `tax_year`
- `tax_eq_factor_final`
- `tax_eq_factor_tentative`
- `tax_bill_min`
- `tax_bill_q10`
- `tax_bill_q25`
- `tax_bill_median`
- `tax_bill_q75`
- `tax_bill_q90`
- `tax_bill_max`
- `tax_bill_mean`
- `tax_bill_sum`
- `tax_bill_delta_median`
- `tax_bill_delta_mean`
- `tax_bill_delta_sum`
- `tax_rate_min`
- `tax_rate_q10`
- `tax_rate_q25`
- `tax_rate_median`
- `tax_rate_q75`
- `tax_rate_q90`
- `tax_rate_max`
- `tax_rate_mean`
- `tax_rate_sum`
- `tax_av_min`
- `tax_av_q10`
- `tax_av_q25`
- `tax_av_median`
- `tax_av_q75`
- `tax_av_q90`
- `tax_av_max`
- `tax_av_mean`
- `tax_av_sum`
- `exe_n_total`
- `exe_n_homeowner`
- `exe_n_senior`
- `exe_n_freeze`
- `exe_n_longtime_homeowner`
- `exe_n_disabled`
- `exe_n_vet_returning`
- `exe_n_vet_dis_lt50`
- `exe_n_vet_dis_50_69`
- `exe_n_vet_dis_ge70`
- `exe_n_abate`
- `exe_eav_total`
- `exe_eav_homeowner`
- `exe_eav_senior`
- `exe_eav_freeze`
- `exe_eav_longtime_homeowner`
- `exe_eav_disabled`
- `exe_eav_vet_returning`
- `exe_eav_vet_dis_lt50`
- `exe_eav_vet_dis_50_69`
- `exe_eav_vet_dis_ge70`
- `exe_eav_abate`

#### Assessment roll

Grouped by stage. Includes statistics for assessed values at each stage of
assessment. Available 2006 to present. Dependent on `iasworld.asmt_all` table.

- `pin_n_tot`
- `pin_n_w_value`
- `pin_pct_w_value`
- `reassessment_year`
- `av_tot_min`
- `av_tot_q10`
- `av_tot_q25`
- `av_tot_median`
- `av_tot_q75`
- `av_tot_q90`
- `av_tot_max`
- `av_tot_mean`
- `av_tot_sum`
- `av_tot_delta_median`
- `av_tot_delta_mean`
- `av_tot_delta_sum`
- `av_tot_delta_pct_median`
- `av_tot_delta_pct_mean`
- `av_bldg_min`
- `av_bldg_q10`
- `av_bldg_q25`
- `av_bldg_median`
- `av_bldg_q75`
- `av_bldg_q90`
- `av_bldg_max`
- `av_bldg_mean`
- `av_bldg_sum`
- `av_bldg_delta_median`
- `av_bldg_delta_mean`
- `av_bldg_delta_sum`
- `av_bldg_delta_pct_median`
- `av_bldg_delta_pct_mean`
- `av_land_min`
- `av_land_q10`
- `av_land_q25`
- `av_land_median`
- `av_land_q75`
- `av_land_q90`
- `av_land_max`
- `av_land_mean`
- `av_land_sum`
- `av_land_delta_median`
- `av_land_delta_mean`
- `av_land_delta_sum`
- `av_land_delta_pct_median`
- `av_land_delta_pct_mean`

#### Ratio statistics

Grouped by stage. Includes ratio statistics for market values at each stage of
assessment. Dependent on `iasworld.asmt_all` table and the existence of a
viable market value column. Removing the 95% CI here since it's largely unused.

- `pin_n_tot`
- `pin_n_w_value`
- `pin_pct_w_value`
- `reassessment_year`
- `sale_n_tot`
- `sale_n_valid_ccao` (CCAO sales validation codes)
- `sale_n_invalid_ccao`
- `sale_n_valid_idor` (IDOR sales validation codes)
- `sale_n_invalid_idor`
- `fmv_tot_min`
- `fmv_tot_q10`
- `fmv_tot_q25`
- `fmv_tot_median`
- `fmv_tot_q75`
- `fmv_tot_q90`
- `fmv_tot_max`
- `fmv_tot_mean`
- `fmv_tot_sum`
- `fmv_tot_delta_median`
- `fmv_tot_delta_mean`
- `fmv_tot_delta_sum`
- `fmv_tot_delta_pct_median`
- `fmv_tot_delta_pct_mean`
- `ratio_min`
- `ratio_q10`
- `ratio_q25`
- `ratio_median`
- `ratio_q75`
- `ratio_q90`
- `ratio_max`
- `ratio_mean`
- `cod`
- `cod_no_sop`
- `cod_n`
- `prd`
- `prd_no_sop`
- `prd_n`
- `prb`
- `prb_no_sop`
- `prb_n`
- `mki`
- `mki_no_sop`
- `mki_n`
- `ratio_met`
- `cod_met`
- `prd_met`
- `prb_met`
- `mki_met`
- `vertical_equity_met`
- `within_05_pct`
- `within_10_pct`
- `within_15_pct`
- `within_20_pct`

### Implementation

- Tables should be constructed using our newly implemented dbt Python model
  support. Preference to use PySpark for speed of construction, Pandas as
  a fallback
- Tables should be materialized as tables, i.e. saved as Parquet files. It may
  be worth experimenting with file partitioning, assuming the files generated
  are sufficiently large
- Tests and documentation should be defined as usual, in the `schema.yml` file
  of each dbt model
- Table should use a new prefix `sot_` in their names
- Tables should be constructed such that adding new columns and/or groups is
  easy and non-breaking
- Major changes to the tables, such as changes to ratio statistic or sales
  validation methodologies, should be accompanied by incrementing the `method`
  column (e.g. `v1` becomes `v2`). The prior method version should *only be
  deprecated with the consent of all major users*. Method changes should be
  documented in the table documentation

### Testing

All new tables should be accompanied by dbt data tests (and unit tests where
appropriate). Data sets should ensure the following *at minimum*:

- Geographies and groups contain their representative sets, i.e. the
  `municipality` geography contains all county municipalities, and `class`
  contains all CCAO class codes
- Values fall within expected ranges
  - Counts are always greater than or equal to 0
  - FMVs and AVs are constrained by
- No null, inf, or NaN values are present (where appropriate)
- Tables are unique by primary key
- Tests are run daily (as part of the `build-daily-dbt-models` workflow)

## Example use cases

The Data Department commonly receives questions like, "What is the count of
homeowner exemptions by Chicago ward?" The new tables would let us quickly and
confidently answer that question using the following query:

```sql
SELECT *
FROM reporting.sot_tax_exe
WHERE year = '2022'
AND geography_type = 'ward'
AND geography_year = '2023'
AND group_type = 'no_group'
```

Another common question is, "What is the percentage change in assessed value
for small single-family homes in district X?"

```sql
SELECT *
FROM reporting.sot_assessment_roll
WHERE year = '2024'
AND geography_type = 'cook_commissioner_district'
AND geography_id = '2'
AND geography_year = '2023'
AND group_type = 'class'
AND group_id IN ('202', '203', '204')
AND stage = 'mailed'
```

## Open Questions

### Mutability

These reporting tables will be mutable because the underlying data is
constantly changing. Even for past assessment years, there still a chance the
aggregate statistics would change thanks to CofEs, changes to sales validation
results, etc.

If immutability/historical values are required, then there are a few possible
setups:

- We could try to use [dbt snapshots](https://docs.getdbt.com/docs/build/snapshots)
  to capture point-in-time versions of the tables. A big caveat here is that
  snapshots to not yet work with Python dbt models.
- We could use dbt snapshots to take snapshots of the underlying data, then
  use that to construct the views.
- We could manually capture the state of the tables each day and save them to
  a history table.

This all begs the question: is immutability required for these tables? Or is it
okay for the values to change slightly over time?

### Other questions

- How exactly does this fit into the overall long-term reporting strategy of
  the office?
- How much of these views will be made public? If they are made public, how
  do we simplify them for public consumption?
