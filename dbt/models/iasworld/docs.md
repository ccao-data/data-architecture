# aasysjur

{% docs table_aasysjur %}
Control table for `asmt` admin module - multi-jurisdiction version.

**Primary Key**: `jur`
{% enddocs %}

# addn

{% docs table_addn %}
Residential additions. Stores data about Home Improvement Exemptions (HIEs)
and other information about `iasworld.dweldat` properties.

**Primary Key**: `jur`, `taxyr`, `parid`, `card`, `lline`
{% enddocs %}

# addrindx

{% docs table_addrindx %}
Address lookup tied to a table/column/line.

**Primary Key**: `jur`, `taxyr`, `parid`, `card`, `lline`, `tble`, `tabseq`
{% enddocs %}

# aprval

{% docs table_aprval %}
Summary of market values (no assessed values).

**Primary Key**: `jur`, `taxyr`, `parid`
{% enddocs %}

# asmt_all

{% docs table_asmt_all %}
Main assessment table, union of `iasworld.asmt` and `iasworld.asmt_hist`.
Provides the latest assessed value for each PIN and used heavily in
Data Department views.

### Nuance

- Unlike most `iasworld` tables, this table _does not_ use `cur = 'Y'` to
  identify the most recent record (it uses `procname` instead).

**Primary Key**: `jur`, `rolltype`, `valclass`, `valyear`, `distcode`,
`seq` `taxyr`, `parid`, `card`
{% enddocs %}

# asmt_hist

{% docs table_asmt_hist %}
History table for assessments that stores non-current records. Same schema
as `iasworld.asmt_all`.

**Primary Key**: `jur`, `rolltype`, `valclass`, `valyear`, `distcode`,
`seq` `taxyr`, `parid`, `card`
{% enddocs %}

# cname

{% docs table_cname %}
Owner code table for owner names.

**Primary Key**: `jur`, `ownnum`
{% enddocs %}

# comdat

{% docs table_comdat %}
Major building table for commercial properties, which includes basically any
non-class 2 property besides land.

**Primary Key**: `jur`, `taxyr`, `parid`, `card`
{% enddocs %}

# comnt

{% docs table_comnt %}
*Unlimited* notes table.

**Primary Key**: `jur`, `parid`, `code`, `comtno`, `caseno`
{% enddocs %}

# cvleg

{% docs table_cvleg %}
Conveyance/sales information. Holds additional info about each sale, such
as PTAX-203 data.

**Primary Key**: `jur`, `taxyr`, `parid`, `conveyno`
{% enddocs %}

# cvown

{% docs table_cvown %}
Conveyance/owner information.

**Primary Key**: `jur`, `taxyr`, `conveyno`, `ownseq`
{% enddocs %}

# cvtran

{% docs table_cvtran %}
Header table for conveyance transactions.

**Primary Key**: `jur`, `taxyr`, `conveyno`
{% enddocs %}

# dedit

{% docs table_dedit %}
CCAO land use code descriptions + edit code descriptions.

**Primary Key**: `tbl1`, `fld1`, `tbl2`, `fld2`, `val1`, `val2`
{% enddocs %}

# dweldat

{% docs table_dweldat %}
Residential property characteristics table (excluding residential
condominiums).

**Primary Key**: `jur`, `taxyr`, `parid`, `lline`
{% enddocs %}

# enter

{% docs table_enter %}
Field inspection tracking table.

**Primary Key**: `jur`, `taxyr`, `parid`
{% enddocs %}

# exadmn

{% docs table_exadmn %}
Exemption applications table.

**Primary Key**: `jur`, `taxyr`, `parid`, `caseno`
{% enddocs %}

# exapp

{% docs table_exapp %}
Exemption applicant information table.

**Primary Key**: `jur`, `taxyr`, `parid`, `caseno`
{% enddocs %}

# excode

{% docs table_excode %}
Exemption code table. In order to match these codes to our commonly defined
exemptions we aggregate them as below:

- homestead exemption
  - `C-HO: GENERAL HOMESTEAD EXEMPTION - COOP`
  - `HO: GENERAL HOMESTEAD EXEMPTION`
- senior homestead exemption
  - `C-SR: SENIOR HOMESTEAD - COOP`
  - `SR: SENIOR HOMESTEAD`
- senior freeze exemption
  - `C-SF: SENIOR FREEZE - COOP`
  - `SF: SENIOR FREEZE`
- long-time homeowner exemption
  - `C-LT: LONG-TIME HOMEOWNER - COOP`
  - `LT: LONG-TIME HOMEOWNER`
- returning veterans exemption
  - `C-RTV: RETURNING VETERAN - COOP`
  - `RTV: RETURNING VETERANS`
- disabled persons exemption
  - `C-DP: DISABLED PERSONS - COOP`
  - `DP: DISABLED PERSON HOMESTD EX`
- veterans with disabilites exemption
  - `C-DV1: DISABLED VETERAN 30-49% - COOP`
  - `C-DV2: DISABLED VETERAN 50-69% - COOP`
  - `DV1: DISABLED VETERAN 30-49%`
  - `DV2: DISABLED VETERAN 50-69%`
  - `DV3: DISABLED VETERAN 70% OR GREATER`
  - `DV3-M: DV3 MULTI-UNIT`
  - `DV4: DISABLED VETERAN 100%, TOTALLY AND PERMANENTLY DISABLED`
  - `DV4-M: DV4 MULTI-UNIT`
- municipality-built gradual exemption
  - `MUNI: MUNICIPALITY-BUILT GRADUAL EXEMPTION`
- world war 2 veteran exemption
  - `WW2: WORLD WAR 2 VETERAN 100%`

NOTE: Some exemption codes are not included in these aggregations since they
apply to AV rather than EAV, the most notable being
`DV5: DISABLED VETERAN IDOR 100%`. For more information available see [exemptions.xlsx](https://cookcounty.sharepoint.com/:x:/r/sites/Data-Assessor/Shared%20Documents/General/exemptions.xlsx).

**Primary Key**: `jur`, `taxyr`, `excode`
{% enddocs %}

# exdet

{% docs table_exdet %}
Exemption administration detail lines tied to all or part of a parcel.

**Primary Key**: `jur`, `taxyr`, `parid`, `card`, `lline`, `excode`,
`lineno`, `scrn`, `fld`
{% enddocs %}

# htagnt

{% docs table_htagnt %}
Agent/attorney code maintenance and name storage.

**Primary Key**: `jur`, `agent`
{% enddocs %}

# htdates

{% docs table_htdates %}
Hearing date and note maintenance.

**Primary Key**: `jur`, `taxyr`, `parid`, `heartyp`, `subkey`
{% enddocs %}

# htpar

{% docs table_htpar %}
Main appeal tracking table. Used in the construction of Data Department
appeal views.

**Primary Key**: `jur`, `taxyr`, `parid`, `heartyp`, `subkey`,
`caseno`, `hrreas`
{% enddocs %}

# land

{% docs table_land %}
Main land information table. Stores the land size, number of lines, proration,
etc. of all parcels.

**Primary Key**: `jur`, `taxyr`, `parid`, `lline`
{% enddocs %}

# legdat

{% docs table_legdat %}
Parcel legal descriptions and addresses. Stores township, property address,
and other location information about each parcel.

**Primary Key**: `jur`, `taxyr`, `parid`
{% enddocs %}

# lpmod

{% docs table_lpmod %}
Calp model.

**Primary Key**: `jur`, `ltype`, `lcode`, `ver`, `nmod`, `zmod`, `lmod`,
`smod`, `umod`
{% enddocs %}

# lpnbhd

{% docs table_lpnbhd %}
Neighborhood parameter table for calp, valuation models, etc.

**Primary Key**: `jur`, `ver`, `nbhd`
{% enddocs %}

# oby

{% docs table_oby %}
Outbuilding table. This is the main storage table for condo unit-level data.
It also stores other miscellaneous sub-PIN information like HIEs.

**Primary Key**: `jur`, `taxyr`, `parid`, `card`, `lline`
{% enddocs %}

# owndat

{% docs table_owndat %}
Property owner information such as name and mailing address.

### Nuance

- Mailing addresses and owner names have not been regularly updated since 2017.

**Primary Key**: `jur`, `taxyr`, `parid`
{% enddocs %}

# pardat

{% docs table_pardat %}
Universe of all Cook County parcels for each tax year. This is *the* base
table, along with `iasworld.legdat`.

**Primary Key**: `jur`, `taxyr`, `parid`
{% enddocs %}

# permit

{% docs table_permit %}
Building permit ingestion and processing.

See [`default.vw_pin_permit`](/#!/model/model.ccao_data_athena.default.vw_pin_permit)
for a view of this table that selects active rows and relevant columns.

**Primary Key**: `parid`, `num`
{% enddocs %}

# rcoby

{% docs table_rcoby %}
Outbuilding cost table.

**Primary Key**: `ver`, `code`
{% enddocs %}

# sales

{% docs table_sales %}
Main table for tracking property sales. Sales are ingested and populated
manually from IDOR's MyDec platform.

**Primary Key**: `jur`, `parid`, `saledt`, `instruno`
{% enddocs %}

# splcom

{% docs table_splcom %}
Split tracking AKA divisions table.

**Primary Key**: `jur`, `taxyr`, `splitno`
{% enddocs %}

# valclass

{% docs table_valclass %}
Class definitions and assessment levels.

**Primary Key**: `jur`, `taxyr`, `rolltype`, `class`, `luc`, `vclass`
{% enddocs %}
