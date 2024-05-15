# aprval_reascd

{% docs table_aprval_reascd %}
Reason codes and descriptions for changes to assessed value. The data feeding
this table is a static parquet file from the data warehouse.

**Primary Key**: `reascd`
{% enddocs %}

# class_dict

{% docs table_class_dict %}
Classification codes and descriptions for real property. Derived from the
[public PDF](https://prodassets.cookcountyassessor.com/s3fs-public/form_documents/Definitions%20for%20Classifications_2023.pdf).

**Primary Key**: `class_code`
{% enddocs %}

# commercial_valuation

{% docs table_commercial_valuation %}
CCAO commercial valuation data, aggregated from the commercial team spreadsheets
[available on the Assessor's site](https://www.cookcountyassessor.com/valuation-reports).

**Primary Key**: `keypin`, `year`
{% enddocs %}

# corner_lot

{% docs table_corner_lot %}
CCAO corner lot indicator. Determined algorithmically by unobstructed access to
perpidincular streets.

**Primary Key**: `pin10`
{% enddocs %}

# hie

{% docs table_hie %}
Legacy Home Improvement Exemption (HIE) data pulled from the AS/400.

This table exists in order to apply HIE characteristics to sales for modeling,
but it will naturally become deprecated as the remaining legacy HOEs expire.

**Primary Key**: `pin`, `year`
{% enddocs %}

# land_nbhd_rate

{% docs table_land_nbhd_rate %}
Neighborhood-level land rates provided by Valuations.

These rates are applied during the modeling process in order to disaggregate
the value of land from the total PIN value. They are provided yearly prior
to modeling.

**Primary Key**: `town_nbhd`, `year`
{% enddocs %}

# land_site_rate

{% docs table_land_site_rate %}
PIN-level land value provided by Valuations.

These PIN-level flat land values were used in the 2021 reassessment year.
They applied specifically to townhome complexes. They are deprecated as of 2022.

**Primary Key**: `pin`, `year`
{% enddocs %}

# pin_condo_char

{% docs table_pin_condo_char %}
Condominium characteristic data collected by Valuations / Data Integrity at
the unit level (14-digit PIN).

Collected yearly prior to each triennial modeling/reassessment cycle. Sourced
from spreadsheets provided by Valuations for each township.

**Primary Key**: `pin`, `year`
{% enddocs %}

# pin_nonlivable

{% docs table_pin_nonlivable %}
Flags for non-livable condominium units such as common areas, parking spaces,
and storage areas.

Collected yearly from Valuations via spreadsheets.

**Primary Key**: `pin`, `year`
{% enddocs %}

# vw_python_model_dependency

{% docs view_python_model_dependency %}
Internal view recording the path where S3 dependency bundles are stored for
Python models in the current environment.

This view is a thin wrapper around the `get_s3_dependency_dir` macro. It acts
as a workaround for the fact that Python models
[do not yet support code
reuse](https://docs.getdbt.com/docs/build/python-models#code-reuse),
so there's no way to call a version of `get_s3_dependency_dir()` in a
Python model or even pass its return value in as a config variable. This
solution works because views can be referenced inside the context of a Python
model via the `dbt.ref()` method even though macros cannot.
{% enddocs %}
