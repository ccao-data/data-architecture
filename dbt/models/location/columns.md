## access_cmap_walk_id

{% docs column_access_cmap_walk_id %}
CMAP ON TO 2050 grid identifier for walk scores
{% enddocs %}

## access_cmap_walk_nta_score

{% docs column_access_cmap_walk_nta_score %}
CMAP walkability score for a given PIN, excluding transit walkability.

Taken from CMAP's ON TO 2050 walkability layer
{% enddocs %}

## access_cmap_walk_total_score

{% docs column_access_cmap_walk_total_score %}
CMAP walkability score for a given PIN, including transit walkability.

Taken from CMAP's ON TO 2050 walkability layer
{% enddocs %}

## census_block_geoid

{% docs column_census_block_geoid %}
15-digit Census block GEOID
{% enddocs %}

## census_block_group_geoid

{% docs column_census_block_group_geoid %}
12-digit Census block group GEOID
{% enddocs %}

## census_congressional_district_geoid

{% docs column_census_congressional_district_geoid %}
4-digit ACS/Census congressional district GEOID
{% enddocs %}

## census_county_subdivision_geoid

{% docs column_census_county_subdivision_geoid %}
10-digit ACS/Census county subdivision GEOID
{% enddocs %}

## census_place_geoid

{% docs column_census_place_geoid %}
7-digit ACS/Census place GEOID
{% enddocs %}

## census_puma_geoid

{% docs column_census_puma_geoid %}
7-digit ACS/Census Public Use Microdata Area (PUMA) GEOID
{% enddocs %}

## census_school_district_elementary_geoid

{% docs column_census_school_district_elementary_geoid %}
7-digit ACS/Census school district (elementary) GEOID
{% enddocs %}

## census_school_district_secondary_geoid

{% docs column_census_school_district_secondary_geoid %}
7-digit ACS/Census school district (secondary) GEOID
{% enddocs %}

## census_school_district_unified_geoid

{% docs column_census_school_district_unified_geoid %}
7-digit ACS/Census school district (unified) GEOID.

Unified districts combine elementary and secondary (high school) into a
single district.
{% enddocs %}

## census_state_representative_geoid

{% docs column_census_state_representative_geoid %}
5-digit Illinois state representative district GEOID
{% enddocs %}

## census_state_senate_geoid

{% docs column_census_state_senate_geoid %}
5-digit Illinois state senate district GEOID
{% enddocs %}

## census_tract_geoid

{% docs column_census_tract_geoid %}
11-digit ACS/Census tract GEOID
{% enddocs %}

## census_zcta_geoid

{% docs column_census_zcta_geoid %}
5-digit Census ZIP Code Tabulation Area (ZCTA) GEOID.

ZCTAs are generalized areal representations of the geographic
extent and distribution of the point-based ZIP codes.
{% enddocs %}

## chicago_community_area_name

{% docs column_chicago_community_area_name %}
Chicago community area name.

For example, `HYDE PARK`, `LOGAN SQUARE`, `LITTLE VILLAGE`, etc.
{% enddocs %}

## chicago_community_area_num

{% docs column_chicago_community_area_num %}
2-digit Chicago community area number
{% enddocs %}

## chicago_industrial_corridor_name

{% docs column_chicago_industrial_corridor_name %}
Chicago industrial corridor name
{% enddocs %}

## chicago_industrial_corridor_num

{% docs column_chicago_industrial_corridor_num %}
2-digit Chicago industrial corridor number
{% enddocs %}

## chicago_police_district_num

{% docs column_chicago_police_district_num %}
2-digit Chicago police district number
{% enddocs %}

## combined_municipality

{% docs column_combined_municipality %}
This is a column which combines tax_municipality_name and cook_municipality_name.
This is because tax_municipality_name does not have values for Cicero and 
for some new parcels. 
Steps:
1: We take the value of tax_municipality_name if
the cardinality is >0 (non-NULL).
2: We check if the PIN is in Cicero in cook_municipality_name. There are two
names for this Village and Town of Cicero. These switch over completely in 2007.
Because of this, we coalesce with either of these values.
3: We check if the PIN is new in iasworld.pardat. If it is, we coalesce the 
value from cook_municipality_name. We then run these values through a
crosswalk file to make sure they align with tax_municipality_name.
4: We return the remaining NULL values from tax_municipality_name

{% enddocs %}

## cook_board_of_review_district_num

{% docs column_cook_board_of_review_district_num %}
1-digit Cook County Board of Review district number
{% enddocs %}

## cook_commissioner_district_num

{% docs column_cook_commissioner_district_num %}
2-digit Cook County commissioner district number
{% enddocs %}

## cook_judicial_district_num

{% docs column_cook_judicial_district_num %}
2-digit Cook County judicial district number
{% enddocs %}

## cook_municipality_name

{% docs column_cook_municipality_name %}

Cook county municipality name from Cook County GIS files
{% enddocs %}

## cook_municipality_num

{% docs column_cook_municipality_num %}
Cook county municipality number. This does not correspond with tax_municipality_num
{% enddocs %}

## econ_coordinated_care_area_num

{% docs column_econ_coordinated_care_area_num %}
3-digit Coordinated Care area number
{% enddocs %}

## econ_enterprise_zone_num

{% docs column_econ_enterprise_zone_num %}
3-digit Enterprise Zone number
{% enddocs %}

## econ_industrial_growth_zone_num

{% docs column_econ_industrial_growth_zone_num %}
3-digit Industrial Growth Zone number
{% enddocs %}

## econ_qualified_opportunity_zone_num

{% docs column_econ_qualified_opportunity_zone_num %}
11-digit Qualified Opportunity Zone number
{% enddocs %}

## env_airport_noise_dnl

{% docs column_env_airport_noise_dnl %}
O'Hare and Midway noise, measured as DNL.

DNL measures the total cumulative sound exposure over a 24-hour period.
Here DNL is imputed using physical models or a kriging surface based on
noise data from monitors around each airport. Noise monitor data retrieved
from the Chicago Department of Aviation
{% enddocs %}

## env_flood_fema_sfha

{% docs column_env_flood_fema_sfha %}
FEMA Special Flood Hazard Area, derived from spatial intersection
with FEMA floodplain maps.

Taken from FEMA site for 2021 only
{% enddocs %}

## env_flood_fs_factor

{% docs column_env_flood_fs_factor %}
First Street flood factor

The flood factor is a risk score, where 10 is the highest risk and 1 is
the lowest risk. Pulled from 2019 First Street extract provided to the CCAO
{% enddocs %}

## env_flood_fs_risk_direction

{% docs column_env_flood_fs_risk_direction %}
First Street risk direction.

Positive scores indicate increasing risk of flood, negative scores indicate
decreasing risk of flood, 0 indicates no movement of risk. Pulled from 2019
First Street extract provided to the CCAO
{% enddocs %}

## env_ohare_noise_contour_half_mile_buffer_bool

{% docs column_env_ohare_noise_contour_half_mile_buffer_bool %}
Indicator for properties within half a mile of the O'Hare
65 DNL noise buffer (OMP)

Ingested from oharenoise.org
{% enddocs %}

## env_ohare_noise_contour_no_buffer_bool

{% docs column_env_ohare_noise_contour_no_buffer_bool %}
Indicator for properties within the O'Hare 65 DNL noise buffer (OMP).

Ingested from oharenoise.org
{% enddocs %}

## misc_subdivision_id

{% docs column_misc_subdivision_id %}
Subdivision ID.

Exact form of this depends on the area
{% enddocs %}

## school_elementary_district_geoid

{% docs column_school_elementary_district_geoid %}
School district (elementary) GEOID.

Derived from Cook County and City of Chicago shapefiles. Chicago Public
Schools are associated with attendance areas where suburban schools are
associated with districts
{% enddocs %}

## school_elementary_district_name

{% docs column_school_elementary_district_name %}
School district (elementary) name.

Derived from Cook County and City of Chicago shapefiles. Chicago Public
Schools are associated with attendance areas where suburban schools are
associated with districts
{% enddocs %}

## school_secondary_district_geoid

{% docs column_school_secondary_district_geoid %}
School district (secondary) GEOID.

Derived from Cook County and City of Chicago shapefiles. Chicago Public
Schools are associated with attendance areas where suburban schools are
associated with districts
{% enddocs %}

## school_secondary_district_name

{% docs column_school_secondary_district_name %}
School district (secondary) name.

Derived from Cook County and City of Chicago shapefiles. Chicago Public
Schools are associated with attendance areas where suburban schools are
associated with districts
{% enddocs %}

## school_unified_district_geoid

{% docs column_school_unified_district_geoid %}
School district (unified) GEOID.

Derived from Cook County and City of Chicago shapefiles. Chicago Public
Schools are associated with attendance areas where suburban schools are
associated with districts
{% enddocs %}

## school_unified_district_name

{% docs column_school_unified_district_name %}
School district (unified) name.

Derived from Cook County and City of Chicago shapefiles. Chicago Public
Schools are associated with attendance areas where suburban schools are
associated with districts
{% enddocs %}

## school_year

{% docs column_school_year %}
School year timespan associated with corresponding boundaries
{% enddocs %}

## tax_district_name

{% docs column_tax_district_name %}
Taxing district name, as seen on Cook County tax bills.

The value is retrieved from [PTAXSIM](https://github.com/ccao-data/ptaxsim)
which performs some cleaning and de-duping.

Note that these values are stored as an array because PINs can occasionally
belong to more than one taxing district of the same type
{% enddocs %}

## tax_district_num

{% docs column_tax_district_num %}
9-digit taxing district ID number.

This is the number used by the Cook County Clerk to uniquely identify each
individual taxing district in the county. The value is retrieved from
[PTAXSIM](https://github.com/ccao-data/ptaxsim) which performs some cleaning
and de-duping.

Note that these values are stored as an array because PINs can occasionally
belong to more than one taxing district of the same type
{% enddocs %}

## ward_name

{% docs column_ward_name %}
Ward name.

Only for municipalities with wards, which currently only includes Chicago and Evanston
{% enddocs %}

## ward_num

{% docs column_ward_num %}
2-digit ward ID number.

Only for municipalities with wards, which currently only includes Chicago and Evanston
{% enddocs %}
