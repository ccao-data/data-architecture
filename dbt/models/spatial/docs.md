# bike_trail

{% docs table_bike_trail %}
Bike trail locations.

**Geometry:** `MULTILINESTRING`
{% enddocs %}

# board_of_review_district

{% docs table_board_of_review_district %}
Board of Review political district boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# building_footprint

{% docs table_building_footprint %}
National building footprints file trimmed to Cook County.

Sourced from the
[Microsoft USBuildingFootprints project](https://github.com/microsoft/USBuildingFootprints).

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# cemetery

{% docs table_cemetery %}
Cemetery boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# census

{% docs table_spatial_census %}
Census geographic boundaries for _all_ geography types.

Filter by the geography and year columns to get geometry for a specific
Census geography. Sourced from Tiger/LINE files via the `tigris` R package.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# central_business_district

{% docs table_spatial_central_business_district %}
Chicago's central business district boundary.

Sourced from the City of Chicago Data Portal.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# coastline

{% docs table_coastline %}
Lake Michigan coastline.

Sourced from Census hydrology files.

**Geometry:** `LINESTRING`
{% enddocs %}

# commissioner_district

{% docs table_commissioner_district %}
Cook County Commissioner District political boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# community_area

{% docs table_community_area %}
Chicago community area boundaries.

Sourced from the City of Chicago Data Portal.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# community_college_district

{% docs table_community_college_district %}
Cook County community college district boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# congressional_district

{% docs table_congressional_district %}
National Congressional district political boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# coordinated_care

{% docs table_coordinated_care %}
Cook County coordinated care area boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# corner

{% docs table_corner %}
CCAO corner lot indicator. Determined algorithmically by unobstructed access to
perpendicular streets.

The algorithm is:

1. Find the minimum rectangle that bounds the parcel, then use that
   rectangle to determine the parcel's orientation and length. These values
   are used in the next step to draw a cross on the parcel.
2. Draw a cross that cuts the parcel in four and extends out slightly
   beyond the minimum rectangle of the parcel. We will look for parcels and
   streets that intersect this cross in order to determine neighbors. Use the
   bearing and length calculated in the previous step to draw the cross (i.e
   the long side of the cross is parallel to the minimum bounding rectangle).
3. Find the arms of each cross which intersect neighboring parcels, where those
   neighboring parcels _also_ touch the cross-originating PIN. The idea here is
   that cross arms that intersect neighboring (touching) parcels are likely to
   be pointed at a building, rather than a street. Delete these arms from the
   cross.
4. Find which remaining cross segments intersect streets, and keep only
   segments that _do_ intersect. This removes segments pointed at buildings,
   empty space, etc.
5. Calculate the angle of remaining cross segments in order to filter
   out segments that are not at right angles. If a parcel >= 3 cross segments
   remaining, it is a corner. If a parcel has 2 cross segments remaining and
   they form a 90 degree angle, it is a corner. All other cases are not
   corners.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# county

{% docs table_county %}
Full Cook County boundary.

Sourced from Cook County GIS.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# enterprise_zone

{% docs table_enterprise_zone %}
Illinois
[Enterprise Zone](https://www.cookcountyil.gov/service/illinois-enterprise-zones)
boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# fire_protection_district

{% docs table_fire_protection_district %}
Fire protection taxing district boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# flood_fema

{% docs table_flood_fema %}
FEMA [Special Flood Hazard Area](https://www.fema.gov/flood-maps) boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# geojson

{% docs table_geojson %}
Raw GeoJSON representation of certain political and taxing district boundaries
for export to Tableau as static map layers.

**Geometry:** `Mixed types`
{% enddocs %}

# golf_course

{% docs table_golf_course %}
Golf course boundaries.

Golf course locations sourced from a combination of Cook County GIS
data and tagged OpenStreetMap amenities.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# grocery_store

{% docs table_grocery_store %}
Grocery stores. Locations sourced from OpenStreetMap (OSM).

	OSM tags include:

	- `shop=supermarket`
	- `shop=wholesale`
	- `shop=greengrocer`

	Only attributes with valid names are kept.

**Geometry:** `POINT`
{% enddocs %}

# hospital

{% docs table_hospital %}
Hospital point locations.

Hospital locations sourced from Cook County GIS.

**Geometry:** `POINT`
{% enddocs %}

# hydrology

{% docs table_hydrology %}
Water locations of any type, including drainage, lakes, canals, etc.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# industrial_corridor

{% docs table_industrial_corridor %}
City of Chicago designated
[Industrial Corridor](https://www.chicago.gov/dam/city/depts/zlup/Sustainable_Development/Publications/Chicago_Sustainable_Industries/CSI_3.pdf)
boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# industrial_growth_zone

{% docs table_industrial_growth_zone %}
Cook County
[Industrial Growth Zone](https://www.cookcountyil.gov/service/growth-zones)
boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# judicial_district

{% docs table_judicial_district %}
Cook County judicial district political boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# library_district

{% docs table_library_district %}
Cook County library taxing district boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# major_road

{% docs table_major_road %}
Major road locations.

Major roads sourced from OpenStreetMap (OSM).
Major roads include any OSM ways tagged with
`highway/motorway`, `highway/trunk`, or `highway/primary`

This data covers major roads data from 2014 onwards and uses an additive
approach for data integration. Starting from 2014, each year's data builds
upon the previous year, with new major road additions being added to the
existing dataset.

**Geometry:** `MULTILINESTRING`
{% enddocs %}

# midway_noise_monitor

{% docs table_midway_noise_monitor %}
Midway airport noise monitor locations and statistics.

Sourced from the Chicago Department of Aviation.

**Geometry:** `POINT`
{% enddocs %}

# municipality

{% docs table_municipality %}
Cook County municipality taxing district boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# neighborhood

{% docs table_neighborhood %}
Cook County Assessor neighborhood boundaries.

These were reconstructed manually using parcel-level orthogonalization
and extracting from old PDF maps.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# ohare_noise_contour

{% docs table_ohare_noise_contour %}
O'Hare 60 DNL noise contour (O'Hare Modernization Project (OMP) version).

**Geometry:** `POLYGON`
{% enddocs %}

# ohare_noise_monitor

{% docs table_ohare_noise_monitor %}
O'Hare airport noise monitor locations and statistics.

Sourced from the Chicago Department of Aviation and
[oharenoise.org](https://oharenoise.org).

**Geometry:** `POINT`
{% enddocs %}

# parcel

{% docs table_parcel %}
Parcel (PIN) polygons and centroids.

This table is the source of truth for parcel locations and served as the
base for _all_ spatial joins. It is created directly from the parcel file
created and maintained by Cook County GIS. Parcel IDs (PINs) and parcel
boundaries are updated by the Assessor and Clerk.

**Geometry:** `MULTIPOLYGON` (centroids also available)
{% enddocs %}

# park

{% docs table_park %}
Park boundaries.

Park locations sourced from OpenStreetMap using the tag `leisure/park`.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# park_district

{% docs table_park_district %}
Cook County park taxing district boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# police_district

{% docs table_police_district %}
Chicago police district boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# qualified_opportunity_zone

{% docs table_qualified_opportunity_zone %}
National
[Qualified Opportunity Zone](https://www.irs.gov/credits-deductions/opportunity-zones-frequently-asked-questions)
boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# railroad

{% docs table_railroad %}
Railroad rail locations.

Includes any type of rail (CTA, Metra, freight, etc.).

Rail locations sourced from Cook County GIS.

**Geometry:** `MULTILINESTRING`
{% enddocs %}

# road

{% docs table_road %}

Illinois Department of Transportation data source from
[https://apps1.dot.illinois.gov/gist2/](https://apps1.dot.illinois.gov/gist2/).
Data focuses on five features; lanes, speed limits, traffic count, road type,
and surface type. Some columns are not present in all years of data (for example
speed limit in 2012) Data for columns is not universally present so we average
numeric values for roads which overlap and have a matching name. For example,
if segment B touches segment A and C with speed limits of 25 and 30, the speed
limit for segment B will be 27.5.

**Geometry:** `MULTILINESTRING`
{% enddocs %}

# sanitation_district

{% docs table_sanitation_district %}
Cook County sanitation taxing district boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# school_district

{% docs table_school_district %}
Cook County school district taxing district boundaries.

Sourced from Cook County GIS.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# school_location

{% docs table_school_location %}
Individual school point locations.

School locations (lat/lon) are sourced from
[GreatSchools.org](https://greatschools.org).

**Geometry:** `POINT`
{% enddocs %}

# secondary_road

{% docs table_secondary_road %}

Secondary road locations.

Secondary roads sourced from OpenStreetMap (OSM).
Secondary roads include any OSM ways tagged with
`highway/secondary`

**Geometry:** `MULTILINESTRING`
{% enddocs %}

# special_service_area

{% docs table_special_service_area %}
Cook County Special Service Area (SSA) boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# stadium

{% docs table_stadium %}
Transformed version of the `spatial.stadium_raw` table containing binary geometry columns of stadium locations.

**Geometry:** `POINT`
{% enddocs %}

# state_representative_district

{% docs table_state_representative_district %}
Illinois state representative district political boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# state_senate_district

{% docs table_state_senate_district %}
Illinois state senate district political boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# subdivision

{% docs table_subdivision %}
Cook County subdivision boundaries.

Sourced from Cook County GIS.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# tif_district

{% docs table_tif_district %}
Cook County Tax Increment Finance district boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# township

{% docs table_township %}
Cook County Assessor township boundaries.

Includes townships within the City of Chicago, which are technically defunct.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}


# transit_dict

{% docs table_transit_dict %}
Dictionary to cleanup transit route and stop names.
{% enddocs %}

# transit_route

{% docs table_transit_route %}
Transit (CTA, PACE, Metra) route locations.

Route locations are sourced from the GTFS feeds of their respective agencies.

**Geometry:** `MULTILINESTRING`
{% enddocs %}

# transit_stop

{% docs table_transit_stop %}
Transit (CTA, PACE, Metra) stop locations.

Stop locations are sourced from the GTFS feeds of their respective agencies.

**Geometry:** `POINT`
{% enddocs %}

# walkability

{% docs table_walkability %}
[CMAP walkability grid](https://www.cmap.illinois.gov/2050/maps/walkability)
polygons.

**Geometry:** `POLYGON`
{% enddocs %}

# ward

{% docs table_ward %}
Combined ward political boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# ward_chicago

{% docs table_ward_chicago %}
City of Chicago ward political boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}

# ward_evanston

{% docs table_ward_evanston %}
City of Evanston ward political boundaries.

**Geometry:** `MULTIPOLYGON`
{% enddocs %}
