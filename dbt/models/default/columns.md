## permit_address_full

{% docs column_permit_address_full %}
Address where permitted work will take place.

This field combines the "mailing address" field in the source data
with the concatenated output of the address component fields like
`address_street_dir`, `address_street_name`, etc. We prefer the
mailing address if it exists, and otherwise we substitute the
concatenated address components.

This field usually includes the municipality name for municipalities
outside Chicago. If the municipality name is missing, the address
is either in Chicago, or we generated it based on the concatenation
of the address component fields, which do not include municipality
name.

Municipalities are responsible for filling out this field when submitting a
permit.
{% enddocs %}

## permit_amount

{% docs column_permit_amount %}
Amount that permitted work is estimated to cost, rounded to the nearest dollar.

Municipalities are responsible for filling out this field when submitting a
permit. For the most part they do, but this field can be null in rare cases
where they do not.
{% enddocs %}

## permit_assessable

{% docs column_permit_assessable %}
Whether the permitted work will affect the property's assessed value.

CCAO permit specialists set this field based on the `work_description` during
the permit review process. If a permit specialist determines that the field is
assessable, they will mark it as such, and the permit will be sent to a field
inspector for field check. The field inspector will then make any necessary
changes to the parcel's characteristics, or will mark the permit for recheck at
a later date if work is still ongoing.

Possible values for this variable are:

- null: The permit has not been reviewed yet
- `A`: Assessable
- `N`: Non-Assessable
{% enddocs %}

## permit_date_submitted

{% docs column_permit_date_submitted %}
Date that the municipality submitted the permit to the CCAO.

This field appears inconsistently in the data, and occasionally represents a
date that is before `date_issued`, which should not be possible. As such, we
prefer using `date_issued` for the purposes of establishing permit timelines.
{% enddocs %}

## permit_date_updated

{% docs column_permit_date_updated %}
Most recent date that the permit was updated in the CCAO system.

If the `Latest Update Date` field in iasWorld is not null, we use its value
to populate this field. `Latest Update Date` requires users to manually fill it
out, however, so it is often null. If it's null, this field will use the value
of the `wen` field that iasWorld automatically updates when a user edits a
record.
{% enddocs %}

## permit_estimated_date_of_completion

{% docs column_permit_estimated_date_of_completion %}
Estimated date that the permitted work will be complete.

Municipalities are responsible for filling out this field when submitting a
permit, but they almost never do, so it is mostly null. As such, we do not rely
on it for establishing permit timelines.
{% enddocs %}

## permit_filing_type

{% docs column_permit_filing_type %}
Deprecated.

Possible values for this variable are:

- null
- `PM`
{% enddocs %}

## permit_improvement_type

{% docs column_permit_improvement_type %}
Type of improvement indicated by the permit.

The CCAO requests that municipalities provide improvement codes as a way of
categorizing the specific type of planned work that the permit represents.
Not all municipalities fill out this field correctly, however, so the
`work_description` field should be considered the source of truth about the
type of work that a permit represents.

Possible values for this variable are:

- `110 - NEW CONSTRUCTION - MAJOR IMPRVT`
- `111 - NEW BUILDING`
- `112 - ADDITIONS`
- `113 - DORMERS`
- `114 - OTHER - MAJOR NEW CONSTRUCTION`
- `130 - NEW CONSTRUCTION - MINOR IMPRVMT`
- `131 - SWIMMING POOL - TENNIS COURT`
- `132 - DRIVEWAYS - PATIOS - WOOD DECK`
- `133 - FENCING (and Gates)`
- `134 - GARAGE CARPORTS BARNS`
- `135 - OTHER - MINOR NEW CONSTRUCTION`
- `151 - BASEMENT ROOM - REC ROOM`
- `152 - ATTIC ROOM - BATHROOMS`
- `153 - FIREPLACE-CENTRAL AIR CONDITIONING`
- `154 - OTHER - REMODELING`
- `155 - ???`
- `156 - REMODELING - INTERIOR ONLY`
- `157 - REMODELING - EXTERIOR ONLY`
- `171 - MAJOR IMPROVEMENT - WRECK`
- `172 - MINOR IMPROVEMENT - WRECK`
- `191 - MAJOR IMPROVEMENT - BURNOUT`
- `192 - MINOR IMPROVEMENT - BURNOUT`
- `211 - NEW BUILDING`
- `212 - ADDITIONS`
- `213 - OTHER - MAJOR NEW CONSTRUCTION`
- `231 - SWIMMING POOL - TENNIS COURT`
- `232 - DRIVEWAYS - PATIOS`
- `233 - FENCING`
- `234 - GARAGE-BARN-BUTLER BUILDING`
- `235 - OTHER - MINOR NEW CONSTRUCTION`
- `236 - ???`
- `251 - PARTITIONING`
- `252 - DECONV/CONV AMT. LIV. UT.`
- `253 - AIR CONDITIONING - FIREPLACES`
- `254 - BASEMENT-ATTIC ROOM BATH`
- `255 - REPLACE-REPAIR BUILDING - FACTORY`
- `256 - OTHER REMODELING`
- `257 - REMODELING - INTERIOR ONLY`
- `258 - REMODELING - EXTERIOR ONLY`
- `271 - MAJOR IMPROVEMENT - WRECK`
- `272 - MINOR IMPROVEMENT - WRECK`
- `291 - MAJOR IMPROVEMENT - BURNOUT`
- `292 - MINOR IMPROVEMENT - BURNOUT`
- `300 - RAILROAD PROPERTIES`
- `310 - NEW CONST MAJOR IMPROVEMENT`
- `311 - CARRIER - NEW MAJOR CONSTRUCTION`
- `312 - NON-CARRIER NEW MAJOR CONSTRUCTION`
- `330 - NEW CONSTRUCTION MINOR IMPROVEMENT`
- `331 - CARRIER - REMODELING`
- `332 - NON-CARRIER - REMODELING`
- `350 - REMODELING`
- `351 - CARRIER - REMODELING`
- `352 - NON-CARRIER - REMODELING`
- `370 - WRECKS`
- `371 - MAJOR IMPROVEMENT WRECK`
- `372 - MINOR IMPROVEMENT - WRECK`
- `390 - BURNOUTS`
- `391 - MAJOR IMPROVEMENT - BURNOUT`
- `392 - MINOR IMPROVEMENT - BURNOUT`
- `400 - EXEMPT PROPERTIES`
- `411 - LEASEHOLD`
- `510 - RECHECKS`
- `511 - OCCUPANCY`
- `512 - PRIOR YEAR PARTIAL ASSESSMENT`
- `515 - MANUALLY ISSUED PERMIT`
- `516 - PRIOR YEAR RECHECK`
- `900 - OTHER`
{% enddocs %}

## permit_job_code_primary

{% docs column_permit_job_code_primary %}
Primary job description.

This field functions as a permit classification field. The
`job_code_secondary` and `improvement_code_*` fields provide
codes with more details on the type of job that the permit represents.

The CCAO requests that municipalities provide job codes as a way of
categorizing the specific type of planned work that the permit represents.
Not all municipalities fill out this field correctly, however, so the
`work_description` field should be considered the source of truth about the
type of work that a permit represents.

Possible values for this variable are:

- `1 - RESIDENTIAL PERMIT`
- `2 - COMMERCIAL PERMIT`
- `3 - RAILROAD PERMIT`
- `4 - EXEMPT PERMIT`
- `5 - OFFICE PERMIT`
- `6 - OCCUPANCY PERMIT`
- `7 - OTHER`
{% enddocs %}

## permit_job_code_secondary

{% docs column_permit_job_code_secondary %}
Secondary job description.

In conjunction with the `job_code_primary` and `improvement_code_*`
fields, this field provides details on the type of job that the
permit represents.

The CCAO requests that municipalities provide job codes as a way of
categorizing the specific type of planned work that the permit represents.
Not all municipalities fill out this field correctly, however, so the
`work_description` field should be considered the source of truth about the
type of work that a permit represents.

Possible values for this variable are:

- `111 - NEW BUILDING`
- `112 - ADDITIONS`
- `113 - DORMERS`
- `114 - OTHER - MAJOR NEW CONSTRUCTION`
- `114.1 - OTHER - MAJOR NEW CONSTRUCTION - foundations`
- `114.2 - OTHER - MAJOR NEW CONSTRUCTION - mobile home cement pads`
- `114.3 - OTHER - MAJOR NEW CONSTRUCTION - structural changes`
- `114.4 - OTHER - MAJOR NEW CONSTRUCTION - seasonal rooms that are conventionally heated (forced hot air, radiant, etc.) such as Florida Rooms or Sun Rooms.`
- `114.5 - OTHER - MAJOR NEW CONSTRUCTION`
- `114.6 - OTHER - MAJOR NEW CONSTRUCTION`
- `114.7 - OTHER - MAJOR NEW CONSTRUCTION`
- `114.8 - OTHER - MAJOR NEW CONSTRUCTION`
- `114.9 - OTHER - MAJOR NEW CONSTRUCTION`
- `131 - SWIMMING POOL - TENNIS COURT`
- `131.1 - SWIMMING POOL - TENNIS COURT - In ground swimming pools (not assessed but recorded)`
- `131.2 - SWIMMING POOL - TENNIS COURT - hot tubs`
- `131.3 - SWIMMING POOL - TENNIS COURT - spas`
- `131.4 - SWIMMING POOL - TENNIS COURT - tennis courts`
- `132 - DRIVEWAYS - PATIOS - WOOD DECK`
- `132.1 - DRIVEWAYS - PATIOS - WOOD DECK - Drive ways`
- `132.1.1 - DRIVEWAYS - PATIOS - WOOD DECK - Drive ways (concrete)`
- `132.1.2 - DRIVEWAYS - PATIOS - WOOD DECK - Drive ways (asphalt)`
- `132.1.3 - DRIVEWAYS - PATIOS - WOOD DECK - Drive ways (brick)`
- `132.2 - DRIVEWAYS - PATIOS - WOOD DECK - deck`
- `132.3 - DRIVEWAYS - PATIOS - WOOD DECK - porches`
- `132.3.1 - DRIVEWAYS - PATIOS - WOOD DECK - porches (standard)`
- `132.3.2 - DRIVEWAYS - PATIOS - WOOD DECK - porches (screen in porches)`
- `132.3.3 - DRIVEWAYS - PATIOS - WOOD DECK - porches (wrap around porches)`
- `132.3.4 - DRIVEWAYS - PATIOS - WOOD DECK - PORCHES (ENCLOSED)`
- `132.4 - DRIVEWAYS - PATIOS - WOOD DECK - patios`
- `132.5 - DRIVEWAYS - PATIOS - WOOD DECK - balconies`
- `133 - FENCING (and Gates)`
- `133.1 - FENCING (and Gates) - wrought iron`
- `133.2 - FENCING (and Gates) - aluminum`
- `133.3 - FENCING (and Gates) - wood`
- `133.4 - FENCING (and Gates) - plastic`
- `134 - GARAGE CARPORTS BARNS`
- `134.1 - GARAGE CARPORTS BARNS - garages`
- `134.2 - GARAGE CARPORTS BARNS - barns`
- `134.3 - GARAGE CARPORTS BARNS - car ports`
- `134.4 - GARAGE CARPORTS BARNS - gazebos`
- `134.5 - GARAGE CARPORTS BARNS - cement pad for miscellaneous use`
- `134.6 - OTHER GARAGE CARPORT BARNS`
- `134.7 - OTHER GARAGE CARPORT BARNS`
- `134.8 - OTHER GARAGE CARPORT BARNS`
- `135 - OTHER - MINOR NEW CONSTRUCTION`
- `135.1 - OTHER - MINOR NEW CONSTRUCTION - lawn sprinkler systems`
- `135.2 - OTHER - MINOR NEW CONSTRUCTION - septic systems`
- `135.3 - OTHER - MINOR NEW CONSTRUCTION - sewer system`
- `135.4 - OTHER - MINOR NEW CONSTRUCTION - retaining walls`
- `135.5 - OTHER - MINOR NEW CONSTRUCTION - handicap ramps`
- `135.6 - OTHER - MINOR NEW CONSTRUCTION - canopies`
- `135.7 - OTHER - MINOR NEW CONSTRUCTION - water taps`
- `151 - BASEMENT ROOM - REC ROOM`
- `151.1 - BASEMENT ROOM - REC ROOM - Basement area being converted from unfinished to finished (including new bathrooms in the basement area)`
- `151.2 - BASEMENT ROOM - REC ROOM - Basement finished area being remodeled`
- `152 - ATTIC ROOM - BATHROOMS`
- `152.1 - ATTIC ROOM - BATHROOMS - Attic area being converted from unfinished to finished (including new bathrooms in the basement area)`
- `152.2 - ATTIC ROOM - BATHROOMS - Attic finished area being remodeled`
- `153 - FIREPLACE-CENTRAL AIR CONDITIONING`
- `153.1 - FIREPLACE-CENTRAL AIR CONDITIONING - fireplace`
- `153.2 - FIREPLACE-CENTRAL AIR CONDITIONING - central air conditioning`
- `153.3 - FIREPLACE-CENTRAL AIR CONDITIONING - replacement central air conditioning`
- `154 - OTHER - REMODELING`
- `154.1 - OTHER - REMODELING - renovation`
- `154.11 - OTHER - REMODELING - water taps - (should be verified that they are not for new construction)`
- `154.2 - OTHER - REMODELING - general repairs`
- `154.3 - OTHER - REMODELING - elevators`
- `154.4 - OTHER - REMODELING - handicap ramps`
- `154.5 - OTHER - REMODELING - fire & flood alarms`
- `154.6 - OTHER - REMODELING - canopies`
- `154.7 - OTHER - REMODELING - water taps - (should be verified that they are not for new construction)`
- `154.8 - OTHER - REMODELING - fire & flood alarms`
- `154.9 - OTHER - REMODELING - canopies`
- `171 - MAJOR IMPROVEMENT - WRECK`
- `171.1 - MAJOR IMPROVEMENT - WRECK - major demolition`
- `171.2 - MAJOR IMPROVEMENT - WRECK - house moving`
- `172 - MINOR IMPROVEMENT - WRECK`
- `172.1 - MINOR IMPROVEMENT - WRECK - garages`
- `172.2 - MINOR IMPROVEMENT - WRECK - sheds`
- `172.3 - MINOR IMPROVEMENT - WRECK - debris removal`
- `191 - MAJOR IMPROVEMENT - BURNOUT`
- `192 - MINOR IMPROVEMENT - BURNOUT`
- `192.1 - MINOR IMPROVEMENT - BURNOUT - garage`
- `192.2 - MINOR IMPROVEMENT - BURNOUT - shed`
- `211 - NEW BUILDING`
- `212 - ADDITIONS`
- `213 - OTHER - MAJOR NEW CONSTRUCTION`
- `213.1 - OTHER - MAJOR NEW CONSTRUCTION - foundations`
- `213.2 - OTHER - MAJOR NEW CONSTRUCTION - build-outs`
- `213.3 - OTHER - MAJOR NEW CONSTRUCTION - cement pads`
- `213.4 - OTHER - MAJOR NEW CONSTRUCTION - loading dock`
- `231 - SWIMMING POOL - TENNIS COURT`
- `231.1 - SWIMMING POOL - TENNIS COURT - tennis courts`
- `231.2 - SWIMMING POOL - TENNIS COURT - swimming pools`
- `231.3 - SWIMMING POOL - TENNIS COURT - recreation improvements`
- `232 - DRIVEWAYS - PATIOS`
- `232.1 - DRIVEWAYS - PATIOS - rock`
- `232.2 - DRIVEWAYS - PATIOS - asphalt`
- `232.3 - DRIVEWAYS - PATIOS - cement`
- `232.4 - DRIVEWAYS - PATIOS - crushed rock`
- `232.5 - DRIVEWAYS - PATIOS - pavers`
- `233 - COMMERICAL FENCING`
- `233.1 - FENCING - wrought iron`
- `233.2 - FENCING - security`
- `233.3 - FENCING - wood`
- `233.4 - FENCING - chain link`
- `234 - GARAGE-BARN-BUTLER-TRASH ENCLOSURES BUILDING`
- `234.1 - GARAGE-BARN-BUTLER BUILDING - garages`
- `234.2 - GARAGE-BARN-BUTLER BUILDING - barns`
- `234.3 - GARAGE-BARN-BUTLER BUILDING - butler buildings`
- `234.4 - Trash Enclosures`
- `235 - OTHER - MINOR NEW CONSTRUCTION`
- `235.1 - OTHER - MINOR NEW CONSTRUCTION - construction trailers`
- `235.10 - OTHER - MINOR NEW CONSTRUCTION - tents`
- `235.11 - OTHER - MINOR NEW CONSTRUCTION - sewer clean out`
- `235.12 - OTHER - MINOR NEW CONSTRUCTION - ATM machines`
- `235.12.1 - OTHER - MINOR NEW CONSTRUCTION - ATM machines - Partitioning`
- `235.12.2 - OTHER - MINOR NEW CONSTRUCTION - ATM machines - machine`
- `235.13 - OTHER - MINOR NEW CONSTRUCTION - Parking Lot`
- `235.2 - OTHER - MINOR NEW CONSTRUCTION - tanks`
- `235.3 - OTHER - MINOR NEW CONSTRUCTION - cell tower (monopoles) pad and fencing`
- `235.4 - OTHER - MINOR NEW CONSTRUCTION - signs`
- `235.5 - OTHER - MINOR NEW CONSTRUCTION - canopies`
- `235.6 - OTHER - MINOR NEW CONSTRUCTION - cell towers (monopoles)`
- `235.7 - OTHER - MINOR NEW CONSTRUCTION - sprinkler system`
- `235.8 - OTHER - MINOR NEW CONSTRUCTION - security system`
- `235.9 - OTHER - MINOR NEW CONSTRUCTION - handicap ramps`
- `251 - PARTITIONING`
- `252 - DECONV/CONV AMT. LIV. UT.`
- `253 - AIR CONDITIONING - FIREPLACES`
- `253.1 - AIR CONDITIONING - FIREPLACES - fireplaces`
- `253.2 - AIR CONDITIONING - FIREPLACES - air conditioning`
- `254 - BASEMENT-ATTIC ROOM BATH`
- `254.1 - BASEMENT-ATTIC ROOM BATH - basement rooms`
- `254.2 - BASEMENT-ATTIC ROOM BATH - bathrooms`
- `254.3 - BASEMENT-ATTIC ROOM BATH - attic rooms`
- `255 - REPLACE-REPAIR BUILDING - FACTORY`
- `255.1 - REPLACE-REPAIR BUILDING - FACTORY - rehab`
- `255.2 - REPLACE-REPAIR BUILDING - FACTORY - replacement`
- `255.3 - REPLACE-REPAIR BUILDING - FACTORY - repair to existing building`
- `255.4 - REPLACE-REPAIR BUILDING - FACTORY - replacement minor`
- `255.5 - REPLACE-REPAIR BUILDING - FACTORY - repair to existing building major`
- `255.6 - REPLACE-REPAIR BUILDING - FACTORY - repair to existing building minor`
- `256 - OTHER REMODELING`
- `256.1 - OTHER REMODELING - elevators`
- `256.2 - OTHER REMODELING - escalators`
- `256.3 - OTHER REMODELING - handicap ramps`
- `256.4 - OTHER REMODELING - awnings`
- `256.5 - OTHER REMODELING - fire alarms`
- `256.6 - OTHER REMODELING - security alarms`
- `256.7 - OTHER REMODELING - fire alarms - Hood Suppression System`
- `256.8 - OTHER REMODELING - fire alarms - Sprinkler for Fire`
- `256.9 - OTHER REMODELING - fire alarms - Security alarms`
- `271 - MAJOR IMPROVEMENT - WRECK`
- `272 - MINOR IMPROVEMENT - WRECK`
- `291 - MAJOR IMPROVEMENT - BURNOUT`
- `292 - MINOR IMPROVEMENT - BURNOUT`
- `311 - CARRIER - NEW MAJOR CONSTRUCTION`
- `312 - NON-CARRIER NEW MAJOR CONSTRUCTION`
- `331 - CARRIER - REMODELING`
- `332 - NON-CARRIER - REMODELING`
- `351 - CARRIER - REMODELING`
- `352 - NON-CARRIER - REMODELING`
- `371 - MAJOR IMPROVEMENT WRECK`
- `372 - MINOR IMPROVEMENT - WRECK`
- `391 - MAJOR IMPROVEMENT - BURNOUT`
- `392 - MINOR IMPROVEMENT - BURNOUT`
- `400 - EXEMPT PROPERTIES`
- `411 - LEASEHOLD`
- `500 - OFFICE PERMITS`
- `510 - RECHECKS`
- `511 - OCCUPANCY`
- `512 - PRIOR YEAR PARTIAL ASSESSMENT`
- `513 - NEW CONSTRUCTION`
- `514 - ADDED IMPROVEMENTS`
- `515 - MANUALLY ISSUED PERMIT`
- `516 - PRIOR YEAR RECHECK`
- `517 - DIVISION PERMIT`
- `517.1 - DIVISION PERMIT - Petition`
- `517.11 - DIVISION PERMIT - Petition Leasehold`
- `517.2 - DIVISION PERMIT - Condominimum`
- `517.3 - DIVISION PERMIT - Subdivision`
- `517.4 - DIVISION PERMIT - Condo Removal`
- `517.5 - DIVISION PERMIT - Petition Road Taking`
- `517.6 - DIVISION PERMIT - Vacation`
- `517.7 - DIVISION PERMIT - Dedication`
- `517.8 - DIVISION PERMIT - Other`
- `517.9 - DIVISION PERMIT - Mix Class`
- `518 - CK BLDG. SF/UP`
- `519 - CONVERT TO NEW MANUAL`
- `520 - AUDIT DEPT INCREASE TO CHART`
- `530 - RECENT I/C SALES`
- `549 - 1985 HISTORICAL LANDMARK`
- `550 - 1986 HISTORICAL LANDMARK`
- `551 - 1987 HISTORICAL LANDMARK`
- `552 - 1988 HISTORICAL LANDMARK`
- `553 - 1989 HISTORICAL LANDMARK`
- `554 - 1990 HISTORICAL LANDMARK`
- `555 - 1991 HISTORICAL LANDMARK`
- `556 - 1992 HISTORICAL LANDMARK`
- `557 - 1993 HISTORICAL LANDMARK`
- `558 - 1994 HISTORICAL LANDMARK`
- `559 - 1995 HISTORICAL LANDMARK`
- `560 - 1996 HISTORICAL LANDMARK`
- `561 - 1997 HISTORICAL LANDMARK`
- `562 - 1998 HISTORICAL LANDMARK`
- `563 - 1999 HISTORICAL LANDMARK`
- `564 - 2000 HISTORICAL LANDMARK`
- `565 - 2001 HISTORICAL LANDMARK`
- `566 - 2002 HISTORICAL LANDMARK`
- `911 - OCCUPANCY CODE - WILL NOT BE USED`
{% enddocs %}

## permit_number

{% docs column_permit_number %}
Permit number created by Smartfile during permit submission.

This field can be null if the permit was not uploaded through Smartfile, so
we do not recommend using it for establishing permit uniqueness. Instead,
prefer the combination of the `pin` and `date_issued` fields.
{% enddocs %}

## permit_recheck_year

{% docs column_permit_recheck_year %}
Year that this permit should be rechecked, if any.

Recheck occurs when a field inspector finds that permitted work is still ongoing
such that they cannot yet update parcel characteristics. In these cases, they
will mark the permit with a `status` of `R` (recheck), and they will
sometimes set a recheck year to indicate when the permit should come back up
for recheck.
{% enddocs %}

## permit_status

{% docs column_permit_status %}
Status of the permit in the Assessor's permit workflow.

Possible values for this variable are:

- `C - CLOSED`: A permit specialist has reviewed the permit and taken all
  necessary action on it.
- `L - LEGACY`: Deprecated.
- `M - MANAGER REVIEW`: A permit specialist has escalated this permit to review
  by a manager. This typically indicates that the permit is ambiguous enough
  that the assessability of the work requires specialized interpretation.
- `O - OPEN`: The permit is in the work queue waiting for a permit specialist
  to review it.
- `P - PENDING`: The permit has been created in our system, but it has not yet
  been reviewed by a permit specialist, and it is not yet in the work queue
  waiting for review.
- `R - RECHECK`: A field inspector has indicated that the work that this permit
  represents is ongoing, so it must be rechecked at a later date once work is
  completed.
{% enddocs %}

## permit_work_description

{% docs column_permit_work_description %}
Short description of permitted work.

Municipalities are responsible for filling out this field when submitting a
permit. It is very rarely null, so we use it as the source of truth for the
work that a permit represents.
{% enddocs %}
