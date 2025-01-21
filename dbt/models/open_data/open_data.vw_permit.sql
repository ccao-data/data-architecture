-- Copy of default.vw_pin_appeal that feeds the "Appeals" open data asset.
-- Some columns from the feeder view may not be present in this view.

SELECT
    CONCAT(pin, permit_number) AS row_id,
    pin,
    permit_number,
    local_permit_number,
    date_issued,
    date_submitted,
    estimated_date_of_completion,
    assessment_year,
    recheck_year,
    CASE
        WHEN status = 'C' THEN 'CLOSED'
        WHEN status = 'L' THEN 'LEGACY'
        WHEN status = 'M' THEN 'MANAGER REVIEW'
        WHEN status = 'O' THEN 'OPEN'
        WHEN status = 'P' THEN 'PENDING'
        WHEN status = 'R' THEN 'RECHECK'
    END AS status,
    CASE
        WHEN assessable = 'A' THEN 'Assessable'
        WHEN assessable = 'N' THEN 'Non-Assessable'
    END AS assessable,
    amount,
    address_full,
    address_street_dir,
    address_street_number,
    address_street_name,
    address_suffix_1,
    address_suffix_2,
    applicant_name,
    CASE
        WHEN job_code_primary = '1' THEN 'RESIDENTIAL PERMIT'
        WHEN job_code_primary = '2' THEN 'COMMERCIAL PERMIT'
        WHEN job_code_primary = '3' THEN 'RAILROAD PERMIT'
        WHEN job_code_primary = '4' THEN 'EXEMPT PERMIT'
        WHEN job_code_primary = '5' THEN 'OFFICE PERMIT'
        WHEN job_code_primary = '6' THEN 'OCCUPANCY PERMIT'
        WHEN job_code_primary = '7' THEN 'OTHER'
    END AS job_code_primary,
    CASE
        WHEN job_code_secondary = '111' THEN 'NEW BUILDING'
        WHEN job_code_secondary = '112' THEN 'ADDITIONS'
        WHEN job_code_secondary = '113' THEN 'DORMERS'
        WHEN job_code_secondary = '114' THEN 'OTHER MAJOR NEW CONSTRUCTION'
        WHEN
            job_code_secondary = '114.1'
            THEN 'OTHER MAJOR NEW CONSTRUCTION foundations'
        WHEN
            job_code_secondary = '114.2'
            THEN 'OTHER MAJOR NEW CONSTRUCTION mobile home cement pads'
        WHEN
            job_code_secondary = '114.3'
            THEN 'OTHER MAJOR NEW CONSTRUCTION structural changes'
        WHEN
            job_code_secondary = '114.4'
            THEN 'OTHER MAJOR NEW CONSTRUCTION seasonal rooms that are conventionally heated (forced hot air, radiant, etc.) such as Florida Rooms or Sun Rooms.' -- noqa
        WHEN job_code_secondary = '114.5' THEN 'OTHER MAJOR NEW CONSTRUCTION'
        WHEN job_code_secondary = '114.6' THEN 'OTHER MAJOR NEW CONSTRUCTION'
        WHEN job_code_secondary = '114.7' THEN 'OTHER MAJOR NEW CONSTRUCTION'
        WHEN job_code_secondary = '114.8' THEN 'OTHER MAJOR NEW CONSTRUCTION'
        WHEN job_code_secondary = '114.9' THEN 'OTHER MAJOR NEW CONSTRUCTION'
        WHEN job_code_secondary = '131' THEN 'SWIMMING POOL TENNIS COURT'
        WHEN
            job_code_secondary = '131.1'
            THEN 'SWIMMING POOL TENNIS COURT In ground swimming pools (not assessed but recorded)' -- noqa
        WHEN
            job_code_secondary = '131.2'
            THEN 'SWIMMING POOL TENNIS COURT hot tubs'
        WHEN job_code_secondary = '131.3' THEN 'SWIMMING POOL TENNIS COURT spas'
        WHEN
            job_code_secondary = '131.4'
            THEN 'SWIMMING POOL TENNIS COURT tennis courts'
        WHEN job_code_secondary = '132' THEN 'DRIVEWAYS PATIOS WOOD DECK'
        WHEN
            job_code_secondary = '132.1'
            THEN 'DRIVEWAYS PATIOS WOOD DECK Drive ways'
        WHEN
            job_code_secondary = '132.1.1'
            THEN '1 DRIVEWAYS PATIOS WOOD DECK Drive ways (concrete)'
        WHEN
            job_code_secondary = '132.1.2'
            THEN '2 DRIVEWAYS PATIOS WOOD DECK Drive ways (asphalt)'
        WHEN
            job_code_secondary = '132.1.3'
            THEN '3 DRIVEWAYS PATIOS WOOD DECK Drive ways (brick)'
        WHEN job_code_secondary = '132.2' THEN 'DRIVEWAYS PATIOS WOOD DECK deck'
        WHEN
            job_code_secondary = '132.3'
            THEN 'DRIVEWAYS PATIOS WOOD DECK porches'
        WHEN
            job_code_secondary = '132.3.1'
            THEN '1 DRIVEWAYS PATIOS WOOD DECK porches (standard)'
        WHEN
            job_code_secondary = '132.3.2'
            THEN '2 DRIVEWAYS PATIOS WOOD DECK porches (screen in porches)'
        WHEN
            job_code_secondary = '132.3.3'
            THEN '3 DRIVEWAYS PATIOS WOOD DECK porches (wrap around porches)'
        WHEN
            job_code_secondary = '132.3.4'
            THEN '4 DRIVEWAYS PATIOS WOOD DECK PORCHES (ENCLOSED)'
        WHEN
            job_code_secondary = '132.4'
            THEN 'DRIVEWAYS PATIOS WOOD DECK patios'
        WHEN
            job_code_secondary = '132.5'
            THEN 'DRIVEWAYS PATIOS WOOD DECK balconies'
        WHEN job_code_secondary = '133' THEN 'FENCING (and Gates)'
        WHEN
            job_code_secondary = '133.1'
            THEN 'FENCING (and Gates) wrought iron'
        WHEN job_code_secondary = '133.2' THEN 'FENCING (and Gates) aluminum'
        WHEN job_code_secondary = '133.3' THEN 'FENCING (and Gates) wood'
        WHEN job_code_secondary = '133.4' THEN 'FENCING (and Gates) plastic'
        WHEN job_code_secondary = '134' THEN 'GARAGE CARPORTS BARNS'
        WHEN job_code_secondary = '134.1' THEN 'GARAGE CARPORTS BARNS garages'
        WHEN job_code_secondary = '134.2' THEN 'GARAGE CARPORTS BARNS barns'
        WHEN job_code_secondary = '134.3' THEN 'GARAGE CARPORTS BARNS car ports'
        WHEN job_code_secondary = '134.4' THEN 'GARAGE CARPORTS BARNS gazebos'
        WHEN
            job_code_secondary = '134.5'
            THEN 'GARAGE CARPORTS BARNS cement pad for miscellaneous use'
        WHEN job_code_secondary = '134.6' THEN 'OTHER GARAGE CARPORT BARNS'
        WHEN job_code_secondary = '134.7' THEN 'OTHER GARAGE CARPORT BARNS'
        WHEN job_code_secondary = '134.8' THEN 'OTHER GARAGE CARPORT BARNS'
        WHEN job_code_secondary = '135' THEN 'OTHER MINOR NEW CONSTRUCTION'
        WHEN
            job_code_secondary = '135.1'
            THEN 'OTHER MINOR NEW CONSTRUCTION lawn sprinkler systems'
        WHEN
            job_code_secondary = '135.2'
            THEN 'OTHER MINOR NEW CONSTRUCTION septic systems'
        WHEN
            job_code_secondary = '135.3'
            THEN 'OTHER MINOR NEW CONSTRUCTION sewer system'
        WHEN
            job_code_secondary = '135.4'
            THEN 'OTHER MINOR NEW CONSTRUCTION retaining walls'
        WHEN
            job_code_secondary = '135.5'
            THEN 'OTHER MINOR NEW CONSTRUCTION handicap ramps'
        WHEN
            job_code_secondary = '135.6'
            THEN 'OTHER MINOR NEW CONSTRUCTION canopies'
        WHEN
            job_code_secondary = '135.7'
            THEN 'OTHER MINOR NEW CONSTRUCTION water taps'
        WHEN job_code_secondary = '151' THEN 'BASEMENT ROOM REC ROOM'
        WHEN
            job_code_secondary = '151.1'
            THEN 'BASEMENT ROOM REC ROOM Basement area being converted from unfinished to finished (including new bathrooms in the basement area)' -- noqa
        WHEN
            job_code_secondary = '151.2'
            THEN 'BASEMENT ROOM REC ROOM Basement finished area being remodeled'
        WHEN job_code_secondary = '152' THEN 'ATTIC ROOM BATHROOMS'
        WHEN
            job_code_secondary = '152.1'
            THEN 'ATTIC ROOM BATHROOMS Attic area being converted from unfinished to finished (including new bathrooms in the basement area)' -- noqa
        WHEN
            job_code_secondary = '152.2'
            THEN 'ATTIC ROOM BATHROOMS Attic finished area being remodeled'
        WHEN
            job_code_secondary = '153'
            THEN 'FIREPLACE-CENTRAL AIR CONDITIONING'
        WHEN
            job_code_secondary = '153.1'
            THEN 'FIREPLACE-CENTRAL AIR CONDITIONING fireplace'
        WHEN
            job_code_secondary = '153.2'
            THEN 'FIREPLACE-CENTRAL AIR CONDITIONING central air conditioning'
        WHEN
            job_code_secondary = '153.3'
            THEN 'FIREPLACE-CENTRAL AIR CONDITIONING replacement central air conditioning' -- noqa
        WHEN job_code_secondary = '154' THEN 'OTHER REMODELING'
        WHEN job_code_secondary = '154.1' THEN 'OTHER REMODELING renovation'
        WHEN
            job_code_secondary = '154.11'
            THEN 'OTHER REMODELING water taps (should be verified that they are not for new construction)' -- noqa
        WHEN
            job_code_secondary = '154.2'
            THEN 'OTHER REMODELING general repairs'
        WHEN job_code_secondary = '154.3' THEN 'OTHER REMODELING elevators'
        WHEN job_code_secondary = '154.4' THEN 'OTHER REMODELING handicap ramps'
        WHEN
            job_code_secondary = '154.5'
            THEN 'OTHER REMODELING fire & flood alarms'
        WHEN job_code_secondary = '154.6' THEN 'OTHER REMODELING canopies'
        WHEN
            job_code_secondary = '154.7'
            THEN 'OTHER REMODELING water taps (should be verified that they are not for new construction)' -- noqa
        WHEN
            job_code_secondary = '154.8'
            THEN 'OTHER REMODELING fire & flood alarms'
        WHEN job_code_secondary = '154.9' THEN 'OTHER REMODELING canopies'
        WHEN job_code_secondary = '171' THEN 'MAJOR IMPROVEMENT WRECK'
        WHEN
            job_code_secondary = '171.1'
            THEN 'MAJOR IMPROVEMENT WRECK major demolition'
        WHEN
            job_code_secondary = '171.2'
            THEN 'MAJOR IMPROVEMENT WRECK house moving'
        WHEN job_code_secondary = '172' THEN 'MINOR IMPROVEMENT WRECK'
        WHEN job_code_secondary = '172.1' THEN 'MINOR IMPROVEMENT WRECK garages'
        WHEN job_code_secondary = '172.2' THEN 'MINOR IMPROVEMENT WRECK sheds'
        WHEN
            job_code_secondary = '172.3'
            THEN 'MINOR IMPROVEMENT WRECK debris removal'
        WHEN job_code_secondary = '191' THEN 'MAJOR IMPROVEMENT BURNOUT'
        WHEN job_code_secondary = '192' THEN 'MINOR IMPROVEMENT BURNOUT'
        WHEN
            job_code_secondary = '192.1'
            THEN 'MINOR IMPROVEMENT BURNOUT garage'
        WHEN job_code_secondary = '192.2' THEN 'MINOR IMPROVEMENT BURNOUT shed'
        WHEN job_code_secondary = '211' THEN 'NEW BUILDING'
        WHEN job_code_secondary = '212' THEN 'ADDITIONS'
        WHEN job_code_secondary = '213' THEN 'OTHER MAJOR NEW CONSTRUCTION'
        WHEN
            job_code_secondary = '213.1'
            THEN 'OTHER MAJOR NEW CONSTRUCTION foundations'
        WHEN
            job_code_secondary = '213.2'
            THEN 'OTHER MAJOR NEW CONSTRUCTION build-outs'
        WHEN
            job_code_secondary = '213.3'
            THEN 'OTHER MAJOR NEW CONSTRUCTION cement pads'
        WHEN
            job_code_secondary = '213.4'
            THEN 'OTHER MAJOR NEW CONSTRUCTION loading dock'
        WHEN job_code_secondary = '231' THEN 'SWIMMING POOL TENNIS COURT'
        WHEN
            job_code_secondary = '231.1'
            THEN 'SWIMMING POOL TENNIS COURT tennis courts'
        WHEN
            job_code_secondary = '231.2'
            THEN 'SWIMMING POOL TENNIS COURT swimming pools'
        WHEN
            job_code_secondary = '231.3'
            THEN 'SWIMMING POOL TENNIS COURT recreation improvements'
        WHEN job_code_secondary = '232' THEN 'DRIVEWAYS PATIOS'
        WHEN job_code_secondary = '232.1' THEN 'DRIVEWAYS PATIOS rock'
        WHEN job_code_secondary = '232.2' THEN 'DRIVEWAYS PATIOS asphalt'
        WHEN job_code_secondary = '232.3' THEN 'DRIVEWAYS PATIOS cement'
        WHEN job_code_secondary = '232.4' THEN 'DRIVEWAYS PATIOS crushed rock'
        WHEN job_code_secondary = '232.5' THEN 'DRIVEWAYS PATIOS pavers'
        WHEN job_code_secondary = '233' THEN 'COMMERICAL FENCING'
        WHEN job_code_secondary = '233.1' THEN 'FENCING wrought iron'
        WHEN job_code_secondary = '233.2' THEN 'FENCING security'
        WHEN job_code_secondary = '233.3' THEN 'FENCING wood'
        WHEN job_code_secondary = '233.4' THEN 'FENCING chain link'
        WHEN
            job_code_secondary = '234'
            THEN 'GARAGE-BARN-BUTLER-TRASH ENCLOSURES BUILDING'
        WHEN
            job_code_secondary = '234.1'
            THEN 'GARAGE-BARN-BUTLER BUILDING garages'
        WHEN
            job_code_secondary = '234.2'
            THEN 'GARAGE-BARN-BUTLER BUILDING barns'
        WHEN
            job_code_secondary = '234.3'
            THEN 'GARAGE-BARN-BUTLER BUILDING butler buildings'
        WHEN job_code_secondary = '234.4' THEN 'Trash Enclosures'
        WHEN job_code_secondary = '235' THEN 'OTHER MINOR NEW CONSTRUCTION'
        WHEN
            job_code_secondary = '235.1'
            THEN 'OTHER MINOR NEW CONSTRUCTION construction trailers'
        WHEN
            job_code_secondary = '235.10'
            THEN 'OTHER MINOR NEW CONSTRUCTION tents'
        WHEN
            job_code_secondary = '235.11'
            THEN 'OTHER MINOR NEW CONSTRUCTION sewer clean out'
        WHEN
            job_code_secondary = '235.12'
            THEN 'OTHER MINOR NEW CONSTRUCTION ATM machines'
        WHEN
            job_code_secondary = '235.12.1'
            THEN 'OTHER MINOR NEW CONSTRUCTION ATM machines Partitioning'
        WHEN
            job_code_secondary = '235.12.2'
            THEN 'OTHER MINOR NEW CONSTRUCTION ATM machines machine'
        WHEN
            job_code_secondary = '235.13'
            THEN 'OTHER MINOR NEW CONSTRUCTION Parking Lot'
        WHEN
            job_code_secondary = '235.2'
            THEN 'OTHER MINOR NEW CONSTRUCTION tanks'
        WHEN
            job_code_secondary = '235.3'
            THEN 'OTHER MINOR NEW CONSTRUCTION cell tower (monopoles) pad and fencing' -- noqa
        WHEN
            job_code_secondary = '235.4'
            THEN 'OTHER MINOR NEW CONSTRUCTION signs'
        WHEN
            job_code_secondary = '235.5'
            THEN 'OTHER MINOR NEW CONSTRUCTION canopies'
        WHEN
            job_code_secondary = '235.6'
            THEN 'OTHER MINOR NEW CONSTRUCTION cell towers (monopoles)'
        WHEN
            job_code_secondary = '235.7'
            THEN 'OTHER MINOR NEW CONSTRUCTION sprinkler system'
        WHEN
            job_code_secondary = '235.8'
            THEN 'OTHER MINOR NEW CONSTRUCTION security system'
        WHEN
            job_code_secondary = '235.9'
            THEN 'OTHER MINOR NEW CONSTRUCTION handicap ramps'
        WHEN job_code_secondary = '251' THEN 'PARTITIONING'
        WHEN job_code_secondary = '252' THEN 'DECONV/CONV AMT. LIV. UT.'
        WHEN job_code_secondary = '253' THEN 'AIR CONDITIONING FIREPLACES'
        WHEN
            job_code_secondary = '253.1'
            THEN 'AIR CONDITIONING FIREPLACES fireplaces'
        WHEN
            job_code_secondary = '253.2'
            THEN 'AIR CONDITIONING FIREPLACES air conditioning'
        WHEN job_code_secondary = '254' THEN 'BASEMENT-ATTIC ROOM BATH'
        WHEN
            job_code_secondary = '254.1'
            THEN 'BASEMENT-ATTIC ROOM BATH basement rooms'
        WHEN
            job_code_secondary = '254.2'
            THEN 'BASEMENT-ATTIC ROOM BATH bathrooms'
        WHEN
            job_code_secondary = '254.3'
            THEN 'BASEMENT-ATTIC ROOM BATH attic rooms'
        WHEN job_code_secondary = '255' THEN 'REPLACE-REPAIR BUILDING FACTORY'
        WHEN
            job_code_secondary = '255.1'
            THEN 'REPLACE-REPAIR BUILDING FACTORY rehab'
        WHEN
            job_code_secondary = '255.2'
            THEN 'REPLACE-REPAIR BUILDING FACTORY replacement'
        WHEN
            job_code_secondary = '255.3'
            THEN 'REPLACE-REPAIR BUILDING FACTORY repair to existing building'
        WHEN
            job_code_secondary = '255.4'
            THEN 'REPLACE-REPAIR BUILDING FACTORY replacement minor'
        WHEN
            job_code_secondary = '255.5'
            THEN 'REPLACE-REPAIR BUILDING FACTORY repair to existing building major' -- noqa
        WHEN
            job_code_secondary = '255.6'
            THEN 'REPLACE-REPAIR BUILDING FACTORY repair to existing building minor' -- noqa
        WHEN job_code_secondary = '256' THEN 'OTHER REMODELING'
        WHEN job_code_secondary = '256.1' THEN 'OTHER REMODELING elevators'
        WHEN job_code_secondary = '256.2' THEN 'OTHER REMODELING escalators'
        WHEN job_code_secondary = '256.3' THEN 'OTHER REMODELING handicap ramps'
        WHEN job_code_secondary = '256.4' THEN 'OTHER REMODELING awnings'
        WHEN job_code_secondary = '256.5' THEN 'OTHER REMODELING fire alarms'
        WHEN
            job_code_secondary = '256.6'
            THEN 'OTHER REMODELING security alarms'
        WHEN
            job_code_secondary = '256.7'
            THEN 'OTHER REMODELING fire alarms Hood Suppression System'
        WHEN
            job_code_secondary = '256.8'
            THEN 'OTHER REMODELING fire alarms Sprinkler for Fire'
        WHEN
            job_code_secondary = '256.9'
            THEN 'OTHER REMODELING fire alarms Security alarms'
        WHEN job_code_secondary = '271' THEN 'MAJOR IMPROVEMENT WRECK'
        WHEN job_code_secondary = '272' THEN 'MINOR IMPROVEMENT WRECK'
        WHEN job_code_secondary = '291' THEN 'MAJOR IMPROVEMENT BURNOUT'
        WHEN job_code_secondary = '292' THEN 'MINOR IMPROVEMENT BURNOUT'
        WHEN job_code_secondary = '311' THEN 'CARRIER NEW MAJOR CONSTRUCTION'
        WHEN
            job_code_secondary = '312'
            THEN 'NON-CARRIER NEW MAJOR CONSTRUCTION'
        WHEN job_code_secondary = '331' THEN 'CARRIER REMODELING'
        WHEN job_code_secondary = '332' THEN 'NON-CARRIER REMODELING'
        WHEN job_code_secondary = '351' THEN 'CARRIER REMODELING'
        WHEN job_code_secondary = '352' THEN 'NON-CARRIER REMODELING'
        WHEN job_code_secondary = '371' THEN 'MAJOR IMPROVEMENT WRECK'
        WHEN job_code_secondary = '372' THEN 'MINOR IMPROVEMENT WRECK'
        WHEN job_code_secondary = '391' THEN 'MAJOR IMPROVEMENT BURNOUT'
        WHEN job_code_secondary = '392' THEN 'MINOR IMPROVEMENT BURNOUT'
        WHEN job_code_secondary = '400' THEN 'EXEMPT PROPERTIES'
        WHEN job_code_secondary = '411' THEN 'LEASEHOLD'
        WHEN job_code_secondary = '500' THEN 'OFFICE PERMITS'
        WHEN job_code_secondary = '510' THEN 'RECHECKS'
        WHEN job_code_secondary = '511' THEN 'OCCUPANCY'
        WHEN job_code_secondary = '512' THEN 'PRIOR YEAR PARTIAL ASSESSMENT'
        WHEN job_code_secondary = '513' THEN 'NEW CONSTRUCTION'
        WHEN job_code_secondary = '514' THEN 'ADDED IMPROVEMENTS'
        WHEN job_code_secondary = '515' THEN 'MANUALLY ISSUED PERMIT'
        WHEN job_code_secondary = '516' THEN 'PRIOR YEAR RECHECK'
        WHEN job_code_secondary = '517' THEN 'DIVISION PERMIT'
        WHEN job_code_secondary = '517.1' THEN 'DIVISION PERMIT Petition'
        WHEN
            job_code_secondary = '517.11'
            THEN 'DIVISION PERMIT Petition Leasehold'
        WHEN job_code_secondary = '517.2' THEN 'DIVISION PERMIT Condominimum'
        WHEN job_code_secondary = '517.3' THEN 'DIVISION PERMIT Subdivision'
        WHEN job_code_secondary = '517.4' THEN 'DIVISION PERMIT Condo Removal'
        WHEN
            job_code_secondary = '517.5'
            THEN 'DIVISION PERMIT Petition Road Taking'
        WHEN job_code_secondary = '517.6' THEN 'DIVISION PERMIT Vacation'
        WHEN job_code_secondary = '517.7' THEN 'DIVISION PERMIT Dedication'
        WHEN job_code_secondary = '517.8' THEN 'DIVISION PERMIT Other'
        WHEN job_code_secondary = '517.9' THEN 'DIVISION PERMIT Mix Class'
        WHEN job_code_secondary = '518' THEN 'CK BLDG. SF/UP'
        WHEN job_code_secondary = '519' THEN 'CONVERT TO NEW MANUAL'
        WHEN job_code_secondary = '520' THEN 'AUDIT DEPT INCREASE TO CHART'
        WHEN job_code_secondary = '530' THEN 'RECENT I/C SALES'
        WHEN job_code_secondary = '549' THEN '1985 HISTORICAL LANDMARK'
        WHEN job_code_secondary = '550' THEN '1986 HISTORICAL LANDMARK'
        WHEN job_code_secondary = '551' THEN '1987 HISTORICAL LANDMARK'
        WHEN job_code_secondary = '552' THEN '1988 HISTORICAL LANDMARK'
        WHEN job_code_secondary = '553' THEN '1989 HISTORICAL LANDMARK'
        WHEN job_code_secondary = '554' THEN '1990 HISTORICAL LANDMARK'
        WHEN job_code_secondary = '555' THEN '1991 HISTORICAL LANDMARK'
        WHEN job_code_secondary = '556' THEN '1992 HISTORICAL LANDMARK'
        WHEN job_code_secondary = '557' THEN '1993 HISTORICAL LANDMARK'
        WHEN job_code_secondary = '558' THEN '1994 HISTORICAL LANDMARK'
        WHEN job_code_secondary = '559' THEN '1995 HISTORICAL LANDMARK'
        WHEN job_code_secondary = '560' THEN '1996 HISTORICAL LANDMARK'
        WHEN job_code_secondary = '561' THEN '1997 HISTORICAL LANDMARK'
        WHEN job_code_secondary = '562' THEN '1998 HISTORICAL LANDMARK'
        WHEN job_code_secondary = '563' THEN '1999 HISTORICAL LANDMARK'
        WHEN job_code_secondary = '564' THEN '2000 HISTORICAL LANDMARK'
        WHEN job_code_secondary = '565' THEN '2001 HISTORICAL LANDMARK'
        WHEN job_code_secondary = '566' THEN '2002 HISTORICAL LANDMARK'
        WHEN job_code_secondary = '911' THEN 'OCCUPANCY CODE WILL NOT BE USED'
    END AS job_code_secondary,
    work_description,
    CASE
        WHEN improvement_code_1 = '110' THEN 'NEW CONSTRUCTION - MAJOR IMPRVT'
        WHEN improvement_code_1 = '111' THEN 'NEW BUILDING'
        WHEN improvement_code_1 = '112' THEN 'ADDITIONS'
        WHEN improvement_code_1 = '113' THEN 'DORMERS'
        WHEN improvement_code_1 = '114' THEN 'OTHER - MAJOR NEW CONSTRUCTION'
        WHEN improvement_code_1 = '130' THEN 'NEW CONSTRUCTION - MINOR IMPRVMT'
        WHEN improvement_code_1 = '131' THEN 'SWIMMING POOL - TENNIS COURT'
        WHEN improvement_code_1 = '132' THEN 'DRIVEWAYS - PATIOS - WOOD DECK'
        WHEN improvement_code_1 = '133' THEN 'FENCING (and Gates)'
        WHEN improvement_code_1 = '134' THEN 'GARAGE CARPORTS BARNS'
        WHEN improvement_code_1 = '135' THEN 'OTHER - MINOR NEW CONSTRUCTION'
        WHEN improvement_code_1 = '151' THEN 'BASEMENT ROOM - REC ROOM'
        WHEN improvement_code_1 = '152' THEN 'ATTIC ROOM - BATHROOMS'
        WHEN
            improvement_code_1 = '153'
            THEN 'FIREPLACE-CENTRAL AIR CONDITIONING'
        WHEN improvement_code_1 = '154' THEN 'OTHER - REMODELING'
        WHEN improvement_code_1 = '155' THEN '???'
        WHEN improvement_code_1 = '156' THEN 'REMODELING - INTERIOR ONLY'
        WHEN improvement_code_1 = '157' THEN 'REMODELING - EXTERIOR ONLY'
        WHEN improvement_code_1 = '171' THEN 'MAJOR IMPROVEMENT - WRECK'
        WHEN improvement_code_1 = '172' THEN 'MINOR IMPROVEMENT - WRECK'
        WHEN improvement_code_1 = '191' THEN 'MAJOR IMPROVEMENT - BURNOUT'
        WHEN improvement_code_1 = '192' THEN 'MINOR IMPROVEMENT - BURNOUT'
        WHEN improvement_code_1 = '211' THEN 'NEW BUILDING'
        WHEN improvement_code_1 = '212' THEN 'ADDITIONS'
        WHEN improvement_code_1 = '213' THEN 'OTHER - MAJOR NEW CONSTRUCTION'
        WHEN improvement_code_1 = '231' THEN 'SWIMMING POOL - TENNIS COURT'
        WHEN improvement_code_1 = '232' THEN 'DRIVEWAYS - PATIOS'
        WHEN improvement_code_1 = '233' THEN 'FENCING'
        WHEN improvement_code_1 = '234' THEN 'GARAGE-BARN-BUTLER BUILDING'
        WHEN improvement_code_1 = '235' THEN 'OTHER - MINOR NEW CONSTRUCTION'
        WHEN improvement_code_1 = '236' THEN '???'
        WHEN improvement_code_1 = '251' THEN 'PARTITIONING'
        WHEN improvement_code_1 = '252' THEN 'DECONV/CONV AMT. LIV. UT.'
        WHEN improvement_code_1 = '253' THEN 'AIR CONDITIONING - FIREPLACES'
        WHEN improvement_code_1 = '254' THEN 'BASEMENT-ATTIC ROOM BATH'
        WHEN improvement_code_1 = '255' THEN 'REPLACE-REPAIR BUILDING - FACTORY'
        WHEN improvement_code_1 = '256' THEN 'OTHER REMODELING'
        WHEN improvement_code_1 = '257' THEN 'REMODELING - INTERIOR ONLY'
        WHEN improvement_code_1 = '258' THEN 'REMODELING - EXTERIOR ONLY'
        WHEN improvement_code_1 = '271' THEN 'MAJOR IMPROVEMENT - WRECK'
        WHEN improvement_code_1 = '272' THEN 'MINOR IMPROVEMENT - WRECK'
        WHEN improvement_code_1 = '291' THEN 'MAJOR IMPROVEMENT - BURNOUT'
        WHEN improvement_code_1 = '292' THEN 'MINOR IMPROVEMENT - BURNOUT'
        WHEN improvement_code_1 = '300' THEN 'RAILROAD PROPERTIES'
        WHEN improvement_code_1 = '310' THEN 'NEW CONST MAJOR IMPROVEMENT'
        WHEN improvement_code_1 = '311' THEN 'CARRIER - NEW MAJOR CONSTRUCTION'
        WHEN
            improvement_code_1 = '312'
            THEN 'NON-CARRIER NEW MAJOR CONSTRUCTION'
        WHEN
            improvement_code_1 = '330'
            THEN 'NEW CONSTRUCTION MINOR IMPROVEMENT'
        WHEN improvement_code_1 = '331' THEN 'CARRIER - REMODELING'
        WHEN improvement_code_1 = '332' THEN 'NON-CARRIER - REMODELING'
        WHEN improvement_code_1 = '350' THEN 'REMODELING'
        WHEN improvement_code_1 = '351' THEN 'CARRIER - REMODELING'
        WHEN improvement_code_1 = '352' THEN 'NON-CARRIER - REMODELING'
        WHEN improvement_code_1 = '370' THEN 'WRECKS'
        WHEN improvement_code_1 = '371' THEN 'MAJOR IMPROVEMENT WRECK'
        WHEN improvement_code_1 = '372' THEN 'MINOR IMPROVEMENT - WRECK'
        WHEN improvement_code_1 = '390' THEN 'BURNOUTS'
        WHEN improvement_code_1 = '391' THEN 'MAJOR IMPROVEMENT - BURNOUT'
        WHEN improvement_code_1 = '392' THEN 'MINOR IMPROVEMENT - BURNOUT'
        WHEN improvement_code_1 = '400' THEN 'EXEMPT PROPERTIES'
        WHEN improvement_code_1 = '411' THEN 'LEASEHOLD'
        WHEN improvement_code_1 = '510' THEN 'RECHECKS'
        WHEN improvement_code_1 = '511' THEN 'OCCUPANCY'
        WHEN improvement_code_1 = '512' THEN 'PRIOR YEAR PARTIAL ASSESSMENT'
        WHEN improvement_code_1 = '515' THEN 'MANUALLY ISSUED PERMIT'
        WHEN improvement_code_1 = '516' THEN 'PRIOR YEAR RECHECK'
        WHEN improvement_code_1 = '900' THEN 'OTHER'
    END AS improvement_code_1,
    CASE
        WHEN improvement_code_2 = '110' THEN 'NEW CONSTRUCTION - MAJOR IMPRVT'
        WHEN improvement_code_2 = '111' THEN 'NEW BUILDING'
        WHEN improvement_code_2 = '112' THEN 'ADDITIONS'
        WHEN improvement_code_2 = '113' THEN 'DORMERS'
        WHEN improvement_code_2 = '114' THEN 'OTHER - MAJOR NEW CONSTRUCTION'
        WHEN improvement_code_2 = '130' THEN 'NEW CONSTRUCTION - MINOR IMPRVMT'
        WHEN improvement_code_2 = '131' THEN 'SWIMMING POOL - TENNIS COURT'
        WHEN improvement_code_2 = '132' THEN 'DRIVEWAYS - PATIOS - WOOD DECK'
        WHEN improvement_code_2 = '133' THEN 'FENCING (and Gates)'
        WHEN improvement_code_2 = '134' THEN 'GARAGE CARPORTS BARNS'
        WHEN improvement_code_2 = '135' THEN 'OTHER - MINOR NEW CONSTRUCTION'
        WHEN improvement_code_2 = '151' THEN 'BASEMENT ROOM - REC ROOM'
        WHEN improvement_code_2 = '152' THEN 'ATTIC ROOM - BATHROOMS'
        WHEN
            improvement_code_2 = '153'
            THEN 'FIREPLACE-CENTRAL AIR CONDITIONING'
        WHEN improvement_code_2 = '154' THEN 'OTHER - REMODELING'
        WHEN improvement_code_2 = '155' THEN '???'
        WHEN improvement_code_2 = '156' THEN 'REMODELING - INTERIOR ONLY'
        WHEN improvement_code_2 = '157' THEN 'REMODELING - EXTERIOR ONLY'
        WHEN improvement_code_2 = '171' THEN 'MAJOR IMPROVEMENT - WRECK'
        WHEN improvement_code_2 = '172' THEN 'MINOR IMPROVEMENT - WRECK'
        WHEN improvement_code_2 = '191' THEN 'MAJOR IMPROVEMENT - BURNOUT'
        WHEN improvement_code_2 = '192' THEN 'MINOR IMPROVEMENT - BURNOUT'
        WHEN improvement_code_2 = '211' THEN 'NEW BUILDING'
        WHEN improvement_code_2 = '212' THEN 'ADDITIONS'
        WHEN improvement_code_2 = '213' THEN 'OTHER - MAJOR NEW CONSTRUCTION'
        WHEN improvement_code_2 = '231' THEN 'SWIMMING POOL - TENNIS COURT'
        WHEN improvement_code_2 = '232' THEN 'DRIVEWAYS - PATIOS'
        WHEN improvement_code_2 = '233' THEN 'FENCING'
        WHEN improvement_code_2 = '234' THEN 'GARAGE-BARN-BUTLER BUILDING'
        WHEN improvement_code_2 = '235' THEN 'OTHER - MINOR NEW CONSTRUCTION'
        WHEN improvement_code_2 = '236' THEN '???'
        WHEN improvement_code_2 = '251' THEN 'PARTITIONING'
        WHEN improvement_code_2 = '252' THEN 'DECONV/CONV AMT. LIV. UT.'
        WHEN improvement_code_2 = '253' THEN 'AIR CONDITIONING - FIREPLACES'
        WHEN improvement_code_2 = '254' THEN 'BASEMENT-ATTIC ROOM BATH'
        WHEN improvement_code_2 = '255' THEN 'REPLACE-REPAIR BUILDING - FACTORY'
        WHEN improvement_code_2 = '256' THEN 'OTHER REMODELING'
        WHEN improvement_code_2 = '257' THEN 'REMODELING - INTERIOR ONLY'
        WHEN improvement_code_2 = '258' THEN 'REMODELING - EXTERIOR ONLY'
        WHEN improvement_code_2 = '271' THEN 'MAJOR IMPROVEMENT - WRECK'
        WHEN improvement_code_2 = '272' THEN 'MINOR IMPROVEMENT - WRECK'
        WHEN improvement_code_2 = '291' THEN 'MAJOR IMPROVEMENT - BURNOUT'
        WHEN improvement_code_2 = '292' THEN 'MINOR IMPROVEMENT - BURNOUT'
        WHEN improvement_code_2 = '300' THEN 'RAILROAD PROPERTIES'
        WHEN improvement_code_2 = '310' THEN 'NEW CONST MAJOR IMPROVEMENT'
        WHEN improvement_code_2 = '311' THEN 'CARRIER - NEW MAJOR CONSTRUCTION'
        WHEN
            improvement_code_2 = '312'
            THEN 'NON-CARRIER NEW MAJOR CONSTRUCTION'
        WHEN
            improvement_code_2 = '330'
            THEN 'NEW CONSTRUCTION MINOR IMPROVEMENT'
        WHEN improvement_code_2 = '331' THEN 'CARRIER - REMODELING'
        WHEN improvement_code_2 = '332' THEN 'NON-CARRIER - REMODELING'
        WHEN improvement_code_2 = '350' THEN 'REMODELING'
        WHEN improvement_code_2 = '351' THEN 'CARRIER - REMODELING'
        WHEN improvement_code_2 = '352' THEN 'NON-CARRIER - REMODELING'
        WHEN improvement_code_2 = '370' THEN 'WRECKS'
        WHEN improvement_code_2 = '371' THEN 'MAJOR IMPROVEMENT WRECK'
        WHEN improvement_code_2 = '372' THEN 'MINOR IMPROVEMENT - WRECK'
        WHEN improvement_code_2 = '390' THEN 'BURNOUTS'
        WHEN improvement_code_2 = '391' THEN 'MAJOR IMPROVEMENT - BURNOUT'
        WHEN improvement_code_2 = '392' THEN 'MINOR IMPROVEMENT - BURNOUT'
        WHEN improvement_code_2 = '400' THEN 'EXEMPT PROPERTIES'
        WHEN improvement_code_2 = '411' THEN 'LEASEHOLD'
        WHEN improvement_code_2 = '510' THEN 'RECHECKS'
        WHEN improvement_code_2 = '511' THEN 'OCCUPANCY'
        WHEN improvement_code_2 = '512' THEN 'PRIOR YEAR PARTIAL ASSESSMENT'
        WHEN improvement_code_2 = '515' THEN 'MANUALLY ISSUED PERMIT'
        WHEN improvement_code_2 = '516' THEN 'PRIOR YEAR RECHECK'
        WHEN improvement_code_2 = '900' THEN 'OTHER'
    END AS improvement_code_2,
    CASE
        WHEN improvement_code_3 = '110' THEN 'NEW CONSTRUCTION - MAJOR IMPRVT'
        WHEN improvement_code_3 = '111' THEN 'NEW BUILDING'
        WHEN improvement_code_3 = '112' THEN 'ADDITIONS'
        WHEN improvement_code_3 = '113' THEN 'DORMERS'
        WHEN improvement_code_3 = '114' THEN 'OTHER - MAJOR NEW CONSTRUCTION'
        WHEN improvement_code_3 = '130' THEN 'NEW CONSTRUCTION - MINOR IMPRVMT'
        WHEN improvement_code_3 = '131' THEN 'SWIMMING POOL - TENNIS COURT'
        WHEN improvement_code_3 = '132' THEN 'DRIVEWAYS - PATIOS - WOOD DECK'
        WHEN improvement_code_3 = '133' THEN 'FENCING (and Gates)'
        WHEN improvement_code_3 = '134' THEN 'GARAGE CARPORTS BARNS'
        WHEN improvement_code_3 = '135' THEN 'OTHER - MINOR NEW CONSTRUCTION'
        WHEN improvement_code_3 = '151' THEN 'BASEMENT ROOM - REC ROOM'
        WHEN improvement_code_3 = '152' THEN 'ATTIC ROOM - BATHROOMS'
        WHEN
            improvement_code_3 = '153'
            THEN 'FIREPLACE-CENTRAL AIR CONDITIONING'
        WHEN improvement_code_3 = '154' THEN 'OTHER - REMODELING'
        WHEN improvement_code_3 = '155' THEN '???'
        WHEN improvement_code_3 = '156' THEN 'REMODELING - INTERIOR ONLY'
        WHEN improvement_code_3 = '157' THEN 'REMODELING - EXTERIOR ONLY'
        WHEN improvement_code_3 = '171' THEN 'MAJOR IMPROVEMENT - WRECK'
        WHEN improvement_code_3 = '172' THEN 'MINOR IMPROVEMENT - WRECK'
        WHEN improvement_code_3 = '191' THEN 'MAJOR IMPROVEMENT - BURNOUT'
        WHEN improvement_code_3 = '192' THEN 'MINOR IMPROVEMENT - BURNOUT'
        WHEN improvement_code_3 = '211' THEN 'NEW BUILDING'
        WHEN improvement_code_3 = '212' THEN 'ADDITIONS'
        WHEN improvement_code_3 = '213' THEN 'OTHER - MAJOR NEW CONSTRUCTION'
        WHEN improvement_code_3 = '231' THEN 'SWIMMING POOL - TENNIS COURT'
        WHEN improvement_code_3 = '232' THEN 'DRIVEWAYS - PATIOS'
        WHEN improvement_code_3 = '233' THEN 'FENCING'
        WHEN improvement_code_3 = '234' THEN 'GARAGE-BARN-BUTLER BUILDING'
        WHEN improvement_code_3 = '235' THEN 'OTHER - MINOR NEW CONSTRUCTION'
        WHEN improvement_code_3 = '236' THEN '???'
        WHEN improvement_code_3 = '251' THEN 'PARTITIONING'
        WHEN improvement_code_3 = '252' THEN 'DECONV/CONV AMT. LIV. UT.'
        WHEN improvement_code_3 = '253' THEN 'AIR CONDITIONING - FIREPLACES'
        WHEN improvement_code_3 = '254' THEN 'BASEMENT-ATTIC ROOM BATH'
        WHEN improvement_code_3 = '255' THEN 'REPLACE-REPAIR BUILDING - FACTORY'
        WHEN improvement_code_3 = '256' THEN 'OTHER REMODELING'
        WHEN improvement_code_3 = '257' THEN 'REMODELING - INTERIOR ONLY'
        WHEN improvement_code_3 = '258' THEN 'REMODELING - EXTERIOR ONLY'
        WHEN improvement_code_3 = '271' THEN 'MAJOR IMPROVEMENT - WRECK'
        WHEN improvement_code_3 = '272' THEN 'MINOR IMPROVEMENT - WRECK'
        WHEN improvement_code_3 = '291' THEN 'MAJOR IMPROVEMENT - BURNOUT'
        WHEN improvement_code_3 = '292' THEN 'MINOR IMPROVEMENT - BURNOUT'
        WHEN improvement_code_3 = '300' THEN 'RAILROAD PROPERTIES'
        WHEN improvement_code_3 = '310' THEN 'NEW CONST MAJOR IMPROVEMENT'
        WHEN improvement_code_3 = '311' THEN 'CARRIER - NEW MAJOR CONSTRUCTION'
        WHEN
            improvement_code_3 = '312'
            THEN 'NON-CARRIER NEW MAJOR CONSTRUCTION'
        WHEN
            improvement_code_3 = '330'
            THEN 'NEW CONSTRUCTION MINOR IMPROVEMENT'
        WHEN improvement_code_3 = '331' THEN 'CARRIER - REMODELING'
        WHEN improvement_code_3 = '332' THEN 'NON-CARRIER - REMODELING'
        WHEN improvement_code_3 = '350' THEN 'REMODELING'
        WHEN improvement_code_3 = '351' THEN 'CARRIER - REMODELING'
        WHEN improvement_code_3 = '352' THEN 'NON-CARRIER - REMODELING'
        WHEN improvement_code_3 = '370' THEN 'WRECKS'
        WHEN improvement_code_3 = '371' THEN 'MAJOR IMPROVEMENT WRECK'
        WHEN improvement_code_3 = '372' THEN 'MINOR IMPROVEMENT - WRECK'
        WHEN improvement_code_3 = '390' THEN 'BURNOUTS'
        WHEN improvement_code_3 = '391' THEN 'MAJOR IMPROVEMENT - BURNOUT'
        WHEN improvement_code_3 = '392' THEN 'MINOR IMPROVEMENT - BURNOUT'
        WHEN improvement_code_3 = '400' THEN 'EXEMPT PROPERTIES'
        WHEN improvement_code_3 = '411' THEN 'LEASEHOLD'
        WHEN improvement_code_3 = '510' THEN 'RECHECKS'
        WHEN improvement_code_3 = '511' THEN 'OCCUPANCY'
        WHEN improvement_code_3 = '512' THEN 'PRIOR YEAR PARTIAL ASSESSMENT'
        WHEN improvement_code_3 = '515' THEN 'MANUALLY ISSUED PERMIT'
        WHEN improvement_code_3 = '516' THEN 'PRIOR YEAR RECHECK'
        WHEN improvement_code_3 = '900' THEN 'OTHER'
    END AS improvement_code_3,
    CASE
        WHEN improvement_code_4 = '110' THEN 'NEW CONSTRUCTION - MAJOR IMPRVT'
        WHEN improvement_code_4 = '111' THEN 'NEW BUILDING'
        WHEN improvement_code_4 = '112' THEN 'ADDITIONS'
        WHEN improvement_code_4 = '113' THEN 'DORMERS'
        WHEN improvement_code_4 = '114' THEN 'OTHER - MAJOR NEW CONSTRUCTION'
        WHEN improvement_code_4 = '130' THEN 'NEW CONSTRUCTION - MINOR IMPRVMT'
        WHEN improvement_code_4 = '131' THEN 'SWIMMING POOL - TENNIS COURT'
        WHEN improvement_code_4 = '132' THEN 'DRIVEWAYS - PATIOS - WOOD DECK'
        WHEN improvement_code_4 = '133' THEN 'FENCING (and Gates)'
        WHEN improvement_code_4 = '134' THEN 'GARAGE CARPORTS BARNS'
        WHEN improvement_code_4 = '135' THEN 'OTHER - MINOR NEW CONSTRUCTION'
        WHEN improvement_code_4 = '151' THEN 'BASEMENT ROOM - REC ROOM'
        WHEN improvement_code_4 = '152' THEN 'ATTIC ROOM - BATHROOMS'
        WHEN
            improvement_code_4 = '153'
            THEN 'FIREPLACE-CENTRAL AIR CONDITIONING'
        WHEN improvement_code_4 = '154' THEN 'OTHER - REMODELING'
        WHEN improvement_code_4 = '155' THEN '???'
        WHEN improvement_code_4 = '156' THEN 'REMODELING - INTERIOR ONLY'
        WHEN improvement_code_4 = '157' THEN 'REMODELING - EXTERIOR ONLY'
        WHEN improvement_code_4 = '171' THEN 'MAJOR IMPROVEMENT - WRECK'
        WHEN improvement_code_4 = '172' THEN 'MINOR IMPROVEMENT - WRECK'
        WHEN improvement_code_4 = '191' THEN 'MAJOR IMPROVEMENT - BURNOUT'
        WHEN improvement_code_4 = '192' THEN 'MINOR IMPROVEMENT - BURNOUT'
        WHEN improvement_code_4 = '211' THEN 'NEW BUILDING'
        WHEN improvement_code_4 = '212' THEN 'ADDITIONS'
        WHEN improvement_code_4 = '213' THEN 'OTHER - MAJOR NEW CONSTRUCTION'
        WHEN improvement_code_4 = '231' THEN 'SWIMMING POOL - TENNIS COURT'
        WHEN improvement_code_4 = '232' THEN 'DRIVEWAYS - PATIOS'
        WHEN improvement_code_4 = '233' THEN 'FENCING'
        WHEN improvement_code_4 = '234' THEN 'GARAGE-BARN-BUTLER BUILDING'
        WHEN improvement_code_4 = '235' THEN 'OTHER - MINOR NEW CONSTRUCTION'
        WHEN improvement_code_4 = '236' THEN '???'
        WHEN improvement_code_4 = '251' THEN 'PARTITIONING'
        WHEN improvement_code_4 = '252' THEN 'DECONV/CONV AMT. LIV. UT.'
        WHEN improvement_code_4 = '253' THEN 'AIR CONDITIONING - FIREPLACES'
        WHEN improvement_code_4 = '254' THEN 'BASEMENT-ATTIC ROOM BATH'
        WHEN improvement_code_4 = '255' THEN 'REPLACE-REPAIR BUILDING - FACTORY'
        WHEN improvement_code_4 = '256' THEN 'OTHER REMODELING'
        WHEN improvement_code_4 = '257' THEN 'REMODELING - INTERIOR ONLY'
        WHEN improvement_code_4 = '258' THEN 'REMODELING - EXTERIOR ONLY'
        WHEN improvement_code_4 = '271' THEN 'MAJOR IMPROVEMENT - WRECK'
        WHEN improvement_code_4 = '272' THEN 'MINOR IMPROVEMENT - WRECK'
        WHEN improvement_code_4 = '291' THEN 'MAJOR IMPROVEMENT - BURNOUT'
        WHEN improvement_code_4 = '292' THEN 'MINOR IMPROVEMENT - BURNOUT'
        WHEN improvement_code_4 = '300' THEN 'RAILROAD PROPERTIES'
        WHEN improvement_code_4 = '310' THEN 'NEW CONST MAJOR IMPROVEMENT'
        WHEN improvement_code_4 = '311' THEN 'CARRIER - NEW MAJOR CONSTRUCTION'
        WHEN
            improvement_code_4 = '312'
            THEN 'NON-CARRIER NEW MAJOR CONSTRUCTION'
        WHEN
            improvement_code_4 = '330'
            THEN 'NEW CONSTRUCTION MINOR IMPROVEMENT'
        WHEN improvement_code_4 = '331' THEN 'CARRIER - REMODELING'
        WHEN improvement_code_4 = '332' THEN 'NON-CARRIER - REMODELING'
        WHEN improvement_code_4 = '350' THEN 'REMODELING'
        WHEN improvement_code_4 = '351' THEN 'CARRIER - REMODELING'
        WHEN improvement_code_4 = '352' THEN 'NON-CARRIER - REMODELING'
        WHEN improvement_code_4 = '370' THEN 'WRECKS'
        WHEN improvement_code_4 = '371' THEN 'MAJOR IMPROVEMENT WRECK'
        WHEN improvement_code_4 = '372' THEN 'MINOR IMPROVEMENT - WRECK'
        WHEN improvement_code_4 = '390' THEN 'BURNOUTS'
        WHEN improvement_code_4 = '391' THEN 'MAJOR IMPROVEMENT - BURNOUT'
        WHEN improvement_code_4 = '392' THEN 'MINOR IMPROVEMENT - BURNOUT'
        WHEN improvement_code_4 = '400' THEN 'EXEMPT PROPERTIES'
        WHEN improvement_code_4 = '411' THEN 'LEASEHOLD'
        WHEN improvement_code_4 = '510' THEN 'RECHECKS'
        WHEN improvement_code_4 = '511' THEN 'OCCUPANCY'
        WHEN improvement_code_4 = '512' THEN 'PRIOR YEAR PARTIAL ASSESSMENT'
        WHEN improvement_code_4 = '515' THEN 'MANUALLY ISSUED PERMIT'
        WHEN improvement_code_4 = '516' THEN 'PRIOR YEAR RECHECK'
        WHEN improvement_code_4 = '900' THEN 'OTHER'
    END AS improvement_code_4
FROM {{ ref('default.vw_pin_permit') }}
