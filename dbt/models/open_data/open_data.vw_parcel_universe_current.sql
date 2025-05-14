-- Most-recent-year-only copy of open_data.vw_parcel_historic. Feeds the "Parcel
-- Universe (Current Year)" open data asset.
-- Some columns from the feeder view may not be present in this view.

SELECT feeder.*
FROM {{ ref('vw_parcel_universe_historical') }} AS feeder
WHERE feeder.year
    = (SELECT MAX(year) FROM {{ ref('vw_parcel_universe_historical') }})
