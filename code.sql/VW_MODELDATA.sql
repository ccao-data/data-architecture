ALTER VIEW VW_MODELDATA AS

SELECT

UNIVERSE.*
, PROPERTY_ADDRESS, PROPERTY_APT_NO, PROPERTY_CITY, PROPERTY_ZIP, MAILING_ADDRESS, MAILING_CITY, MAILING_ZIP
, geoid, census_tract, ward, ohare_noise, floodplain, withinmr100, withinmr101300
, commissioner_dist, reps_dist, senate_dist, tif_agencynum, PUMA, municipality, FIPS
, midincome, white_perc, black_perc, his_perc, other_perc
, centroid_x, centroid_y
, sale_date, sale_price, DOC_NO, DEED_TYPE

FROM VW_RES_UNIVERSE AS UNIVERSE

LEFT JOIN
	VW_PINGEO GEO
	ON UNIVERSE.PIN = GEO.PIN
INNER JOIN
	VW_VALID_IDORSALES SALES
	ON UNIVERSE.PIN = SALES.PIN AND UNIVERSE.TAX_YEAR = SALES.TAX_YEAR