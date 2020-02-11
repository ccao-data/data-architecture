ALTER VIEW VW_ASSMNTDATA AS

SELECT 

UNIVERSE.*
, PROPERTY_ADDRESS, PROPERTY_APT_NO, PROPERTY_CITY, PROPERTY_ZIP, MAILING_ADDRESS, MAILING_CITY, MAILING_ZIP
, geoid, census_tract, ward, ohare_noise, floodplain, withinmr100, withinmr101300
, commissioner_dist, reps_dist, senate_dist, tif_agencynum, PUMA, municipality, FIPS
, midincome, white_perc, black_perc, his_perc, other_perc
, centroid_x, centroid_y
, most_recent_sale_date, most_recent_sale_price, DOC_NO, DEED_TYPE
, AMT_TAX_PAID / most_recent_sale_price as effective_tax_rate

FROM VW_RES_UNIVERSE AS UNIVERSE

LEFT JOIN
	VW_PINGEO GEO
	ON UNIVERSE.PIN = GEO.PIN
LEFT JOIN
	(SELECT * FROM VW_MOST_RECENT_IDORSALES
	WHERE YEAR(most_recent_sale_date) >= (SELECT MAX(TAX_YEAR) FROM AS_HEADT) - 1) SALES
	ON UNIVERSE.PIN = SALES.PIN

WHERE UNIVERSE.TAX_YEAR = (SELECT MAX(TAX_YEAR) FROM AS_HEADT)