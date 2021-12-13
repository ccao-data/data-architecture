-- View for location data and identifiers
CREATE OR REPLACE VIEW default.vw_pin_location AS
SELECT
    par.parid AS pin,
    par.taxyr AS year,
    par.class AS class,
    SUBSTR(leg.taxdist, 1, 2) AS town_code,
    par.nbhd AS nbhd_code,
    leg.taxdist AS tax_code,
    sp.lon, sp.lat,
    sp.x_3435, sp.y_3435,
    cen_tract.geoid AS census_geoid_tract
FROM iasworld.pardat par
LEFT JOIN iasworld.legdat leg
    ON par.parid = leg.parid
    AND par.taxyr = leg.taxyr
LEFT JOIN spatial.parcel sp
    ON par.parid = RPAD(sp.pin10, 14, '0')
    AND par.taxyr = sp.year
LEFT JOIN (
    SELECT *
    FROM spatial.census
    WHERE geography = 'tract'
) cen_tract
    ON par.taxyr = cen_tract.year
    AND ST_Within(
        ST_Point(sp.x_3435, sp.y_3435),
        ST_GeomFromBinary(cen_tract.geometry_3435)
    )

--x tax code
--x nbhd
--x town
-- Census (all geos)
-- FEMA flood
-- ohare in noise
-- school dist (elem, sec, unif)
-- FS flood
-- uninc
-- subdivisions

-- SSAs
-- TIF
-- municipality
-- state rep
-- state senate
-- commissioner
-- congressional
-- judicial
-- wards