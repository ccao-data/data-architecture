-- View that supplies dynamic datetime values in a way that we can mock for
-- unit tests. See:
-- https://discourse.getdbt.com/t/dynamic-dates-in-unit-tests/16883/2
SELECT CURRENT_DATE AS date_today
