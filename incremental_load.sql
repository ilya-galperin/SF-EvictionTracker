TRUNCATE TABLE raw.soda_evictions;

INSERT INTO raw.soda_evictions (eviction_id, raw_id, created_at, updated_at, address, city, state, zip, file_date, 
								constraints_date, supervisor_district, neighborhood,
								breach, non_payment, nuisance, illegal_use, failure_to_sign_renewal, access_denial,
								unapproved_subtenant, owner_move_in, demolition, capital_improvement, substantial_rehab,
								ellis_act_withdrawal, condo_conversion, roommate_same_unit, other_cause, late_payments, 
								lead_remediation, development, good_samaritan_ends)

VALUES ( 'M080633', 'z0001', '2020-01-01', '2020-06-01', 'addrTest', 'cityTest', 'stateTest', 'zipTest', '2020-01-01',
		'2020-01-01', '999', 'nbrhdTest',
		'false', 'false', 'false', 'false', 'false', 'false',
		'false', 'false', 'false', 'false', 'false',
		'false', 'false', 'false', 'false', 'false',
		'false', 'false', 'false'),
		( 'M170228', 'z0002', '2020-01-01', '2020-06-01', 'addrTest', 'cityTest', 'stateTest', 'zipTest', '2020-01-01',
		'2020-01-01', '999', 'nbrhdTest',
		'true', 'true', 'true', 'true', 'true', 'true', 
		'true', 'true', 'true', 'true', 'true',
		'true', 'true', 'true', 'true', 'true',
		'true', 'true', 'true'),
		( 'M200108', 'z0003', '2019-01-01', '2020-06-02', 'addrTest', 'cityTest', 'stateTest', 'zipTest', '2019-01-02',
		'2019-01-03', '999', 'nbrhdTest',
		'true', 'true', 'true', 'true', 'true', 'true', 
		'true', 'true', 'true', 'true', 'true',
		'false', 'false', 'false', 'false', 'false',
		'false', 'false', 'false'),
		( 'M200113', 'z0004', '2019-01-01', '2020-06-02', 'addrTest', 'cityTest', 'stateTest', 'zipTest', '2019-01-02',
		'2019-01-03', '999', 'nbrhdTest',
		'false', 'false', 'false', 'false', 'false', 'false', 
		'false', 'false', 'false', 'false', 'false',
		'false', 'false', 'false', 'false', 'false',
		'false', 'false', 'true'),
		( 'M200110', 'z0005', '2020-06-02', '2020-06-02', 'zzz', 'zzz', 'zzz', 'zzz', '2020-01-01',
		'2020-01-01', '999', 'zzz',
		'false', 'false', 'false', 'false', 'false', 'false', 
		'false', 'false', 'false', 'false', 'false',
		'false', 'false', 'false', 'false', 'false',
		'true', 'false', 'false')


TRUNCATE TABLE staging.fact_Evictions;

--- WONT NEED TO DO THIS PART
TRUNCATE TABLE staging.dim_Location;
INSERT INTO staging.dim_Location (location_key, neighborhood, supervisor_district, city, state, zip_code)
SELECT location_key, neighborhood, supervisor_district, city, state, zip_code
FROM prod.dim_Location;
--

INSERT INTO staging.dim_Location (neighborhood, supervisor_district, city, state, zip_code)
SELECT 
	se.neighborhood,
	se.supervisor_district,
	se.city,
	se.state,
	se.zip_code
FROM (
	SELECT DISTINCT
		COALESCE(neighborhood, 'Unknown') as neighborhood,
		COALESCE(supervisor_district, 'Unknown') as supervisor_district,
		COALESCE(city, 'Uknown') as city,
		COALESCE(state, 'Unknown') as state,
		COALESCE(zip, 'Unknown') as zip_code
	FROM raw.soda_evictions
	) se
LEFT JOIN staging.dim_Location dl 
	ON se.neighborhood = dl.neighborhood
	AND se.supervisor_district = dl.supervisor_district
	AND se.city = dl.city
	AND se.state = dl.state
	AND se.zip_code = dl.zip_code
WHERE 
	dl.location_key IS NULL;
	

INSERT INTO prod.dim_Location (location_key, neighborhood, supervisor_district, city, state, zip_code)
SELECT location_key, neighborhood, supervisor_district, city, state, zip_code
FROM staging.dim_Location
ON CONFLICT (location_key) DO NOTHING;
	
	
-- CREATE ARRAY LIST FROM NEW FACTS
-- REBUILD ARRAY LIST FROM STAGING BRIDGE/REASON TABLES, BRIDGE KEY + ARRAY LIST 
-- JOIN ARRAY LIST ON BRIDGE KEY ARRAY LIST, IF DOES NOT EXIST THEN CREATE NEW BRIDGE KEY
-- JOIN UPDATED BRIDGE TABLE BACK TO THE RAW FACTS ON THE ARRAYS OF BOTH TO GET THE BRIDGE KEY FOR FACT TABLE




