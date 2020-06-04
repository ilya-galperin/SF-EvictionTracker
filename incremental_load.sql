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


-- Populate Location Dimension

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
	
	
-- Populate Reason Bridge Table

SELECT DISTINCT
	reason_group_key,
	ARRAY_AGG(reason_key ORDER BY reason_key ASC) as rk_array
INTO TEMP tmp_existing_reason_groups
FROM staging.br_Reason_Group
GROUP BY reason_group_key; --123

SELECT 
	concat_reason,
	ARRAY_AGG(reason_key ORDER BY reason_key ASC) as rk_array
INTO TEMP tmp_new_reason_groups
FROM (
	SELECT DISTINCT
		string_to_array(TRIM(TRAILING '|' FROM (CASE WHEN concat_reason = '' THEN 'Unknown' ELSE concat_reason END)), '|') as concat_reason,
		unnest(string_to_array(TRIM(TRAILING '|' FROM (CASE WHEN concat_reason = '' THEN 'Unknown' ELSE concat_reason END)), '|')) unnested_reason
	FROM (
		SELECT DISTINCT
			CASE WHEN non_payment = 'true' THEN 'non_payment|' ELSE '' END||
			CASE WHEN breach = 'true' THEN 'breach|' ELSE '' END||
			CASE WHEN nuisance = 'true' THEN 'nuisance|' ELSE '' END||
			CASE WHEN illegal_use = 'true' THEN 'illegal_use|' ELSE '' END||
			CASE WHEN failure_to_sign_renewal = 'true' THEN 'failure_to_sign_renewal|' ELSE '' END||
			CASE WHEN access_denial = 'true' THEN 'access_denial|' ELSE '' END||
			CASE WHEN unapproved_subtenant = 'true' THEN 'unapproved_subtenant|' ELSE '' END||
			CASE WHEN owner_move_in = 'true' THEN 'owner_move_in|' ELSE '' END||
			CASE WHEN demolition = 'true' THEN 'demolition|' ELSE '' END||
			CASE WHEN capital_improvement = 'true' THEN 'capital_improvement|' ELSE '' END||
			CASE WHEN substantial_rehab = 'true' THEN 'substantial_rehab|' ELSE '' END||
			CASE WHEN ellis_act_withdrawal = 'true' THEN 'ellis_act_withdrawal|' ELSE '' END||
			CASE WHEN condo_conversion = 'true' THEN 'condo_conversion|' ELSE '' END||
			CASE WHEN roommate_same_unit = 'true' THEN 'roommate_same_unit|' ELSE '' END||
			CASE WHEN other_cause = 'true' THEN 'other_cause|' ELSE '' END||
			CASE WHEN late_payments = 'true' THEN 'late_payments|' ELSE '' END||
			CASE WHEN lead_remediation = 'true' THEN 'lead_remediation|' ELSE '' END||
			CASE WHEN development = 'true' THEN 'development|' ELSE '' END||
			CASE WHEN good_samaritan_ends = 'true' THEN 'good_samaritan_ends|' ELSE '' END
				as concat_reason
		FROM raw.soda_evictions
		) se1
	GROUP BY concat_reason
	) se2
JOIN staging.dim_Reason r ON se2.unnested_reason = r.reason_code
GROUP BY concat_reason; --22

INSERT INTO staging.br_Reason_Group (reason_group_key, reason_key)
SELECT
	final_grp.max_key + new_grp.tmp_group_key as reason_group_key,
	new_grp.reason_key as reason_key
FROM (
	SELECT DISTINCT
		ROW_NUMBER() OVER(ORDER BY concat_reason) as tmp_group_key,
		concat_reason,
		unnest(n.rk_array) as reason_key
	FROM tmp_new_reason_groups n --22
	LEFT JOIN tmp_existing_reason_groups e ON n.rk_array = e.rk_array
	WHERE e.reason_group_key IS NULL
	) new_grp
LEFT JOIN (SELECT MAX(reason_group_key) max_key FROM staging.br_Reason_Group) final_grp ON 1=1
ORDER BY reason_group_key, reason_key;

DROP TABLE tmp_existing_reason_groups;
DROP TABLE tmp_new_reason_groups;


-- Migrate to Production Schema

INSERT INTO prod.dim_Location (location_key, neighborhood, supervisor_district, city, state, zip_code)
SELECT location_key, neighborhood, supervisor_district, city, state, zip_code
FROM staging.dim_Location
	ON CONFLICT (location_key) DO NOTHING;	

INSERT INTO prod.br_Reason_Group (reason_group_key, reason_key)
SELECT stg.reason_group_key, stg.reason_key 
FROM staging.br_Reason_Group stg
LEFT JOIN prod.br_Reason_Group prd 
	ON stg.reason_group_key = prd.reason_group_key 
	AND stg.reason_key = prd.reason_key
WHERE 
	prd.reason_group_key IS NULL;


