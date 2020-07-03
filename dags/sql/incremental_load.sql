-- echo "" > /home/airflow/airflow/dags/sql/incremental_load.sql
-- nano /home/airflow/airflow/dags/sql/incremental_load.sql

-- Populate District Dimension

INSERT INTO staging.dim_district
	(district, population, households, percent_asian, percent_black, percent_white, percent_native_am,
	percent_pacific_isle, percent_other_race, percent_latin, median_age, total_units, 
	percent_owner_occupied, percent_renter_occupied, median_rent_as_perc_of_income, median_household_income, 
	median_family_income, per_capita_income, percent_in_poverty)
SELECT 
	district,
	population::int,
	households::int,
	perc_asian::numeric as percent_asian,
	perc_black::numeric as percent_black,
	perc_white::numeric as percent_white,
	perc_nat_am::numeric as percent_native_am,
	perc_nat_pac::numeric as percent_pacific_isle,
	perc_other::numeric as percent_other_race,
	perc_latin::numeric as percent_latin,
	median_age::numeric,
	total_units::int,
	perc_owner_occupied::numeric as percent_owner_occupied,
	perc_renter_occupied::numeric as percent_renter_occupied,
	median_rent_as_perc_of_income::numeric,
	median_household_income::numeric,
	median_family_income::numeric, 			
	per_capita_income::numeric,
	perc_in_poverty::numeric as percent_in_poverty
FROM raw.district_data
	ON CONFLICT (district) DO UPDATE SET 
		population = EXCLUDED.population,
		households = EXCLUDED.households,
		percent_asian = EXCLUDED.percent_asian,
		percent_black = EXCLUDED.percent_black,
		percent_white = EXCLUDED.percent_white,
		percent_native_am = EXCLUDED.percent_native_am,
		percent_pacific_isle = EXCLUDED.percent_pacific_isle,
		percent_other_race = EXCLUDED.percent_other_race,
		percent_latin = EXCLUDED.percent_latin,
		median_age = EXCLUDED.median_age,
		total_units = EXCLUDED.total_units,
		percent_owner_occupied = EXCLUDED.percent_owner_occupied,
		percent_renter_occupied = EXCLUDED.percent_renter_occupied,
		median_rent_as_perc_of_income = EXCLUDED.median_rent_as_perc_of_income,
		median_household_income = EXCLUDED.median_household_income,
		median_family_income = EXCLUDED.median_family_income,
		per_capita_income = EXCLUDED.per_capita_income,
		percent_in_poverty = EXCLUDED.percent_in_poverty;


-- Populate Neighborhood Dimension

INSERT INTO staging.dim_neighborhood
	(neighborhood, neighborhood_alt_name, population, households, percent_asian, percent_black, percent_white, 
	percent_native_am, percent_pacific_isle, percent_other_race, percent_latin, median_age, total_units, 
	percent_owner_occupied, percent_renter_occupied, median_rent_as_perc_of_income, median_household_income, 
	median_family_income, per_capita_income, percent_in_poverty)
SELECT 
	acs_name as neighborhood,
	db_name as neighborhood_alt_name,
	population::int,
	households::int,
	perc_asian::numeric as percent_asian,
	perc_black::numeric as percent_black,
	perc_white::numeric as percent_white,
	perc_nat_am::numeric as percent_native_am,
	perc_nat_pac::numeric as percent_pacific_isle,
	perc_other::numeric as percent_other_race,
	perc_latin::numeric as percent_latin,
	median_age::numeric,
	total_units::int,
	perc_owner_occupied::numeric as percent_owner_occupied,
	perc_renter_occupied::numeric as percent_renter_occupied,
	median_rent_as_perc_of_income::numeric,
	median_household_income::numeric,
	median_family_income::numeric,
	per_capita_income::numeric,
	perc_in_poverty::numeric as percent_in_poverty
FROM raw.neighborhood_data
	ON CONFLICT (neighborhood) DO UPDATE SET
		neighborhood_alt_name = EXCLUDED.neighborhood_alt_name,
		population = EXCLUDED.population,
		households = EXCLUDED.households,
		percent_asian = EXCLUDED.percent_asian,
		percent_black = EXCLUDED.percent_black,
		percent_white = EXCLUDED.percent_white,
		percent_native_am = EXCLUDED.percent_native_am,
		percent_pacific_isle = EXCLUDED.percent_pacific_isle,
		percent_other_race = EXCLUDED.percent_other_race,
		percent_latin = EXCLUDED.percent_latin,
		median_age = EXCLUDED.median_age,
		total_units = EXCLUDED.total_units,
		percent_owner_occupied = EXCLUDED.percent_owner_occupied,
		percent_renter_occupied = EXCLUDED.percent_renter_occupied,
		median_rent_as_perc_of_income = EXCLUDED.median_rent_as_perc_of_income,
		median_household_income = EXCLUDED.median_household_income,
		median_family_income = EXCLUDED.median_family_income,
		per_capita_income = EXCLUDED.per_capita_income,
		percent_in_poverty = EXCLUDED.percent_in_poverty;


-- Populate Location Dimension

INSERT INTO staging.dim_location (city, state, zip_code)
SELECT 
	se.city,
	se.state,
	se.zip_code
FROM (
	SELECT DISTINCT
		COALESCE(city, 'Unknown') as city,
		COALESCE(state, 'Unknown') as state,
		COALESCE(zip, 'Unknown') as zip_code
	FROM raw.soda_evictions
	) se
LEFT JOIN staging.dim_location dl 
	ON se.city = dl.city
	AND se.state = dl.state
	AND se.zip_code = dl.zip_code
WHERE 
	dl.location_key IS NULL;
	
	
-- Populate Reason Bridge Table

SELECT DISTINCT
	reason_group_key,
	ARRAY_AGG(reason_key ORDER BY reason_key ASC) as rk_array
INTO TEMP tmp_existing_reason_groups
FROM staging.br_reason_group
GROUP BY reason_group_key; 

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
JOIN staging.dim_reason r ON se2.unnested_reason = r.reason_code
GROUP BY concat_reason; 

INSERT INTO staging.br_reason_group (reason_group_key, reason_key)
SELECT
	final_grp.max_key + new_grp.tmp_group_key as reason_group_key,
	new_grp.reason_key as reason_key
FROM (
	SELECT DISTINCT
		ROW_NUMBER() OVER(ORDER BY concat_reason) as tmp_group_key,
		concat_reason,
		unnest(n.rk_array) as reason_key
	FROM tmp_new_reason_groups n
	LEFT JOIN tmp_existing_reason_groups e ON n.rk_array = e.rk_array
	WHERE e.reason_group_key IS NULL
	) new_grp
LEFT JOIN (SELECT MAX(reason_group_key) max_key FROM staging.br_reason_group) final_grp ON 1=1
ORDER BY reason_group_key, reason_key;

DROP TABLE tmp_existing_reason_groups;
DROP TABLE tmp_new_reason_groups;

					    	    
-- Populate Staging Fact Table

SELECT DISTINCT
	reason_group_key,
	ARRAY_AGG(reason_key ORDER BY reason_key ASC) as rk_array
INTO TEMP tmp_existing_reason_groups
FROM staging.br_reason_group
GROUP BY reason_group_key; 
					    
SELECT 
	eviction_id,
	ARRAY_AGG(reason_key ORDER BY reason_key ASC) as rk_array
INTO TEMP tmp_fct_reason_groups	
FROM (
	SELECT 
		eviction_id,
		unnest(string_to_array(TRIM(TRAILING '|' FROM (CASE WHEN concat_reason = '' THEN 'Unknown' ELSE concat_reason END)), '|')) as unnested_reason
	FROM (
		SELECT 
			eviction_id,
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
	) se2
JOIN staging.dim_reason r ON se2.unnested_reason = r.reason_code	
GROUP BY se2.eviction_id; 
					    
SELECT
	eviction_id, 
	reason_group_key
INTO tmp_reason_group_lookup
FROM tmp_fct_reason_groups f
JOIN tmp_existing_reason_groups d ON f.rk_array = d.rk_array;			    


TRUNCATE TABLE staging.fact_evictions;
					    
INSERT INTO staging.fact_evictions 
	(eviction_key, district_key, neighborhood_key, location_key, reason_group_key, file_date_key, constraints_date_key, street_address)
SELECT
	f.eviction_id as eviction_key,
	COALESCE(d.district_key, -1) as district_key,
	COALESCE(n.neighborhood_key, -1) as neighborhood_key,
	COALESCE(l.location_key, -1) as location_key,
	r.reason_group_key as reason_group_key,
	COALESCE(dt1.date_key, -1) as file_date_key,
	COALESCE(dt2.date_key, -1) as constraints_date_key,
	f.address as street_address
FROM raw.soda_evictions f
JOIN tmp_reason_group_lookup r ON f.eviction_id = r.eviction_id
LEFT JOIN staging.dim_district d ON f.supervisor_district = d.district
LEFT JOIN staging.dim_neighborhood n ON f.neighborhood = n.neighborhood_alt_name
LEFT JOIN staging.dim_location l 
	ON COALESCE(f.city, 'Unknown') = l.city
	AND COALESCE(f.state, 'Unknown') = l.state
	AND COALESCE(f.zip, 'Unknown') = l.zip_code
LEFT JOIN staging.dim_date dt1 ON f.file_date = dt1.date
LEFT JOIN staging.dim_date dt2 ON f.constraints_date = dt2.date;

DROP TABLE tmp_existing_reason_groups;
DROP TABLE tmp_fct_reason_groups;
DROP TABLE tmp_reason_group_lookup;

					    
					    
-- Merge Into Production Schema

INSERT INTO prod.dim_district
	(district_key, district, population, households, percent_asian, percent_black, percent_white, percent_native_am,
	percent_pacific_isle, percent_other_race, percent_latin, median_age, total_units, 
	percent_owner_occupied, percent_renter_occupied, median_rent_as_perc_of_income, median_household_income, 
	median_family_income, per_capita_income, percent_in_poverty)
SELECT 	
	district_key, district, population, households, percent_asian, percent_black, percent_white, percent_native_am,
	percent_pacific_isle, percent_other_race, percent_latin, median_age, total_units, 
	percent_owner_occupied, percent_renter_occupied, median_rent_as_perc_of_income, median_household_income, 
	median_family_income, per_capita_income, percent_in_poverty
FROM staging.dim_district	
	ON CONFLICT(district_key) DO UPDATE SET
		district = EXCLUDED.district,
		population = EXCLUDED.population,
		households = EXCLUDED.households,
		percent_asian = EXCLUDED.percent_asian,
		percent_black = EXCLUDED.percent_black,
		percent_white = EXCLUDED.percent_white,
		percent_native_am = EXCLUDED.percent_native_am,
		percent_pacific_isle = EXCLUDED.percent_pacific_isle,
		percent_other_race = EXCLUDED.percent_other_race,
		percent_latin = EXCLUDED.percent_latin,
		median_age = EXCLUDED.median_age,
		total_units = EXCLUDED.total_units,
		percent_owner_occupied = EXCLUDED.percent_owner_occupied,
		percent_renter_occupied = EXCLUDED.percent_renter_occupied,
		median_rent_as_perc_of_income = EXCLUDED.median_rent_as_perc_of_income,
		median_household_income = EXCLUDED.median_household_income,
		median_family_income = EXCLUDED.median_family_income,
		per_capita_income = EXCLUDED.per_capita_income,
		percent_in_poverty = EXCLUDED.percent_in_poverty;


INSERT INTO prod.dim_neighborhood
	(neighborhood_key, neighborhood, neighborhood_alt_name, population, households, percent_asian, percent_black, percent_white, 
	percent_native_am, percent_pacific_isle, percent_other_race, percent_latin, median_age, total_units, 
	percent_owner_occupied, percent_renter_occupied, median_rent_as_perc_of_income, median_household_income, 
	median_family_income, per_capita_income, percent_in_poverty)
SELECT 
	neighborhood_key, neighborhood, neighborhood_alt_name, population, households, percent_asian, percent_black, percent_white, 
	percent_native_am, percent_pacific_isle, percent_other_race, percent_latin, median_age, total_units, 
	percent_owner_occupied, percent_renter_occupied, median_rent_as_perc_of_income, median_household_income, 
	median_family_income, per_capita_income, percent_in_poverty
FROM staging.dim_neighborhood
	ON CONFLICT (neighborhood_key) DO UPDATE SET
		neighborhood = EXCLUDED.neighborhood,
		neighborhood_alt_name = EXCLUDED.neighborhood_alt_name,
		population = EXCLUDED.population,
		households = EXCLUDED.households,
		percent_asian = EXCLUDED.percent_asian,
		percent_black = EXCLUDED.percent_black,
		percent_white = EXCLUDED.percent_white,
		percent_native_am = EXCLUDED.percent_native_am,
		percent_pacific_isle = EXCLUDED.percent_pacific_isle,
		percent_other_race = EXCLUDED.percent_other_race,
		percent_latin = EXCLUDED.percent_latin,
		median_age = EXCLUDED.median_age,
		total_units = EXCLUDED.total_units,
		percent_owner_occupied = EXCLUDED.percent_owner_occupied,
		percent_renter_occupied = EXCLUDED.percent_renter_occupied,
		median_rent_as_perc_of_income = EXCLUDED.median_rent_as_perc_of_income,
		median_household_income = EXCLUDED.median_household_income,
		median_family_income = EXCLUDED.median_family_income,
		per_capita_income = EXCLUDED.per_capita_income,
		percent_in_poverty = EXCLUDED.percent_in_poverty;


INSERT INTO prod.dim_location (location_key, city, state, zip_code)
SELECT location_key, city, state, zip_code
FROM staging.dim_location
	ON CONFLICT (location_key) DO NOTHING;	

					    
INSERT INTO prod.br_reason_group (reason_group_key, reason_key)
SELECT stg.reason_group_key, stg.reason_key 
FROM staging.br_reason_group stg
LEFT JOIN prod.br_reason_group prd 
	ON stg.reason_group_key = prd.reason_group_key 
	AND stg.reason_key = prd.reason_key
WHERE 
	prd.reason_group_key IS NULL;
					    
					    
INSERT INTO prod.fact_evictions 
	(eviction_key, district_key, neighborhood_key, location_key, reason_group_key, file_date_key, constraints_date_key, street_address)
SELECT eviction_key, district_key, neighborhood_key, location_key, reason_group_key, file_date_key, constraints_date_key, street_address
FROM staging.fact_evictions 
	ON CONFLICT (eviction_key) DO UPDATE SET 
		district_key = EXCLUDED.district_key,
		neighborhood_key = EXCLUDED.neighborhood_key,
		location_key = EXCLUDED.location_key,
		reason_group_key = EXCLUDED.reason_group_key,
		file_date_key = EXCLUDED.file_date_key,
		constraints_date_key = EXCLUDED.constraints_date_key,
		street_address = EXCLUDED.street_address;
