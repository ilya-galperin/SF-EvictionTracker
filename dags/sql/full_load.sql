# echo "" > /home/airflow/airflow/dags/sql/full_load.sql
# nano /home/airflow/airflow/dags/sql/full_load.sql

-- Populate District Dimension

INSERT INTO staging.dim_district (district_key, district)
SELECT -1, 'Unknown';

INSERT INTO staging.dim_district (district, population, households, percent_asian, percent_black, percent_white, percent_native_am,
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
FROM raw.district_data;	


-- Populate Neighborhood Dimension

INSERT INTO staging.dim_neighborhood (neighborhood_key, neighborhood)
SELECT -1, 'Unknown';

INSERT INTO staging.dim_neighborhood (neighborhood, neighborhood_alt_name, population, households, percent_asian, percent_black, percent_white, 
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
FROM raw.neighborhood_data;	


-- Populate Location Dimension

INSERT INTO staging.dim_location (location_key, city, state, zip_code)
SELECT -1, 'Unknown', 'Unknown', 'Unknown';

INSERT INTO staging.dim_location (city, state, zip_code)
SELECT DISTINCT
	COALESCE(city, 'Unknown') as city,
	COALESCE(state, 'Unknown') as state,
	COALESCE(zip, 'Unknown') as zip_code
FROM raw.soda_evictions
WHERE 
 city IS NOT NULL OR state IS NOT NULL OR zip IS NOT NULL;


-- Populate Reason Dimension

INSERT INTO staging.dim_reason (reason_key, reason_code, reason_desc)
VALUES (-1, 'Unknown', 'Unknown');

INSERT INTO staging.dim_reason (reason_code, reason_desc)
VALUES 	('non_payment', 'Non-Payment'),
	('breach', 'Breach'),
	('nuisance', 'Nuisance'),
	('illegal_use', 'Illegal Use'),
	('failure_to_sign_renewal', 'Failure to Sign Renewal'),
	('access_denial', 'Access Denial'),
	('unapproved_subtenant', 'Unapproved Subtenant'),
	('owner_move_in', 'Owner Move-In'),
	('demolition', 'Demolition'),
	('capital_improvement', 'Capital Improvement'),
	('substantial_rehab', 'Substantial Rehab'),
	('ellis_act_withdrawal', 'Ellis Act Withdrawal'),
	('condo_conversion', 'Condo Conversion'),
	('roommate_same_unit', 'Roommate Same Unit'),
	('other_cause', 'Other Cause'),
	('late_payments', 'Late Payments'),
	('lead_remediation', 'Lead Remediation'),
	('development', 'Development'),
	('good_samaritan_ends', 'Good Samaritan Ends');

	
-- Populate Reason Bridge Table

SELECT 
	ROW_NUMBER() OVER(ORDER BY concat_reason) as group_key,
	string_to_array(concat_reason, '|') as reason_array,
	concat_reason
INTO TEMP tmp_reason_group
FROM (
	SELECT DISTINCT
		TRIM(TRAILING '|' FROM (CASE WHEN concat_reason = '' THEN 'Unknown' ELSE concat_reason END)) as concat_reason
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
		) f1
	) f2;

INSERT INTO staging.br_reason_group (reason_group_key, reason_key)
SELECT DISTINCT
	group_key as reason_group_key,
	reason_key
FROM (SELECT group_key, unnest(reason_array) unnested FROM tmp_reason_group) grp
JOIN staging.dim_Reason r ON r.reason_code = grp.unnested;	


-- Populate Date Dimension Table

INSERT INTO staging.dim_date (date_key, date, year, month, month_name, day, day_of_year, weekday_name, calendar_week, 
				formatted_date, quartal, year_quartal, year_month, year_calendar_week, weekend, us_holiday,
				period, cw_start, cw_end, month_start, month_end)
SELECT -1, '1900-01-01', -1, -1, 'Unknown', -1, -1, 'Unknown', -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown',
		'Unknown', 'Unknown', 'Unknown', 'Unknown', '1900-01-01', '1900-01-01', '1900-01-01', '1900-01-01';
		

INSERT INTO staging.dim_date (date_key, date, year, month, month_name, day, day_of_year, weekday_name, calendar_week, 
				formatted_date, quartal, year_quartal, year_month, year_calendar_week, weekend, us_holiday,
				period, cw_start, cw_end, month_start, month_end)
SELECT
	TO_CHAR(datum, 'yyyymmdd')::int as date_key,
	datum as date,
	EXTRACT(YEAR FROM datum) as year,
	EXTRACT(MONTH FROM datum) as month,
	TO_CHAR(datum, 'TMMonth') as month_name,
	EXTRACT(DAY FROM datum) as day,
	EXTRACT(doy FROM datum) as day_of_year,
	TO_CHAR(datum, 'TMDay') as weekday_name,
	EXTRACT(week FROM datum) as calendar_week,
	TO_CHAR(datum, 'dd. mm. yyyy') as formatted_date,
	'Q' || TO_CHAR(datum, 'Q') as quartal,
	TO_CHAR(datum, 'yyyy/"Q"Q') as year_quartal,
	TO_CHAR(datum, 'yyyy/mm') as year_month,
	TO_CHAR(datum, 'iyyy/IW') as year_calendar_week,
	CASE WHEN EXTRACT(isodow FROM datum) IN (6, 7) THEN 'Weekend' ELSE 'Weekday' END as weekend,
	CASE WHEN TO_CHAR(datum, 'MMDD') IN ('0101', '0704', '1225', '1226') THEN 'Holiday' ELSE 'No holiday' END
			as us_holiday,
	CASE WHEN TO_CHAR(datum, 'MMDD') BETWEEN '0701' AND '0831' THEN 'Summer break'
	     WHEN TO_CHAR(datum, 'MMDD') BETWEEN '1115' AND '1225' THEN 'Christmas season'
	     WHEN TO_CHAR(datum, 'MMDD') > '1225' OR TO_CHAR(datum, 'MMDD') <= '0106' THEN 'Winter break'
		 ELSE 'Normal' END
			as period,
	datum + (1 - EXTRACT(isodow FROM datum))::int as cw_start,
	datum + (7 - EXTRACT(isodow FROM datum))::int as cw_end,
	datum + (1 - EXTRACT(DAY FROM datum))::int as month_start,
	(datum + (1 - EXTRACT(DAY FROM datum))::int + '1 month'::interval)::date - '1 day'::interval as month_end
FROM (
	SELECT '1997-01-01'::date + SEQUENCE.DAY as datum
	FROM generate_series(0,10956) as SEQUENCE(DAY)
	GROUP BY SEQUENCE.DAY
     ) DQ;


-- Populate Evictions Fact Table

SELECT 
	eviction_id,
	group_key as reason_group_key
INTO tmp_reason_facts
FROM (
	SELECT 
		eviction_id,
		TRIM(TRAILING '|' FROM (CASE WHEN concat_reason = '' THEN 'Unknown' ELSE concat_reason END)) as concat_reason
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
		) grp
	) f_grp
JOIN tmp_reason_group t_grp ON f_grp.concat_reason = t_grp.concat_reason;	


INSERT INTO staging.fact_evictions (eviction_key, district_key, neighborhood_key, location_key, reason_group_key, file_date_key, 
									constraints_date_key, street_address)
SELECT 
	f.eviction_id as eviction_key,
	COALESCE(d.district_key, -1) as district_key,
	COALESCE(n.neighborhood_key, -1) as neighborhood_key,
	COALESCE(l.location_key, -1) as location_key,
	reason_group_key,
	COALESCE(dt1.date_key, -1) as file_date_key,
	COALESCE(dt2.date_key, -1) as constraints_date_key,
	f.address as street_address
FROM raw.soda_evictions f
LEFT JOIN tmp_reason_facts r ON f.eviction_id = r.eviction_id
LEFT JOIN staging.dim_district d ON f.supervisor_district = d.district
LEFT JOIN staging.dim_neighborhood n ON f.neighborhood = n.neighborhood_alt_name
LEFT JOIN staging.dim_location l 
	ON COALESCE(f.city, 'Unknown') = l.city
	AND COALESCE(f.state, 'Unknown') = l.state
	AND COALESCE(f.zip, 'Unknown') = l.zip_code
LEFT JOIN staging.dim_date dt1 ON f.file_date = dt1.date
LEFT JOIN staging.dim_date dt2 ON f.constraints_date = dt2.date;

DROP TABLE tmp_reason_group;
DROP TABLE tmp_reason_facts;

		     
-- Migrate to Production Schema

INSERT INTO prod.dim_district 
	(district, population, households, percent_asian, percent_black, percent_white, percent_native_am,
	percent_pacific_isle, percent_other_race, percent_latin, median_age, total_units, 
	percent_owner_occupied, percent_renter_occupied, median_rent_as_perc_of_income, median_household_income, 
	median_family_income, per_capita_income, percent_in_poverty)
SELECT 
	district, population, households, percent_asian, percent_black, percent_white, percent_native_am,
	percent_pacific_isle, percent_other_race, percent_latin, median_age, total_units, 
	percent_owner_occupied, percent_renter_occupied, median_rent_as_perc_of_income, median_household_income, 
	median_family_income, per_capita_income, percent_in_poverty
FROM staging.dim_district;

INSERT INTO prod.dim_neighborhood
	(neighborhood, neighborhood_alt_name, population, households, percent_asian, percent_black, percent_white, 
	percent_native_am, percent_pacific_isle, percent_other_race, percent_latin, median_age, total_units, 
	percent_owner_occupied, percent_renter_occupied, median_rent_as_perc_of_income, median_household_income, 
	median_family_income, per_capita_income, percent_in_poverty)
SELECT
	neighborhood, neighborhood_alt_name, population, households, percent_asian, percent_black, percent_white, 
	percent_native_am, percent_pacific_isle, percent_other_race, percent_latin, median_age, total_units, 
	percent_owner_occupied, percent_renter_occupied, median_rent_as_perc_of_income, median_household_income, 
	median_family_income, per_capita_income, percent_in_poverty
FROM staging.dim_neighborhood;

INSERT INTO prod.dim_location (location_key, city, state, zip_code)
SELECT location_key, city, state, zip_code
FROM staging.dim_location;

INSERT INTO prod.dim_reason (reason_key, reason_code, reason_desc)
SELECT reason_key, reason_code, reason_desc
FROM staging.dim_reason;

INSERT INTO prod.br_reason_group (reason_group_key, reason_key)
SELECT reason_group_key, reason_key
FROM staging.br_reason_group;

INSERT INTO prod.dim_date 
	(date_key, date, year, month, month_name, day, day_of_year, weekday_name, calendar_week, 
	formatted_date, quartal, year_quartal, year_month, year_calendar_week, weekend, us_holiday,
	period, cw_start, cw_end, month_start, month_end)
SELECT 
	date_key, date, year, month, month_name, day, day_of_year, weekday_name, calendar_week, 
	formatted_date, quartal, year_quartal, year_month, year_calendar_week, weekend, us_holiday, period, 
	cw_start, cw_end, month_start, month_end
FROM staging.dim_date;

INSERT INTO prod.fact_evictions 
	(eviction_key, district_key, neighborhood_key, location_key, reason_group_key, 
	file_date_key, constraints_date_key, street_address)
SELECT 
	eviction_key, district_key, neighborhood_key, location_key, reason_group_key, 
	file_date_key, constraints_date_key, street_address
FROM staging.fact_evictions;
