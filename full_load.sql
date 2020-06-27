-- Populate Location Dimension

INSERT INTO staging.dim_Location (location_key, city, state, zip_code)
SELECT -1, 'Unknown', 'Uknown', 'Unknown';

INSERT INTO staging.dim_Location (city, state, zip_code)
SELECT DISTINCT
	COALESCE(city, 'Uknown') as city,
	COALESCE(state, 'Unknown') as state,
	COALESCE(zip, 'Unknown') as zip_code
FROM raw.soda_evictions;


-- Populate Reason Dimension

INSERT INTO staging.dim_Reason (reason_key, reason_code, reason_desc)
VALUES (-1, 'Unknown', 'Unknown');

INSERT INTO staging.dim_Reason (reason_code, reason_desc)
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

INSERT INTO staging.br_Reason_Group (reason_group_key, reason_key)
SELECT DISTINCT
	group_key as reason_group_key,
	reason_key
FROM (SELECT group_key, unnest(reason_array) unnested FROM tmp_reason_group) grp
JOIN staging.dim_Reason r ON r.reason_code = grp.unnested;	


-- Populate Date Dimension Table

INSERT INTO staging.dim_Date (date_key, date, year, month, month_name, day, day_of_year, weekday_name, calendar_week, 
							formatted_date, quartal, year_quartal, year_month, year_calendar_week, weekend, us_holiday,
							period, cw_start, cw_end, month_start, month_end)
SELECT -1, '1900-01-01', -1, -1, 'Unknown', -1, -1, 'Unknown', -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown',
		'Unknown', 'Unknown', 'Unknown', 'Unknown', '1900-01-01', '1900-01-01', '1900-01-01', '1900-01-01';
		

INSERT INTO staging.dim_Date (date_key, date, year, month, month_name, day, day_of_year, weekday_name, calendar_week, 
							formatted_date, quartal, year_quartal, year_month, year_calendar_week, weekend, us_holiday,
							period, cw_start, cw_end, month_start, month_end)
SELECT
	TO_CHAR(datum, 'yyyymmdd')::INT as date_key,
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
	datum + (1 - EXTRACT(isodow FROM datum))::INTEGER as cw_start,
	datum + (7 - EXTRACT(isodow FROM datum))::INTEGER as cw_end,
	datum + (1 - EXTRACT(DAY FROM datum))::INTEGER as month_start,
	(datum + (1 - EXTRACT(DAY FROM datum))::INTEGER + '1 month'::INTERVAL)::DATE - '1 day'::INTERVAL as month_end
FROM (
	SELECT '1997-01-01'::DATE + SEQUENCE.DAY as datum
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


INSERT INTO staging.fact_Evictions (eviction_key, location_key, reason_group_key, file_date_key, constraints_date_key, street_address)
SELECT 
	f.eviction_id as eviction_key,
	COALESCE(l.location_key, -1) as location_key,
	r.reason_group_key as reason_group_key,
	COALESCE(d1.date_key, -1) as file_date_key,
	COALESCE(d2.date_key, -1) as constraints_date_key,
	f.address as street_address
FROM raw.soda_evictions f
LEFT JOIN tmp_reason_facts r ON f.eviction_id = r.eviction_id
LEFT JOIN staging.dim_Location l 
	ON COALESCE(f.city, 'Unknown') = l.city
	AND COALESCE(f.state, 'Unknown') = l.state
	AND COALESCE(f.zip, 'Unknown') = l.zip_code
LEFT JOIN staging.dim_Date d1 ON f.file_date = d1.date
LEFT JOIN staging.dim_Date d2 ON f.constraints_date = d2.date;

DROP TABLE tmp_reason_group;
DROP TABLE tmp_reason_facts;

		     
-- Migrate to Production Schema

INSERT INTO prod.dim_Location (location_key, city, state, zip_code)
SELECT location_key, city, state, zip_code
FROM staging.dim_Location;

INSERT INTO prod.dim_Reason (reason_key, reason_code, reason_desc)
SELECT reason_key, reason_code, reason_desc
FROM staging.dim_Reason;

INSERT INTO prod.br_Reason_Group (reason_group_key, reason_key)
SELECT reason_group_key, reason_key
FROM staging.br_Reason_Group;

INSERT INTO prod.dim_Date 
		(date_key, date, year, month, month_name, day, day_of_year, weekday_name, calendar_week, 
		formatted_date, quartal, year_quartal, year_month, year_calendar_week, weekend, us_holiday,
		period, cw_start, cw_end, month_start, month_end)
SELECT 
		date_key, date, year, month, month_name, day, day_of_year, weekday_name, calendar_week, 
		formatted_date, quartal, year_quartal, year_month, year_calendar_week, weekend, us_holiday, period, 
		cw_start, cw_end, month_start, month_end
FROM staging.dim_Date;

INSERT INTO prod.fact_Evictions (eviction_key, location_key, reason_group_key, file_date_key, constraints_date_key, street_address)
SELECT eviction_key, location_key, reason_group_key, file_date_key, constraints_date_key, street_address
FROM staging.fact_Evictions;	
