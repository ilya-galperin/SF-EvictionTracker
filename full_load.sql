-- Populate Location Dimension

INSERT INTO staging.dim_Location (LocationKey, Neighborhood, SupervisorDistrict, City, State, ZipCode)
SELECT -1, 'Uknown', 'Uknown', 'Unknown', 'Uknown', 'Unknown';

INSERT INTO staging.dim_Location (Neighborhood, SupervisorDistrict, City, State, ZipCode)
SELECT DISTINCT
	COALESCE(neighborhood, 'Unknown') as Neighborhood,
	COALESCE(supervisor_district, 'Unknown') as SupervisorDistrict,
	COALESCE(city, 'Uknown') as City,
	COALESCE(state, 'Unknown') as State,
	COALESCE(zip, 'Unknown') as ZipCode
FROM raw.soda_evictions


-- Populate Reason Dimension

INSERT INTO staging.dim_Reason(ReasonKey, ReasonCode, ReasonDesc)
VALUES (-1, 'Unknown', 'Unknown');

INSERT INTO staging.dim_Reason(ReasonCode, ReasonDesc)
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

DROP TABLE IF EXISTS tmp_reason_group;

SELECT 
	ROW_NUMBER() OVER(ORDER BY concat_reason) as GroupKey,
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
		FROM raw.soda_Evictions
		) f1
	) f2;

INSERT INTO staging.br_Reason_Group(ReasonGroupKey, ReasonKey)
SELECT DISTINCT
	GroupKey,
	ReasonKey
FROM (SELECT GroupKey, unnest(reason_array) unnested FROM tmp_reason_group) grp
JOIN staging.dim_Reason r ON r.ReasonCode = grp.unnested;	


-- Populate Date Dimension Table

INSERT INTO staging.dim_Date (DateKey, Date, Year, Month, MonthName, Day, DayOfYear, WeekdayName, CalendarWeek, 
							FormattedDate, Quartal, YearQuartal, YearMonth, YearCalendarWeek, Weekend, USHoliday,
							Period, CWStart, CWEnd, MonthStart, MonthEnd)
SELECT -1, '1900-01-01', -1, -1, 'Unknown', -1, -1, 'Unknown', -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown',
		'Unknown', 'Unknown', 'Unknown', 'Unknown', '1900-01-01', '1900-01-01', '1900-01-01', '1900-01-01';
		

INSERT INTO staging.dim_Date (DateKey, Date, Year, Month, MonthName, Day, DayOfYear, WeekdayName, CalendarWeek, 
							FormattedDate, Quartal, YearQuartal, YearMonth, YearCalendarWeek, Weekend, USHoliday,
							Period, CWStart, CWEnd, MonthStart, MonthEnd)
SELECT
	TO_CHAR(datum, 'yyyymmdd')::INT as DateKey,
	datum as Date,
	EXTRACT(YEAR FROM datum) as Year,
	EXTRACT(MONTH FROM datum) as Month,
	TO_CHAR(datum, 'TMMonth') as MonthName,
	EXTRACT(DAY FROM datum) as Day,
	EXTRACT(doy FROM datum) as DayOfYear,
	TO_CHAR(datum, 'TMDay') as WeekdayName,
	EXTRACT(week FROM datum) as CalendarWeek,
	TO_CHAR(datum, 'dd. mm. yyyy') as FormattedDate,
	'Q' || TO_CHAR(datum, 'Q') as Quartal,
	TO_CHAR(datum, 'yyyy/"Q"Q') as YearQuartal,
	TO_CHAR(datum, 'yyyy/mm') as YearMonth,
	TO_CHAR(datum, 'iyyy/IW') as YearCalendarWeek,
	CASE WHEN EXTRACT(isodow FROM datum) IN (6, 7) THEN 'Weekend' ELSE 'Weekday' END as Weekend,
	CASE WHEN TO_CHAR(datum, 'MMDD') IN ('0101', '0704', '1225', '1226') THEN 'Holiday' ELSE 'No holiday' END
			as USHoliday,
	CASE WHEN TO_CHAR(datum, 'MMDD') BETWEEN '0701' AND '0831' THEN 'Summer break'
	     WHEN TO_CHAR(datum, 'MMDD') BETWEEN '1115' AND '1225' THEN 'Christmas season'
	     WHEN TO_CHAR(datum, 'MMDD') > '1225' OR TO_CHAR(datum, 'MMDD') <= '0106' THEN 'Winter break'
		 ELSE 'Normal' END
			as Period,
	datum + (1 - EXTRACT(isodow FROM datum))::INTEGER as CWStart,
	datum + (7 - EXTRACT(isodow FROM datum))::INTEGER as CWEnd,
	datum + (1 - EXTRACT(DAY FROM datum))::INTEGER as MonthStart,
	(datum + (1 - EXTRACT(DAY FROM datum))::INTEGER + '1 month'::INTERVAL)::DATE - '1 day'::INTERVAL as MonthEnd
FROM (
	SELECT '1997-01-01'::DATE + SEQUENCE.DAY as datum
	FROM generate_series(0,10956) as SEQUENCE(DAY)
	GROUP BY SEQUENCE.DAY
     ) DQ


-- Populate Evictions Fact Table

DROP TABLE IF EXISTS tmp_reason_facts;

SELECT 
	eviction_id,
	GroupKey as ReasonGroupKey
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
		FROM raw.soda_Evictions
		) grp
	) f_grp
JOIN tmp_reason_group t_grp ON f_grp.concat_reason = t_grp.concat_reason;	


INSERT INTO staging.fact_Evictions (EvictionKey, LocationKey, ReasonGroupKey, FileDateKey, ConstraintsDateKey, StreetAddress)
SELECT 
	f.eviction_id as EvictionKey,
	COALESCE(l.LocationKey, -1) as LocationKey,
	r.ReasonGroupKey as ReasonGroupKey,
	COALESCE(d1.DateKey, -1) as FileDateKey,
	COALESCE(d2.DateKey, -1) as ConstraintsDateKey,
	f.address as StreetAddress
FROM raw.soda_evictions f
LEFT JOIN tmp_reason_facts r on f.eviction_id = r.eviction_id
LEFT JOIN staging.dim_Location l 
	ON COALESCE(f.neighborhood, 'Unknown') = l.Neighborhood
	AND COALESCE(f.supervisor_district, 'Unknown') = l.SupervisorDistrict
	AND COALESCE(f.city, 'Unknown') = l.City
	AND COALESCE(f.state, 'Unknown') = l.State
	AND COALESCE(f.zip, 'Unknown') = l.ZipCode
LEFT JOIN staging.dim_Date d1 ON f.file_date = d1.Date
LEFT JOIN staging.dim_Date d2 ON f.constraints_date = d2.Date;
