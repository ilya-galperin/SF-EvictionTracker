DROP SCHEMA IF EXISTS raw CASCADE;
DROP SCHEMA IF EXISTS staging CASCADE;
DROP SCHEMA IF EXISTS prod CASCADE;

CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA prod;

CREATE UNLOGGED TABLE raw.soda_evictions(
	raw_id text,
	created_at timestamp,
	updated_at timestamp,
	eviction_id text,
	address text,
	city text,
	state text,
	zip text,
	file_date timestamp,
	non_payment boolean,
	breach boolean,
	nuisance boolean,
	illegal_use boolean,
	failure_to_sign_renewal boolean,
	access_denial boolean,
	unapproved_subtenant boolean,
	owner_move_in boolean,
	demolition boolean,
	capital_improvement boolean,
	substantial_rehab boolean,
	ellis_act_withdrawal boolean,
	condo_conversion boolean,
	roommate_same_unit boolean,
	other_cause boolean,
	late_payments boolean,
	lead_remediation boolean,
	development boolean,
	good_samaritan_ends boolean,
	constraints_date timestamp,
	supervisor_district text,
	neighborhood text
);

CREATE TABLE staging.dim_Location (
	LocationKey serial PRIMARY KEY,
	Neighborhood text,
	SupervisorDistrict text,
	City text,
	State text,
	ZipCode text
);

CREATE TABLE staging.dim_Reason (
	ReasonKey serial PRIMARY KEY,
	ReasonCode text,
	ReasonDesc text
);

CREATE TABLE staging.br_Reason_Group (
	ReasonGroupKey int,
	ReasonKey int
);	

CREATE TABLE staging.dim_Date (
	DateKey int PRIMARY KEY,
	Date date,
	Year int,
	Month int,
	MonthName text,
	Day int,
	DayOfYear int,
	WeekdayName text,
	CalendarWeek int,
	FormattedDate text,
	Quartal text,
	YearQuartal text,
	YearMonth text,
	YearCalendarWeek text,
	Weekend text,
	USHoliday text,
	Period text,
	CWStart date,
	CWEnd date,
	MonthStart date,
	MonthEnd date
);

CREATE TABLE staging.fact_Evictions (
	EvictionKey text PRIMARY KEY,
	LocationKey int,
	ReasonGroupKey int,
	FileDateKey int,
	ConstraintsDateKey int,
	StreetAddress text
);
