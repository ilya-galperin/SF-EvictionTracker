DROP SCHEMA IF EXISTS raw CASCADE;
DROP SCHEMA IF EXISTS staging CASCADE;
DROP SCHEMA IF EXISTS prod CASCADE;

CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA prod;


-- Raw
CREATE UNLOGGED TABLE raw.soda_evictions (
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

CREATE UNLOGGED TABLE raw.district_data (
	district text,
	population text,
	households text,
	perc_asian text,
	perc_black text,
	perc_white text,
	perc_nat_am text,
	perc_nat_pac text,
	perc_other text,
	perc_latin text,
	median_age text,
	total_units text,
	perc_owner_occupied text,
	perc_renter_occupied text,
	median_rent_as_perc_of_income text,
	median_household_income text,
	median_family_income text,
	per_capita_income text,
	perc_in_poverty text
);

CREATE UNLOGGED TABLE raw.neighborhood_data (
	acs_name text,
	db_name text,
	population text,
	households text,
	perc_asian text,
	perc_black text,
	perc_white text,
	perc_nat_am text,
	perc_nat_pac text,
	perc_other text,
	perc_latin text,
	median_age text,
	total_units text,
	perc_owner_occupied text,
	perc_renter_occupied text,
	median_rent_as_perc_of_income text,
	median_household_income text,
	median_family_income text,
	per_capita_income text,
	perc_in_poverty text
);

-- Staging
CREATE TABLE staging.dim_Location (
	location_key serial PRIMARY KEY,
	city text,
	state text,
	zip_code text
);

CREATE TABLE staging.dim_District (
	district_key serial PRIMARY KEY,
	district text,
	population integer,
	households integer,
	percent_asian numeric,
	percent_black numeric,
	percent_white numeric,
	percent_native_am numeric,
	percent_pacific_isle numeric,
	percent_other_race numeric,
	percent_latin numeric,
	median_age numeric,
	total_units integer,
	percent_owner_occupied numeric,
	percent_renter_occupied numeric,
	median_rent_as_perc_of_income numeric,
	median_household_income numeric,
	median_family_income numeric,
	per_capita_income numeric,
	perc_in_poverty numeric
);

CREATE TABLE staging.dim_Neighborhood (
	neighborhood_key serial PRIMARY KEY,
	neighborhood text,
	neighborhood_alt_name text,
	population integer,
	households integer,
	percent_asian numeric,
	percent_black numeric,
	percent_white numeric,
	percent_native_am numeric,
	percent_pacific_isle numeric,
	percent_other_race numeric,
	percent_latin numeric,
	median_age numeric,
	total_units integer,
	percent_owner_occupied numeric,
	percent_renter_occupied numeric,
	median_rent_as_perc_of_income numeric,
	median_household_income numeric,
	median_family_income numeric,
	per_capita_income numeric,
	perc_in_poverty numeric
);

CREATE TABLE staging.dim_Reason (
	reason_key serial PRIMARY KEY,
	reason_code text,
	reason_desc text
);

CREATE TABLE staging.br_Reason_Group (
	reason_group_key int,
	reason_key int
);	
CREATE INDEX reason_group_key_idx ON staging.br_Reason_Group (reason_group_key);
CREATE INDEX reason_key_idx ON staging.br_Reason_Group (reason_key);

CREATE TABLE staging.dim_Date (
	date_key int PRIMARY KEY,
	date date,
	year int,
	month int,
	month_name text,
	day int,
	day_of_year int,
	weekday_name text,
	calendar_week int,
	formatted_date text,
	quartal text,
	year_quartal text,
	year_month text,
	year_calendar_week text,
	weekend text,
	us_holiday text,
	period text,
	cw_start date,
	cw_end date,
	month_start date,
	month_end date
);

CREATE TABLE staging.fact_Evictions (
	eviction_key text PRIMARY KEY,
	location_key int,
	district_key int,
	neighborhood_key int,
	reason_group_key int,
	file_date_key int,
	constraints_date_key int,
	street_address text
);


-- Prod
CREATE TABLE prod.dim_Location (
	location_key serial PRIMARY KEY,
	city text,
	state text,
	zip_code text
);

CREATE TABLE prod.dim_District (
	district_key serial PRIMARY KEY,
	district text,
	population integer,
	households integer,
	percent_asian numeric,
	percent_black numeric,
	percent_white numeric,
	percent_native_am numeric,
	percent_pacific_isle numeric,
	percent_other_race numeric,
	percent_latin numeric,
	median_age numeric,
	total_units integer,
	percent_owner_occupied numeric,
	percent_renter_occupied numeric,
	median_rent_as_perc_of_income numeric,
	median_household_income numeric,
	median_family_income numeric,
	per_capita_income numeric,
	perc_in_poverty numeric
);

CREATE TABLE prod.dim_Neighborhood (
	neighborhood_key serial PRIMARY KEY,
	neighborhood text,
	neighborhood_alt_name text,
	population integer,
	households integer,
	percent_asian numeric,
	percent_black numeric,
	percent_white numeric,
	percent_native_am numeric,
	percent_pacific_isle numeric,
	percent_other_race numeric,
	percent_latin numeric,
	median_age numeric,
	total_units integer,
	percent_owner_occupied numeric,
	percent_renter_occupied numeric,
	median_rent_as_perc_of_income numeric,
	median_household_income numeric,
	median_family_income numeric,
	per_capita_income numeric,
	perc_in_poverty numeric
);

CREATE TABLE prod.dim_Reason (
	reason_key serial PRIMARY KEY,
	reason_code text,
	reason_desc text
);

CREATE TABLE prod.br_Reason_Group (
	reason_group_key int,
	reason_key int
);	
CREATE INDEX reason_group_key_idx ON prod.br_Reason_Group (reason_group_key);
CREATE INDEX reason_key_idx ON prod.br_Reason_Group (reason_key);

CREATE TABLE prod.dim_Date (
	date_key int PRIMARY KEY,
	date date,
	year int,
	month int,
	month_name text,
	day int,
	day_of_year int,
	weekday_name text,
	calendar_week int,
	formatted_date text,
	quartal text,
	year_quartal text,
	year_month text,
	year_calendar_week text,
	weekend text,
	us_holiday text,
	period text,
	cw_start date,
	cw_end date,
	month_start date,
	month_end date
);

CREATE TABLE prod.fact_Evictions (
	eviction_key text PRIMARY KEY,
	location_key int,
	district_key int,
	neighborhood_key int,
	reason_group_key int,
	file_date_key int,
	constraints_date_key int,
	street_address text
);
