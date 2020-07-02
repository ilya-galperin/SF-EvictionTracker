-- echo "" > /home/airflow/airflow/dags/sql/trunc_target_tables.sql
-- nano /home/airflow/airflow/dags/sql/trunc_target_tables.sql
TRUNCATE TABLE raw.soda_evictions;
TRUNCATE TABLE raw.district_data;
TRUNCATE TABLE raw.neighborhood_data;
