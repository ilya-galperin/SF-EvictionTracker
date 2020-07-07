# SF-EvictionTracker

Tracking district-level eviction rates and neighborhood socio-economic data for San Francisco in the months following COVID-19-related shelter-in-place orders. Data warehouse infrastructure is housed in the AWS ecosystem and uses Apache Airflow for orchestration with public-facing dashboards created using Metabase. Please feel free to reach out with any questions.

Public Dashboard Link: http://sf-evictiontracker-metabase.us-east-1.elasticbeanstalk.com/public/dashboard/f637e470-8ea9-4b03-af80-53988e5b6a9b


<h3>ARCHITECTURE:</h3>

[IMG]


<h3>DATA MODEL:</h3>

`Dimension Tables -`
`
dim_district
dim_neighborhood
dim_location
dim_reason
dim_date
`
Bridge Tables - 

br_reason_group

Fact Tables -

fact_evictions

The data model is implemented using a star schema with a bridge table to accomodate any new permutations for the reason dimension. More information on bridge tables can be found here: https://www.kimballgroup.com/2012/02/design-tip-142-building-bridges/


<h3>ETL FLOW:</h3>

General Overview - 
- Evictions data is collected from the SODA API and moved into an S3 Bucket
- Neighborhood/district census data is stored as a CSV in S3
- Once the API load to S3 is complete, data is moved into RDS into a "raw" schema and moves through a staging schema for processing
- ETL job execution is complete once data is moved from the staging schema into the final production tables

DAGs and Customer Airflow Operators -

[DAG Image]

There are 2 DAGs (Directed Acyclic Graph) used for this project - <b>full load</b> which is used for the initialize setup and <b>incremental load</b> which is scheduled to run daily and pull new data from the Socrata Open Data API.

The DAGs use two customer operators. They have been purpose built for this project but are easily expandable to be used in other data pipelines.

1. soda_to_s3_operator:

2. s3_to_postges_operator:


<h3>INFRASTRUCTURE/ENVIRONMENT:</h3>

This project is hosted in the AWS ecosystem and uses the following resources:

EC2 -
- t2.medium - dedicated resource for Airflow, managed by AWS Instant Scheduler to complete the daily DAG run and shut off after execution 
- t2.small - used to host Metabase, always online

RDS -
- t2.small - hosts application database for Metabase and the data warehouse

Elastic Beanstalk is used to deploy the Metabase web application.


<h3>METABASE DASHBOARDS:</h3>

The dashboards are publically accessible here: http://sf-evictiontracker-metabase.us-east-1.elasticbeanstalk.com/public/dashboard/f637e470-8ea9-4b03-af80-53988e5b6a9b

Some examples screengrabs below!
