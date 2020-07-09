# SF-EvictionTracker

Tracking district-level eviction rates and neighborhood socio-economic data for San Francisco in the months following COVID-19-related shelter-in-place orders. Data warehouse infrastructure is housed in the AWS ecosystem and uses Apache Airflow for orchestration with public-facing dashboards created using Metabase. 

Questions? Feel free to reach me at ilya.glprn@gmail.com.

Public Dashboard Link: http://sf-evictiontracker-metabase.us-east-1.elasticbeanstalk.com/public/dashboard/f637e470-8ea9-4b03-af80-53988e5b6a9b



<h3>ARCHITECTURE:</h3>

![Architecture](https://i.imgur.com/s2gLBZt.png)


<h3>DATA MODEL:</h3>

Dimension Tables:
`dim_district`
`dim_neighborhood`
`dim_location`
`dim_reason`
`dim_date`
`br_reason_group`

Fact Tables:
`fact_evictions`

The data model is implemented using a star schema with a bridge table to accomodate any new permutations for the reason dimension. More information on bridge tables can be found here: https://www.kimballgroup.com/2012/02/design-tip-142-building-bridges/


<h3>ETL FLOW:</h3>

General Overview - 
- Evictions data is collected from the SODA API and moved into an S3 Bucket
- Neighborhood/district census data is stored as a CSV in S3
- Once the API load to S3 is complete, data is moved into RDS into a "raw" schema and moves through a staging schema for processing
- ETL job execution is complete once data is moved from the staging schema into the final production tables

DAGs and Custom Airflow Operators -

![Ops](https://i.imgur.com/WTOUiGU.jpg)
![Dag](https://i.imgur.com/yJb3DKT.jpg)

There are 2 DAGs (Directed Acyclic Graphs) used for this project - <b>full load</b> which should be executed on initial setup and <b>incremental load</b> which is scheduled to run daily and pull new data from the Socrata Open Data API.

The incremental load DAG uses XCom to pass the filesize of the load between the API call task and a ShortCircuitOperator to skip downstream tasks if the API call produces no results. 

The DAGs use two customer operators. They have been purpose built for this project but are easily expandable to be used in other data pipelines.

1. soda_to_s3_operator: Queries the Socrata Open Data API using a SoQL string and uploads the results to an S3 bucket. Includes optional function to check source data size and abort ETL if filesize exceeds user-defined limit.

2. s3_to_postges_operator: Collects data from a file hosted on AWS S3 and loads it into a Postgres table. Current version supports JSON and CSV source data types.


<h3>INFRASTRUCTURE:</h3>

This project is hosted in the AWS ecosystem and uses the following resources:

EC2 -
- t2.medium - dedicated resource for Airflow, managed by AWS Instance Scheduler to complete the daily DAG run and shut off after execution 
- t2.small - used to host Metabase, always online

RDS -
- t2.small - hosts application database for Metabase and the data warehouse

Elastic Beanstalk is used to deploy the Metabase web application.


<h3>DASHBOARD:</h3>

The dashboard is publically accessible here: http://sf-evictiontracker-metabase.us-east-1.elasticbeanstalk.com/public/dashboard/f637e470-8ea9-4b03-af80-53988e5b6a9b

Some examples screengrabs below!

![Dash](https://i.imgur.com/3ghyDWw.jpg)

