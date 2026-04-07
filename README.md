# 5114-progress-report

**acquisition folder** contains code for AWS Lambda that collects real time updates and static schedule updates. Triggered using Eventbridge rules.


**spark folder** 
- spark_load_rt.py decodes realtime protobuf data from S3 and writes them to raw tables in Snowflake. 
- spark_load_static.py writes static schedule updates from S3 to dimension tables in Snowflake, if an update is available.
- Configurations and packages work for Spark 3.5.0 

**sql folder** contains SQL ran in Snowflake for creating all tables and deriving mart tables and fact tables from raw tables.
