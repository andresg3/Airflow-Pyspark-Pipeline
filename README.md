# Airflow-Pyspark-Pipeline

A simple data warehouse infrastructure with an ETL pipeline running inside docker with Apache Airflow for data orchestration, AWS Redshift for cloud data warehouse. The data files are dropped every morning on a local directory and moved to 'Landing Bucket' on AWS S3. "Transform" jobs are written in Pyspark. Used 'DockerOperator' from Airflow to spin up a Pyspark instance(jupyter/pyspark-notebook). Jobs are scheduled to run every morning.

### Architecture
![alt text](https://github.com/andresg3/Airflow-Pyspark-Pipeline/blob/master/images/airflow_pyspark_pipeline%20(2).png)

### Setup
- Clone this repo
- Install pipenv
- Run in terminal of cloned repo: 
  - sh start.sh
- Open a new terminal tab and run:
  - sh start_airflow.sh
- In your browser: http://localhost:8080/admin/

### ETL Flow
- Data files are collected from a local folder and uploaded to S3 Landing Bucket
- An operator will the copy the data files from Landing to Working Zone
- Once data is copied to Working zone a Spark job is triggered, it reads data and applies transformations. Then data is re-partitioned and moved to Processed Zone
- Warehouse Operator reads data from Processed Zone and loads it into Redshift Staging Tables. From there data is loaded to Warehouse Tables via Upsert statements.
- Another custom Operator then loads the Analytics Tables that can be queried to show Popular Books, authors, ratings... etc

### DAG view
img here






### Notes
In the future I would try to spin up a small EMR Cluster on AWS and run the processing jobs there using a larger dataset.
