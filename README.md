# STEDI
Using AWS Glue, AWS S3, Python, and Spark, generate Python scripts to build a lakehouse solution in AWS

## first
### Import customer, accelerometer and step trainer data to ATHENA
accelerometer_landing.sql
customer_landing.sql
#### result:
accelerometer_landing.png
customer_landing.png

## second
### mount a filter to generate customer data with acceptance for research use 
customer_landing_to_trusted.py
### mount a filter to generate accelerometer data with acceptance for research use 
accelerometer_landing_to_trusted_zone.py
### create table customer_trusted from s3://...customer/trusted/
### create table accelerometer_trusted from s3://...accelerometer/trusted/

## thrird



