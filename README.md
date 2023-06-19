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
### mount a filter to generate an customer data with acceptance for research and have data of accelerometer
customer_trusted_to_curated.py
### create table customer_trusted from s3://...customer/trusted/
customer_curated.png
### create table accelerometer_trusted from s3://...accelerometer/trusted/

## thrird
### mount a filter to generate step trainer data from customer curated
trainer_trusted_to_curated.py
### create a table step_trainer_trusted

### mount a filter to generate step trainer trusted data join accelerometer trusted data
machine_learning_curated.py
### create a table machine_learning_curated


