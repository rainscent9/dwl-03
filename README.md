# DWL-03 - Project Eldorado Monitoring Tool
Janine Wiedemar, Filipe Ribeiro de Oliveira, Simon Bolzli  
Fall semester 2021

## Summary
The result of this project is a monitoring tool called Eldorado Monitoring Tool, which collects data on the Internet about a specific company, as a demo case for Credit Suisse. As data sources we use NewsAPI, Twitter and Yahoo! Finance (replaced by Finnhub in the course of the project). The data is collected using Apache Airflow running on a physical Linux machine in Docker Compose. As a data store, we use Amazon RDS with PostgreSQL which also forms our data lake. The goal of the project is to monitor all news and mentions related to Credit Suisse and link them to stock price trends.

## Install Apache Airflow on Ubuntu 20.04 LTS
Apache Airflow manages our DAGs. Every API has its own DAG. Installing Apache Airflow requires Docker Compose, the instructions by Marc Lamberti (15/10/2021) explain that in great detail https://www.youtube.com/watch?v=aTaytcxy2Ck.

### Install docker
https://docs.docker.com/engine/install/ubuntu/

### Install docker compose
sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose  
sudo chmod +x /usr/local/bin/docker-compose  
sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose  
docker-compose --version  

### Install airflow in docker
mkdir docker-airflow  
cd docker-airflow  
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.2.2/docker-compose.yaml'  
mkdir ./dags ./plugins ./logs  
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env  
NOTE: add necessary keys for DAGs in .env file

## Run Apache Airflow
### initialize airflow instance with .yaml file  
sudo docker-compose up airflow-init  

### Run airflow
sudo docker-compose up  

### Check if instances are healthy in separate terminal
sudo docker ps  
