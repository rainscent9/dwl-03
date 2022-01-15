# Project Eldorado Monitoring Tool
HSLU MScIDS, Module Data Warehousing and Data Lake Systems   
Authors: Janine Wiedemar, Filipe Ribeiro de Oliveira, Simon Bolzli  
Fall semester 2021

## Summary
The result of this project is a monitoring tool called Eldorado Monitoring Tool, which collects data on the Internet about a specific company, as a demo case for Credit Suisse. As data sources we use NewsAPI, Twitter and Finnhub. The data is collected using Apache Airflow running on a virtual Linux machine in Docker Compose. As a data store, we use Amazon RDS with PostgreSQL which also forms our data lake. The goal of the project is to monitor all news and mentions related to Credit Suisse run a segment analysis and link them to stock price. This porject can easily be modified to gather data for other companies.

### Dashboard
The online Tableau Dashboard shows the collected data of the last 24h. 
![Dashboard](Tableau/img/dashboard.png?raw=true)

### Warnings
When the stock price falls below a certain threshold the user receives an email with a warning. This mechanism was automated with Tableau Online.
![Dashboard](Tableau/img/warning_email.png?raw=true)

## Install Apache Airflow on Ubuntu 20.04 LTS
Apache Airflow manages our DAGs. Every API has its own DAG. Installing Apache Airflow requires Docker Compose, the instructions by Marc Lamberti (15/10/2021) explain that in great detail https://www.youtube.com/watch?v=aTaytcxy2Ck.

### Install docker
https://docs.docker.com/engine/install/ubuntu/

### Install docker compose
```Shell
sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose  
sudo chmod +x /usr/local/bin/docker-compose  
sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose  
docker-compose --version  
```

### Install airflow in docker
Skip this part if you want to build this project.  
```Shell
mkdir docker-airflow  
cd docker-airflow  

## Latest version of Airflow can be found here:
## https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.2.2/docker-compose.yaml'

mkdir ./dags ./plugins ./logs  
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env  
```
NOTE: add necessary keys for DAGs in .env file

## Run Apache Airflow
### initialize airflow instance with .yaml file  
```Shell
sudo docker-compose up airflow-init  
```

### Run airflow
```Shell
sudo docker-compose up  
```

### Check if instances are healthy in separate terminal
```Shell
sudo docker ps  
```

## Trouble shooting
### Airflow is not running anymore
Delete all containers and restart.  
```Shell
docker-compose down --volumes --rmi all  
docker-compose up
```

### Check memory usage
If the machine runs into memory excepions (error 28) it can't install docker containers propperly and will try forever and ever and ever, not stopping.  
First remove unused files and then enter "docker-compose up" again.
