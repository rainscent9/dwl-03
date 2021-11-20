# dwl-03
Eldorado Monitoring Tool

## installing Apache Airflow on Ubuntu 20.04 LTS
## 15.10.2021
## https://www.youtube.com/watch?v=aTaytcxy2Ck
####################################################

## install docker compose
sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose
docker-compose --version
####################################################

## install airflow in docker
mkdir docker-airflow
cd docker-airflow
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.2.2/docker-compose.yaml'
mkdir ./dags ./plugins ./logs
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

## initialize airflow instance with .yaml file
sudo docker-compose up airflow-init

## run airflow
sudo docker-compose up

## check if instances are healthy in new terminal
sudo docker ps
####################################################
