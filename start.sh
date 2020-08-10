kill -9 $(lsof -i:8080 -t) 2> /dev/null
export AIRFLOW_HOME=$PWD/airflow
echo $AIRFLOW_HOME
pipenv install apache-airflow==1.10.11
pipenv install docker
cp ./airflow.cfg ./airflow
pipenv run airflow initdb
pipenv run airflow scheduler
