# tir102-project

1.sudo docker exec -it -u root <container_id> /bin/bash

2.in container with account airflow : 
sudo chmod -R 777 /opt/airflow

3.sudo docker exec -it <container_id> /bin/bash

4.in container with account airflow : 
airflow users create --username <> --firstname <> --lastname <> --role <> --email <>
<EX>
airflow users create --username admin --firstname Shopping --lastname Yuan --role Admin --email shopping0789604@gmail.com
with password 0000



<!-- if start airflow on multiple container run:
nohup airflow scheduler > /opt/airflow/airflow-scheduler.log 2>&1 & -->

5.Now you can trigger dags likes :
airflow dags trigger d_07_example_data_pipeline
or in airflow web UI : localhost:8080

