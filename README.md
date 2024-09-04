# tir102-project
after attach to container

1.in terminal : 
airflow users create --username admin --firstname Shopping --lastname Yuan --role Admin --email shopping0789604@gmail.com

with password 0000

2.then run:

airflow scheduler

3.Now you can trigger dags likes :
airflow dags trigger d_07_example_data_pipeline
or in airflow web UI : localhost:8080
