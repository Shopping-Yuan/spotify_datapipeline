# tir102-project
after attach to container
in terminal : 
airflow users create --username admin --firstname Shopping --lastname Yuan --role Admin --email shopping0789604@gmail.com

with password 0000

then run:

airflow scheduler

Now you can trigger dags likes :
airflow dags trigger d_07_example_data_pipeline

<!-- 
      _AIRFLOW_WWW_USER_CREATE: 'true'
      _AIRFLOW_WWW_USER_USERNAME: ${_AIRFLOW_WWW_USER_USERNAME:-airflow}
      _AIRFLOW_WWW_USER_PASSWORD: ${_AIRFLOW_WWW_USER_PASSWORD:-airflow}
      _PIP_ADDITIONAL_REQUIREMENTS: ''
    user: "0:0"
    volumes:
      - .:/sources -->
