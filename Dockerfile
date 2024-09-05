FROM apache/airflow:latest
USER root

ENV AIRFLOW_HOME=/opt/airflow/
ENV AIRFLOW__CORE__PLUGINS_FOLDER=/opt/airflow/plugins

RUN apt-get update -q && \
    apt-get install -yq procps

# 設置目錄權限
RUN chmod -R 777 /opt/airflow

USER airflow

RUN pip install --upgrade pip

# 安裝 Python 包
RUN pip install requests beautifulsoup4 pandas ipython spotipy opencc-python-reimplemented selenium
