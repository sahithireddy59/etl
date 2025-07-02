FROM apache/airflow:2.8.2
USER root
COPY requirements.txt /
RUN chown airflow: /requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt && pip install django>=3.2 