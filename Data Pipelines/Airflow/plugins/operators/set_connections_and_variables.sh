chmod 0700 /opt/airflow/start-services.sh
/opt/airflow/start-services.sh
chmod 0700 /opt/airflow/start.sh
/opt/airflow/start.sh
chmod +x /home/workspace/airflow/dags/cd0031-automate-data-pipelines/set_connections.sh
/home/workspace/airflow/dags/cd0031-automate-data-pipelines/set_connections.sh
airflow users create --email joelmagee@sbcglobal.net --firstname Joel --lastname Magee --password admin --role Admin --username admin
airflow scheduler