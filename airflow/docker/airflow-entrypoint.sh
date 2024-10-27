airflow db migrate
# Create the admin user
airflow users create \
    -r Admin \
    -u $AIRFLOW_USERNAME \
    -f $AIRFLOW_FIRSTNAME \
    -l $AIRFLOW_LASTNAME  \
    -p $AIRFLOW_PASSWORD \
    -e $AIRFLOW_EMAIL
# Start the webserver in the background
airflow webserver &
# Start the scheduler in the background
airflow scheduler &
# Wait for all background processes to finish
wait
