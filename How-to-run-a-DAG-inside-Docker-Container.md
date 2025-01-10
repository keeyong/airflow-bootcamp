## How to run a DAG inside Docker Container's command line

1. Open a terminal & move to the same folder where you cloned data226 repo

2. Change the current folder to week8/airflow
 - The sub-folders (“dags”) are in sync with the container
 - In Cloud Composer, Cloud Storage was used to sync

3. Look for airflow-worker's ID and get the first 3 characters ("7e2" in this example): 
```
docker ps  # look for airflow-worker

CONTAINER ID   IMAGE                  COMMAND                  CREATED        STATUS                  PORTS                    NAMES
7e26f0e7a183   apache/airflow:2.9.1   "/usr/bin/dumb-init …"   26 hours ago   Up 26 hours (healthy)   8080/tcp                 learn-airflow-airflow-worker-1
```

4. Log in the Airflow Worker's Docker Container 
```
docker exec -it XXX sh # log into the worker container
```

5. Run different Airflow commands
```
(airflow) airflow dags list
(airflow) airflow tasks list YfinanceToSnowflake 
(airflow) airflow dags test YfinanceToSnowflake
```
