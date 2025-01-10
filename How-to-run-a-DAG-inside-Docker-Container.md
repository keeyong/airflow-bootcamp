## Docker Container 안에서 동작하는 Airflow 커맨드 라인에서 DAG 실행 방법

1. Airflow가 docker로 실행 중임을 먼저 확인
2. 먼저 터미널을 실행
3. docker ps 명령을 실행
4. 위의 명령 결과에서 airflow-worker의 ID를 찾아 처음 3글자만 기억 (아래의 예에서는 "7e2"): 
```
docker ps

CONTAINER ID   IMAGE                  COMMAND                  CREATED        STATUS                  PORTS                    NAMES
7e26f0e7a183   apache/airflow:2.9.1   "/usr/bin/dumb-init …"   26 hours ago   Up 26 hours (healthy)   8080/tcp                 learn-airflow-airflow-worker-1
```

5. 아래 명령을 실행해서 Airflow Worker Docker Container안으로 로그인 
```
docker exec -it 7e2 sh
```

6. 여기서 다양한 Airflow 명령을 실행해보기
```
(airflow) airflow dags list
(airflow) airflow tasks list YfinanceToSnowflake 
(airflow) airflow dags test YfinanceToSnowflake
```
