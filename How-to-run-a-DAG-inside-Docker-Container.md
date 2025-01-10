## Docker Container 안에서 동작하는 Airflow 커맨드 라인에서 DAG 실행 방법

1. Airflow가 docker로 실행 중임을 먼저 확인
2. 먼저 터미널을 실행
3. docker ps 명령을 실행
4. 위의 명령 결과에서 airflow-beginner-airflow-1의 ID를 찾아 처음 3글자만 기억 (아래의 예에서는 "7e2"): 
```
docker ps
```
```
CONTAINER ID   IMAGE                  COMMAND                  CREATED          STATUS                    PORTS                    NAMES
b9ab5916f962   apache/airflow:2.9.2   "/usr/bin/dumb-init …"   6 minutes ago    Up 5 minutes (healthy)    0.0.0.0:8081->8080/tcp   airflow-beginner-airflow-1
7e2c1f568ba3   postgres:13            "docker-entrypoint.s…"   10 minutes ago   Up 10 minutes (healthy)   5432/tcp                 airflow-beginner-postgres-1
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
