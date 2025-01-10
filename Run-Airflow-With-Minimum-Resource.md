## LocalExecutor 사용 Airflow를 Docker Container로 실행하기

이 방식은 단 2개의 컨테이너만 필요로 하기에 메모리 등의 자원을 훨씬 더 필요로 한다. 

1. Docker Desktop이 실행 중임을 먼저 확인한다.
2. 다음에 터미널을 연다 (윈도우라면 CMD 혹은 PowerShell)
3. 적당한 폴더로 이동한다.
4. 다음으로 강의 GitHub Repo를 클론
``` 
git clone https://github.com/keeyong/airflow-beginner.git
```
git이 설치되어 있지 않다면 설치 후 실행하던지 [압축파일](https://github.com/keeyong/airflow-beginner/archive/refs/heads/main.zip)을 다운로드 받고 압축을 풀 것
5. 다음으로 방금 다운로드받은 GitHub repo 폴더 안으로 이동
```
cd airflow-beginner
```
6. 다음으로 Airflow 환경을 초기화
```
docker compose -f docker-compose.yaml up airflow-init
```
7. 다음으로 Airflow 서비스를 실행
```
docker compose -f docker-compose.yaml up
```
8. 조금 기다린 후에 [http://localhost:8081](http://localhost:8081)를 방문. ID:PW로 airflow:airflow를 입력

