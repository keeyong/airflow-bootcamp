from jinja2 import Environment, FileSystemLoader
import os
import yaml

# 현재 파일(__file__)의 절대 경로를 기준으로, 해당 파일이 위치한 디렉토리 파악
file_dir = os.path.dirname(os.path.abspath(__file__))
print(file_dir)    # generator.py가 위치한 폴더를 출력함

# jinja2 템플릿 엔진 환경을 설정합니다 (Environment)
# file_dir이 가르키는 위치에서 템플릿 파일을 찾도록 FileSystemLoader를 설정
# env 객체를 통해 템플릿 관련 작업을 수행 가능
env = Environment(loader=FileSystemLoader(file_dir))

# templated_dag.jinja2 템플릿 파일 로딩
# 뒤에서 template.render로 템플릿의 내용을 채우게 됨
template = env.get_template('templated_dag.jinja2')

# file_dir 내에 있는 모든 파일을 하나씩 읽어서 루프
for f in os.listdir(file_dir):
    # 파일명이 ".yml"로 끝나는 파일만 처리
    if f.endswith(".yml"):
        # yml 파일을 열고 YAML 형식의 설정을 파이썬 객체로 로드
        with open(f"{file_dir}/{f}", "r") as cf:
            config = yaml.safe_load(cf)
            
            # 로드된 config 딕셔너리에서 'dag_id' 키를 사용해
            # get_price_{dag_id}.py라는 이름의 파일 생성
            with open(f"dags/get_price_{config['dag_id']}.py", "w") as f:
                # 템플릿 파일(templated_dag.jinja2)을 render()하며 config를 전달하고,
                # 그 결과 문자열을 새로 만든 파이썬 파일에 작성합니다.
                f.write(template.render(config))
