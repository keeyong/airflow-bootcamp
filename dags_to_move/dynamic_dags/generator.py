from jinja2 import Environment, FileSystemLoader
import yaml
import os

file_dir = os.path.dirname(os.path.abspath(__file__))
env = Environment(loader=FileSystemLoader(file_dir))
template = env.get_template('templated_dag.jinja2')

for f in os.listdir(file_dir):
    if f.endswith(".yml"):
        with open(f"{file_dir}/{f}", "r") as cf:
            config = yaml.safe_load(cf)
            with open(f"dags/get_price_{config['dag_id']}.py", "w") as f:
                f.write(template.render(config))
