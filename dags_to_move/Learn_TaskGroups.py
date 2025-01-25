from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

import pendulum

with DAG(
    dag_id="Learn_Task_Group",
    start_date=pendulum.today('UTC').add(days=-2),
    catchup=False
) as dag:

    start = EmptyOperator(task_id="start")

    # Task Group #1
    with TaskGroup("Download", tooltip="Tasks for downloading data") as section_1:
        task_1 = EmptyOperator(task_id="task_1")
        task_2 = BashOperator(task_id="task_2", bash_command='echo 1')
        task_3 = EmptyOperator(task_id="task_3")

        task_1 >> [task_2, task_3]

    # Task Group #2
    with TaskGroup("Process", tooltip="Tasks for processing data") as section_2:
        task_1 = EmptyOperator(task_id="task_1")

        with TaskGroup("inner_section_2", tooltip="Tasks for inner_section2") as inner_section_2:
            task_2 = BashOperator(task_id="task_2", bash_command='echo 1')
            task_3 = EmptyOperator(task_id="task_3")
            task_4 = EmptyOperator(task_id="task_4")

            [task_2, task_3] >> task_4

    end = EmptyOperator(task_id='end')

    start >> section_1 >> section_2 >> end
