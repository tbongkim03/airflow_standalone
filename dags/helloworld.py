from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
with DAG(
    'helloworld',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='hello world DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['helloworld'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    extract = BashOperator(
        task_id='extract',
        bash_command='date',
    )

    where = BashOperator(
        task_id='where',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )

    tmp = DummyOperator(task_id='tmp')
    water = DummyOperator(task_id='water')
    pm = DummyOperator(task_id='pm')
    hpm = DummyOperator(task_id='hpm')
    voc = DummyOperator(task_id='voc')
    tmpgraph = DummyOperator(task_id='tmpgraph')
    feeltmp = DummyOperator(task_id='feeltmp')
    pmgrade = DummyOperator(task_id='pmgrade')
    combine = DummyOperator(task_id='combine')
    task_start = DummyOperator(task_id='task_start')
    task_end = DummyOperator(task_id='task_end')

    task_start >> extract >> combine
    extract >> where >> combine
    extract >> tmp >> combine
    extract >> water >> combine
    extract >> pm >> combine
    extract >> hpm >> combine
    extract >> voc >> combine

    tmp >> tmpgraph >> combine
    tmp >> feeltmp >> combine
    water >> feeltmp >> combine
    pm >> pmgrade >> combine
    hpm >> pmgrade >> combine
    voc >> pmgrade >> combine

    combine >> task_end
