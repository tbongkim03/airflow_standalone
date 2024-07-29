from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import PythonOperator
from pprint import pprint

with DAG(
        'movie',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='movie',
    #schedule_interval=timedelta(days=1),
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['movie'],
) as dag:
    
    def get_data(ds, **kwargs):
        print("###############################")
        #print(ds)
        #print(kwargs)
        print(f"ds_nodash => {kwargs['ds_nodash']}")
        print(f"kwargs type => {type(kwargs)}")
        print("###############################")

        from mov.api.call import get_key
        key = get_key()
        print(f"MOVIE_API_KEY => {key}")

    def print_context(ds=None, **kwargs):
        #print(kwargs)
        print(ds)
        #print(f"ds_nodash => {kwargs['ds_nodash']}")
        
    run_this = PythonOperator(
            task_id="print_the_context", 
            python_callable=print_context
    )

    task_get = PythonOperator(
        task_id='get.data',
        #bash_command="""
        #GET_URL=""
        #bash pip install git+${GET_URL}
        #"""
        python_callable=get_data
    )
    task_save = BashOperator(
        task_id="save.data",
        bash_command="""
        SAVE_URL=""
        #bash pip install git+${SAVE_URL}
        """
    )
    task_a = BashOperator(
        task_id="a",
        bash_command="""
        """
    )

    task_b = BashOperator(
        task_id="b",
        bash_command="""
        """
   )

    task_c = BashOperator(
            task_id="c",
            bash_command="""
            """
    )

    task_d = BashOperator(
            task_id="d",
            bash_command="""
            """
    )

    task_err = BashOperator(
            task_id="task_err",
            bash_command="""
            """,
            trigger_rule="one_failed"
    )

    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end', trigger_rule='all_done')

    task_start >> task_get
    task_get >> task_save
    task_save >> task_err

    task_save >> [task_a, task_b, task_c, task_d] >> task_end
    task_err >> task_end

    task_start >> run_this >> task_end
