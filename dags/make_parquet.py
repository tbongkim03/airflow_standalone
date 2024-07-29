from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

def gen_emp(id, rule='all_success'):
    op = EmptyOperator(task_id=id, trigger_rule=rule)
    return op

with DAG(
        'make_parquet.py',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='make_parquet',
    #schedule_interval=timedelta(days=1),
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['parquet', 'hadoop', 'column'],
) as dag:

    task_check = BashOperator(
        task_id='check.done',
        bash_command="""
        DONE_FILE={{ var.value.IMPORT_DONE_PATH }}/{{ds_nodash}}/_DONE
        bash {{ var.value.CHECK_SH }} $DONE_FILE
        """
    )

    task_parquet = BashOperator(
        task_id='to.parquet',
        bash_command="""
        figlet "to.parquet start"
       
        #mkdir -p ~/data/parquet/{{ds_nodash}}

        READ_PATH=~/data/csv/{{ds_nodash}}/csv.csv
        SAVE_PATH=~/data/parquet/
        
        Dir="dt={{ds}}"
        echo $SAVE_PATH/$Dir
        if [ -d $SAVE_PATH/$Dir ]; then
            rm -rf $SAVE_PATH/$Dir
            figlet "DELETE !!!!!!"
        fi
        python ~/airflow/py/csv2parquet.py $READ_PATH $SAVE_PATH
        figlet "make.parquet done"
        """
    )

    task_err = BashOperator(
            task_id="task_err",
            bash_command="""
            """,
            trigger_rule="one_failed"
    )

    task_done = BashOperator(
        task_id='make.done',
        bash_command="""
        """
     )
    
    task_start = gen_emp('start')
    task_end = gen_emp('end', 'all_done')

    task_start >> task_check

    task_check >> task_parquet >> task_done
    
    task_check >> task_err >> task_end

    task_done >> task_end
