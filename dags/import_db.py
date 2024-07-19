from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
with DAG(
        'import_db',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='import db csv',
    #schedule_interval=timedelta(days=1),
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['simple', 'bash', 'etl', 'shop'],
) as dag:

    task_check = BashOperator(
        task_id='check.done',
        bash_command="""
        bash {{ var.value.CHECK_SH }} {{ds_nodash}}
        """
    )

    task_to_csv = BashOperator(
        task_id="to.csv",
        bash_command="""
        COUNT_PATH_FILE=~/data/count/{{ds_nodash}}/count.log
        
        mkdir -p ~/data/csv/{{ds_nodash}}
        CSV_PATH=~/data/csv/{{ds_nodash}}
        
        if [ $? == 0 ]; then
            figlet "success"
            cat $COUNT_PATH_FILE | awk '{print "{{ds}},"$2","$1}' > $CSV_PATH/csv.csv
        else
            figlet "fail"
        fi
        """
    )

    task_to_tmp = BashOperator(
            task_id="to.tmp",
            bash_command="""
            """
    )

    task_to_base = BashOperator(
            task_id="to.base",
            bash_command="""
            """
    )

    task_make_done = BashOperator(
            task_id="make_done",
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
    task_end = EmptyOperator(task_id='end')

    task_start >> task_check
    task_check >> task_to_csv
    task_check >> task_err

    task_to_csv >> task_to_tmp >> task_to_base >> task_make_done

    task_err >> task_end
    task_make_done >> task_end
