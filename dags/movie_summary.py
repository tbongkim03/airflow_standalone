from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator, BranchPythonOperator

from pprint import pprint
import pandas as pd

with DAG(
        'movie_summary',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='movie_summary',
    #schedule_interval=timedelta(days=1),
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 25),
    catchup=True,
    tags=['movie_summary'],
) as dag:
    REQUIREMENTS = [
        "git+https://github.com/tbongkim03/mov_agg.git@0.5/agg"
                ]

    def gen_empty(*ids):
        tasks=[]
        for id in ids:
            task = EmptyOperator(task_id=id)
            tasks.append(task)
        return tuple(tasks)

    def gen_vpython(**idsfns):
        tasks=[]
        for id, fn in idsfns.items():
            
            # opkw 때문에
            if type(fn) == list:
                kw=fn[1]
                #print("*"*100)
                #pprint(f"{id} : {fn[0]}\n{kw}")
                #print("*"*100)
                task = PythonVirtualenvOperator(
                    task_id=id,
                    python_callable=fn[0],
                    requirements=REQUIREMENTS,
                    system_site_packages=False,
                    op_kwargs = kw
                )
            else:
       #         print("*"*100)
        #        pprint(f"{id} : {fn}")
         #       print("*"*100)
                task = PythonVirtualenvOperator(
                    task_id=id,
                    python_callable=fn,
                    requirements=REQUIREMENTS,
                    system_site_packages=False,
                )
            tasks.append(task)
        #print("*"*10)
        return tasks

    def apply_type(**context):
        from pprint import pprint
        print("*"*100)
        pprint(context)
        print("*"*100)

        #from mov.api.call import apply_type2df
        #df = apply_type2df(load_dt=ds_nodash)
    def merge_df(ds_nodash):
        from mov_agg.util import merge
        df = merge(int(ds_nodash))
        print(df)
        #print(ds.to_string(index = False))
    def de_dup():
        print("ddf")

    def summary_df():
        print("smf")
    
    start, end = gen_empty('start', 'end')
    
    opkw = { "url_param": {"multiMovieYn": "Y"} }

    idsfns = {
            "type.fun": [apply_type,opkw],
            "merge.fun": merge_df,
            "dedup.fun": de_dup,
            "summary.fun": summary_df

    }
    tasks = gen_vpython(**idsfns)

    start >> tasks[1] >> tasks[2] >> tasks[0] >> tasks[3] >> end
