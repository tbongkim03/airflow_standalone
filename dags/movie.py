from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import PythonOperator, PythonVirtualenvOperator, BranchPythonOperator

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
    start_date=datetime(2024, 7, 25),
    catchup=True,
    tags=['movie'],
) as dag:
   
    def common_get_data(ds_nodash, url_param):
    #def common_get_data(ds_nodash, {"MOVIE_4_KEY": "F"}):
        from mov.api.call import save2df
        df = save2df(load_dt=ds_nodash, url_param=url_param)
        
        print(df[['movieCd', 'movieNm']].head(5))
        
        for k, v in url_param.items():
            df[k] = v
        
        #p_cols = list(url_param.keys()).insert(0, 'load_dt')
        p_cols = ['load_dt'] + list(url_param.keys())
        df.to_parquet('~/tmp/test_parquet', 
                partition_cols=p_cols
                # partition_cols=['load_dt', 'movieKey']
        )

    def save_data(ds_nodash):
        from mov.api.call import apply_type2df
        
        df = apply_type2df(load_dt=ds_nodash)
        
        print("*" * 33)
        print(df.head(10))
        print("*" * 33)
        print(df.dtypes)

        # 개봉일 기준 그룹핑 누적 관객수 합
        print("개봉일 기준 그룹핑 누적 관객수 합")
        g = df.groupby('openDt')
        sum_df = g.agg({'audiCnt': 'sum'}).reset_index()
        print(sum_df)

    def branch_fun(ds_nodash):
        import os
        home_dir = os.path.expanduser("~")
        path = os.path.join(home_dir, f"tmp/test_parquet/load_dt={ds_nodash}")
        if os.path.exists(path):
            return rm_dir.task_id
        else:
            return "get.start", "task.echo"

    branch_op = BranchPythonOperator(
            task_id="branch.op",
            python_callable=branch_fun,
            #provide_context=True
    )
        
    task_save = PythonVirtualenvOperator(
        task_id="save.data",
        python_callable=save_data,
        requirements=["git+https://github.com/tbongkim03/mov.git@0.3.1/url_param"],
        system_site_packages=False,
        trigger_rule='one_success',
        #venv_cache_path="/home/michael/tmp/airflow_venv/get_data"
    )
   
    # 다양성 영화 유무
    multi_y = PythonVirtualenvOperator(
        task_id='multi.y',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=["git+https://github.com/tbongkim03/mov.git@0.3.1/url_param"],
        #op_args=[1,2,3,4],
        op_kwargs={
            "url_param": {"multiMovieYn": "Y"},
        },
    )
    
    multi_n = PythonVirtualenvOperator(
        task_id='multi.n',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=["git+https://github.com/tbongkim03/mov.git@0.3.1/url_param"],
        #op_args=["{{ds_nodash}}", "{{ds}}"],
        op_kwargs={
            "url_param": {"multiMovieYn": "N"}
            #"ds": "2024-11-11",
            #"ds_nodash", "2024111"
            #.
            #.
            #.
        }
    )

    nation_k = PythonVirtualenvOperator(
        task_id='nation.k',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=["git+https://github.com/tbongkim03/mov.git@0.3.1/url_param"],
        #op_args=["{{ds_nodash}}", "{{ds}}"],
        op_kwargs={
            "url_param": {"repNationCd": "K"}
        }
    )

    nation_f = PythonVirtualenvOperator(
        task_id='nation.f',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=["git+https://github.com/tbongkim03/mov.git@0.3.1/url_param"],
        #op_args=["{{ds_nodash}}", "{{ds}}"],
        op_kwargs={
            "url_param": {"repNationCd": "F"}
        }
    )

    rm_dir = BashOperator(
        task_id="rm.dir",
        bash_command="""
        figlet REMOVE SUCCESS
        rm -rf ~/tmp/test_parquet/load_dt={{ ds_nodash }}
        """
    )

    task_echo = BashOperator(
        task_id='task.echo',
        bash_command="echo 'task'"
    )

    get_start = EmptyOperator(
                    task_id='get.start',
                    trigger_rule="all_done"
                )
    get_end = EmptyOperator(task_id='get.end')

    throw_err = BashOperator(
            task_id='throw.err',
            bash_command="exit 1",
            trigger_rule="all_done"

    )

    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end', trigger_rule='all_done')

    task_start >> branch_op
    task_start >> throw_err >> task_save
    
    branch_op >> [ rm_dir, get_start, task_echo ]
    
    rm_dir >> get_start
    
    get_start >> [ multi_y, multi_n, nation_k, nation_f ] >> get_end

    get_end >> task_save >> task_end
