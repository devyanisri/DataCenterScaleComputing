from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


from ETL.local2s3 import main
from ETL.transform import transform_main
from ETL.rds import rds_main


default_args = {
    "owner": "devyani.srivastava",
    "depends_on_past": False,
    "start_date": datetime(2023, 11, 19),
    "retries": 1,
}

with DAG(
    dag_id="outcomes_DAG",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
        start = BashOperator(task_id = "START",
                             bash_command = "echo start")

        extract_data_to_s3 =  PythonOperator(task_id = "EXTRACT_DATA_TO_S3",
                                                  python_callable = main,)
        
        transform_data =  PythonOperator(task_id = "TRANSFORM_DATA",
                                                  python_callable = transform_main,)
        
        load_dim_animals = PythonOperator(task_id="LOAD_DIM_ANIMALS",
                                             python_callable=rds_main,
                                             op_kwargs={"file_name": 'dim_animals.csv', "table_name": 'dim_animals'},)

        load_dim_outcome_types = PythonOperator(task_id="LOAD_DIM_OUTCOME_TYPES",
                                             python_callable=rds_main,
                                             op_kwargs={"file_name": 'dim_outcome_types.csv', "table_name": 'dim_outcome_types'},)
        
        load_dim_dates = PythonOperator(task_id="LOAD_DIM_DATES",
                                             python_callable=rds_main,
                                             op_kwargs={"file_name": 'dim_dates.csv', "table_name": 'dim_dates'},)
        
        load_fct_outcomes = PythonOperator(task_id="LOAD_FCT_OUTCOMES",
                                             python_callable=rds_main,
                                             op_kwargs={"file_name": 'fct_outcomes.csv', "table_name": 'fct_outcomes'},)

        end = BashOperator(task_id = "END", bash_command = "echo end")

        start >> extract_data_to_s3 >> transform_data >> [load_dim_animals, load_dim_outcome_types, load_dim_dates, load_fct_outcomes] >> end
        