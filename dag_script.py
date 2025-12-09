from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from Extraction import run_extraction # here we import  the function from our etl script
from Transformation import run_transformation
from Loading import run_loading



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 9),
    'email': 'JojoXandra@outlook.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retries_delay': timedelta(minutes=1)
}

dag = DAG(
    'zipco_food_pipeline_dag',
    default_args=default_args,
    description='This represents Zipco Food etl pipeline'
)

extraction = PythonOperator(
    task_id='extraction_layer',
    python_callable=run_extraction,
    dag=dag,
)

transformation = PythonOperator(
    task_id='transformation_layer',
    python_callable=run_transformation,
    dag=dag,
)

loading = PythonOperator(
    task_id='loading_layer',
    python_callable=run_loading,
    dag=dag,
)

extraction >> transformation >> loading