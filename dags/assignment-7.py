from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

def hello_world_py(*args, **kwargs):
    print('Hello World from PythonOperator')

default_args = {
    'start_date': days_ago(1),
}

dag = DAG(
    'assignment-7',
    default_args=default_args,
    description='Assignment 7 DAG',
    schedule_interval=None,
    # schedule_interval='*/1 * * * *',
)

s3_sensor = S3KeySensor(
    task_id='s3_bucket_sensor',
    bucket_name='gds-assignment-7-tavish',
    bucket_key='landing_zone/',
    aws_conn_id='aws_default',
    dag=dag,
)

step_adder = EmrAddStepsOperator(
    task_id='trigger_spark_job_on_emr',
    job_flow_id='j-2SPMXOJ98PXHP',
    aws_conn_id='aws_default',
    steps=[{
        'Name': 'Run PySpark Script',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                's3://gds-assignment-7-tavish/s3_to_redshift_spark.py',
            ],
        },
    }],
    dag=dag,
)

step_checker = EmrStepSensor(
    task_id='check_step',
    job_flow_id='j-2SPMXOJ98PXHP',
    step_id="{{ task_instance.xcom_pull(task_ids='trigger_spark_job_on_emr', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    poke_interval=120,  # Check every 2 minutes
    timeout=86400,  # Fail if not completed in 1 day
    mode='poke',
    dag=dag,
)

s3_sensor >> step_adder >> step_checker