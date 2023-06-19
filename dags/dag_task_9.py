import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
import pandas as pd
from pandas import DataFrame
from pandas.core import series

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 6, 18),
    'email': ['innowisetestacc@gmail.com'],
}

dag = DAG('task_9', default_args=default_args, schedule_interval='*/20 * * * *')


def more_than_10_errors_in_a_minute(**kwargs):
    df = kwargs['task_instance'].xcom_pull(task_ids='load_data_into_df_task')
    df.set_index('date', inplace=True)
    errors_per_minute = df.resample('1T').count()
    print(type(errors_per_minute))
    instances_with_more_than_10_errors = errors_per_minute[errors_per_minute['severity'] > 10]
    if not instances_with_more_than_10_errors.empty:
        instances_with_more_than_10_errors.to_csv('/opt/airflow/data/final_min.csv')
        return True
    return False


def more_than_10_errors_in_one_bundle_id_in_1_hour(**kwargs):
    df = kwargs['task_instance'].xcom_pull(task_ids='load_data_into_df_task')
    print(type(df))
    df.set_index('date', inplace=True)
    print(type(df))
    errors_per_hour = df.resample('1H').apply(lambda x: x['bundle_id'].value_counts())
    print(type(errors_per_hour))
    df = DataFrame(errors_per_hour)
    instances_with_more_than_10_errors_in_hour = df[df['count'] > 10]
    if not instances_with_more_than_10_errors_in_hour.empty:
        instances_with_more_than_10_errors_in_hour.to_csv('/opt/airflow/data/final_hour.csv')
        return True
    return False


def load_data_into_df():
    column_names = ['severity', 'bundle_id', 'date']
    df = pd.read_csv("/opt/airflow/data/data.csv", usecols=[2, 15, 23], names=column_names)
    df_with_errors_only = df[df['severity'] == 'Error']
    df_with_errors_only['date'] = pd.to_datetime(df['date'], unit='s')
    return df_with_errors_only


def send_email(**kwargs):
    condition_met_min = kwargs['task_instance'].xcom_pull(task_ids='check_1_minute_task')
    condition_met_hour = kwargs['task_instance'].xcom_pull(task_ids='check_1_hour_task')
    if condition_met_min:
        subject = 'Error alert!'
        body = 'More than 10 errors in a minute.'
        email_operator = EmailOperator(
            task_id='send_email_task',
            to='innowisetestacc@gmail.com',
            subject=subject,
            html_content=body,
            files=['/opt/airflow/data/final_min.csv'],
        )
        email_operator.execute(context=kwargs)
    if condition_met_hour:
        subject = 'Error alert!'
        body = 'More than 10 errors in an hour in one bundle.'
        email_operator = EmailOperator(
            task_id='send_email_task',
            to='innowisetestacc@gmail.com',
            subject=subject,
            html_content=body,
            files=['/opt/airflow/data/final_hour.csv'],
        )
        email_operator.execute(context=kwargs)


def delete_file():
    file_path_1 = '/opt/airflow/data/data.csv'
    file_path_2 = '/opt/airflow/data/final_hour.csv'
    file_path_3 = '/opt/airflow/data/final_min.csv'
    if os.path.exists(file_path_1):
        os.remove(file_path_1)
        return True
    if os.path.exists(file_path_2):
        os.remove(file_path_2)
        return True
    if os.path.exists(file_path_3):
        os.remove(file_path_3)
        return True
    return False


# delete_file_task = PythonOperator(
#     task_id='delete_file_task',
#     python_callable=delete_file,
#     dag=dag,
# )

file_sensor_task = FileSensor(
    task_id='file_sensor_task',
    fs_conn_id="task_9_fs",
    poke_interval=10,
    timeout=20,
    filepath='data.csv',
    dag=dag,
)

load_data_into_df_task = PythonOperator(
    task_id='load_data_into_df_task',
    python_callable=load_data_into_df,
    provide_context=True,
    dag=dag,
)

check_1_minute_task = PythonOperator(
    task_id='check_1_minute_task',
    python_callable=more_than_10_errors_in_a_minute,
    provide_context=True,
    dag=dag,
)

check_1_hour_task = PythonOperator(
    task_id='check_1_hour_task',
    python_callable=more_than_10_errors_in_one_bundle_id_in_1_hour,
    provide_context=True,
    dag=dag,
)

send_email_task = PythonOperator(
    task_id='send_email_task',
    python_callable=send_email,
    provide_context=True,
    dag=dag,
)

# file_sensor_task >> check_condition_task >> send_email_task >> delete_file_task

file_sensor_task >> load_data_into_df_task >> [check_1_minute_task, check_1_hour_task] >> send_email_task
