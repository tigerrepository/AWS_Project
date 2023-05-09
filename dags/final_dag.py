import boto3, json , pprint, requests, textwrap, time, logging
import os, os.path
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, date
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
import pyarrow.parquet as pq
import numpy as np
import pandas as pd 
import s3fs,decimal,time
from airflow.exceptions import AirflowException
from s3path import S3Path
from airflow.models import DagRun

region_name = 'us-east-1'

emr_spark_job = "{{dag_run.conf['emr_spark_job']}}"
dataset_to_be_processed = "{{dag_run.conf['dataset_to_be_processed']}}"
dataset_path = "{{dag_run.conf['dataset_path']}}"
spark_config = "{{dag_run.conf['spark_config']}}"
app_config = "{{dag_run.conf['app_config']}}"

def client(region_name):
    global emr
    emr = boto3.client('emr', region_name=region_name)

def get_security_group_id(group_name, region_name):
    ec2 = boto3.client('ec2', region_name=region_name)
    response = ec2.describe_security_groups(GroupNames=[group_name])
    return response['SecurityGroups'][0]['GroupId']

def create_cluster(region_name, cluster_name='Vivek_Cluster' + str(datetime.now()), release_label='emr-6.2.1',master_instance_type='m5.xlarge', num_core_nodes=1, core_node_instance_type='m5.xlarge',**kwargs):
    emr_master_security_group_id = get_security_group_id('emr-securitygroup', region_name=region_name)
    emr_slave_security_group_id= emr_master_security_group_id
    cluster_response = emr.run_job_flow(
        Name=cluster_name,
        ReleaseLabel=release_label,
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': master_instance_type,
                    'InstanceCount': 1,
                },
                {
                    'Name': "Slave nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': core_node_instance_type,
                    'InstanceCount': num_core_nodes,
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'Ec2KeyName' : 'vivek_ec2_key',
            'EmrManagedMasterSecurityGroup': emr_master_security_group_id,
            'EmrManagedSlaveSecurityGroup': emr_slave_security_group_id
        },
        BootstrapActions= [
            {
                'Name': 'Install boto3',
                'ScriptBootstrapAction': {
                            'Path': 's3://landing-zone-vivek/Dependency/dependencies.sh',
                        }
            }
			],
		
        AutoTerminationPolicy = {'IdleTimeout': 600},	
        VisibleToAllUsers=True,
        JobFlowRole= "ec2-admin",
        ServiceRole= "emr_demo",
        Applications=[
            { 'Name': 'hadoop' },
            { 'Name': 'spark' },
            { 'Name': 'hive' },
            { 'Name': 'livy' },
            { 'Name': 'zeppelin' },
            { 'Name': 'JupyterEnterpriseGateway'},
            { 'Name': 'JupyterHub'},
        ]
    )
    return cluster_response['JobFlowId']


def get_cluster_dns(cluster_id):
    response = emr.describe_cluster(ClusterId=cluster_id)
    return response['Cluster']['MasterPublicDnsName']


def wait_for_cluster_creation(cluster_id):
    emr.get_waiter('cluster_running').wait(ClusterId=cluster_id)

def livy_task(master_dns,emr_spark_job,dataset,spark_config,dataset_path):
    host = 'http://' + master_dns + ':8998'
    data = {"file":emr_spark_job, "className":"com.example.SparkApp","args":[dataset,spark_config,dataset_path]}
    headers = {'Content-Type': 'application/json'}
    response =requests.post(host+ '/batches', data=json.dumps(data), headers=headers)
    logging.info(response.json())
    return response.json()
    
def terminate_cluster(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.terminate_job_flows(JobFlowIds=[cluster_id])
    
    
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022,2,15),
    'email': ['vivek@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True
}


# Initialize the DAG
# Concurrency --> Number of tasks allowed to run concurrently
dag = DAG('AWS_Transformation', concurrency=3, schedule_interval=None, default_args=default_args)

client(region_name='us-east-1')

#session=boto3.Session(aws_access_key_id='',aws_secret_access_key='')
s3 = boto3.resource('s3')

def read_config(app_config):
    response = s3.Object('landing-zone-vivek',app_config)
    data=response.get()['Body'].read().decode('utf-8')
    appdata=json.loads(data)
    return appdata
    
def copy_data(dataset_path,**kwargs):
    ti =kwargs['ti']
    data = ti.xcom_pull(task_ids='read_config')
    copy_source = {
                'Bucket': data['landing-bucket'],
                'Key': dataset_path
            }
    bucket = s3.Bucket(data['raw-bucket'])
    bucket.copy(copy_source, dataset_path)
    
def pre_validation(dataset_path,dataset,**kwargs):
    ti=kwargs['ti']
    data = ti.xcom_pull(task_ids='read_config')
    
    s3 = s3fs.S3FileSystem()
    
    landing_path = 's3://' + data['landing-bucket'] + '/' +  dataset_path
    raw_path = 's3://' + data['raw-bucket'] + '/' +  dataset_path
    
    df_landingzone=pq.ParquetDataset(landing_path, filesystem=s3).read_pandas().to_pandas()
    df_rawzone=pq.ParquetDataset(raw_path, filesystem=s3).read_pandas().to_pandas()
    
    if df_rawzone[df_rawzone.columns[0]].count()!= 0:
        for raw_columnname in df_rawzone.columns:
            if df_rawzone[raw_columnname].count() == df_landingzone[raw_columnname].count():
                print('Count satisfied with',str(raw_columnname))
            else:
                print("count not matched")
                return ValueError("Count ", str(raw_columnname))
    else:
        raise ValueError("No Data Available")
        
    
    Actives_schema = {'advertising_id':str,'city':str,'location_category':str,'location_granularities':str,'location_source':np.ndarray,'state':str,
                    'timestamp':np.int64,'user_id':str,'user_latitude':np.float64,'user_longitude':np.float64,'month':str,'date':date}
    Viewership_schema = {'advertising_id':str,'channel_genre':str,'channel_name':str,'city':str,'device':str,'device_type':str,'duration':int,'grid_id':str,
                        'language':str,'location_category':str,'location_granularities':str,'location_source':np.ndarray,'record_timestamp':np.int64,
                        'show_genre':str,'show_name':str,'state':str,'user_lat':np.float64,'user_long':np.float64,'month':str,'date':date}
                        
    if dataset == 'Actives':
        schema = Actives_schema
    else:
        schema = Viewership_schema
        
    for raw_columnname in df_rawzone.columns:
        if isinstance(df_rawzone[raw_columnname][0],schema[raw_columnname]):
            print('Column data type satisfied with',str(raw_columnname))
        else:
            print("datatype not matched")
            return TypeError("datatype ", str(raw_columnname))
            
            
def create_emr(**kwargs):
    cluster_id = create_cluster(region_name='us-east-1', cluster_name='Vivek_Cluster',  num_core_nodes=1)
    return cluster_id
    
def wait_for_completion(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    wait_for_cluster_creation(cluster_id)
    
def transformation(emr_spark_job,dataset,spark_config,dataset_path,**kwargs):
    # ti is the Task Instance
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    wait_for_cluster_creation(cluster_id)
    cluster_dns = get_cluster_dns(cluster_id)
    statement_response = livy_task(cluster_dns,emr_spark_job,dataset,spark_config,dataset_path)
    return statement_response
    
def terminate_emr(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    terminate_cluster(cluster_id)
    
def post_validation(dataset_path,dataset,**kwargs):
    ti=kwargs['ti']
    data = ti.xcom_pull(task_ids='read_config')
	
    s3f = s3fs.S3FileSystem()
	
    #to get staging zone location in partitioned way
    months = ['January','February','March','April','May','June','July','August','September','October','November','December']
    today = date.today()
    month = months[today.month-1]
 
    raw_path = 's3://' + data['raw-bucket'] + '/' + dataset_path
    staging_path =  data['mask-'+ dataset]['destination']['data-location'] + 'month=' + month + '/' + 'date=' + str(today) 
    """
    while not os.path.exists(staging_path):
        time.sleep(1)
    if os.path.isfile(staging_path):
    
    
    #wait_for_s3_object(path)
    waiter= s3.get_waiter('object_exists')
    waiter.wait(Bucket=data['staging-bucket'], Key = path,
                  WaiterConfig={
                     'Delay': 2})
    """
    path = ('/').join(staging_path.split('/')[1:])
    while not S3Path(path).exists():
        time.sleep(1)
    
    df_rawzone=pq.ParquetDataset(raw_path, filesystem=s3f).read_pandas().to_pandas()
    df_stagingzone=pq.ParquetDataset(staging_path, filesystem=s3f).read_pandas().to_pandas()
    
    if df_stagingzone[df_stagingzone.columns[0]].count()!= 0:
        for staging_columnname in df_stagingzone.columns:
            if df_stagingzone[staging_columnname].count() == df_rawzone[staging_columnname].count():
                print('Count satisfied with',str(staging_columnname))
            else:
                print("count not matched")
                return ValueError("Count ", str(staging_columnname))
    else:
        raise ValueError("No Data Available")	
    
    
    
    Actives_schema = {'advertising_id':str,'city':str,'location_category':str,'location_granularities':str,'location_source':np.ndarray,'state':str,
                    'timestamp':np.int64,'user_id':str,'user_latitude':np.float64,'user_longitude':np.float64,'month':str,'date':date}
    
    Viewership_schema = {'advertising_id':str,'channel_genre':str,'channel_name':str,'city':str,'device':str,'device_type':str,'duration':int,'grid_id':str,
                        'language':str,'location_category':str,'location_granularities':str,'location_source':np.ndarray,'record_timestamp':np.int64,
                        'show_genre':str,'show_name':str,'state':str,'user_lat':np.float64,'user_long':np.float64,'month':str,'date':date}
                        
    if dataset == 'Actives':
        schema = Actives_schema
        casting_cols= data['mask-'+ dataset]['datatype-update-cols'] + ["location_source:StringType","user_latitude:DecimalType","user_longitude:DecimalType"]
    else:
        schema = Viewership_schema
        casting_cols= data['mask-'+ dataset]['datatype-update-cols'] + ["location_source:StringType","user_lat:DecimalType","user_long:DecimalType"]
        
    datatype_dict = {'StringType':str,'IntegerType':int,'LongType':np.int64,'DoubleType':np.float64,'DateType':date,'ArrayType':np.ndarray,
                    'DecimalType':decimal.Decimal}
    
    
    for item in casting_cols:
        col=item.split(':')[0]
        dtype=item.split(':')[1]
        schema[col]=datatype_dict[dtype]
        
    for staging_columnname in df_stagingzone.columns:
        if isinstance(df_stagingzone[staging_columnname][0],schema[staging_columnname]):
            print('Column data type satisfied with',str(staging_columnname))
        else:
            print("datatype not matched")
            return TypeError("datatype ", str(staging_columnname)) 
    
read_config=PythonOperator(
    task_id='read_config',
    python_callable=read_config,
    op_kwargs={'app_config': app_config},
    dag=dag)
    
copy_data=PythonOperator(
    task_id='copy_data',
    python_callable=copy_data,
    op_kwargs={'dataset_path': dataset_path},
    dag=dag)

pre_validation=PythonOperator(
    task_id='pre_validation',
    python_callable=pre_validation,
    op_kwargs={'dataset_path': dataset_path, 'dataset':dataset_to_be_processed},
    dag=dag)
 
# Define the individual tasks using Python Operators
create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=create_cluster,
    op_kwargs={'region_name':'us-east-1', 'cluster_name':'Vivek_Cluster', 'num_core_nodes':1},
    dag=dag)

wait_for_cluster_completion = PythonOperator(
    task_id='wait_for_cluster_completion',
    python_callable=wait_for_completion,
    dag=dag)

transformation = PythonOperator(
    task_id='transformation',
    python_callable=transformation,
    op_kwargs={'emr_spark_job':emr_spark_job, 'dataset':dataset_to_be_processed, 'spark_config':spark_config,'dataset_path':dataset_path},
    dag=dag)

terminate_cluster = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_cluster,
    trigger_rule='all_done',
    dag=dag)

post_validation=PythonOperator(
    task_id='post_validation',
    python_callable=post_validation,
    op_kwargs={'dataset_path': dataset_path, 'dataset':dataset_to_be_processed},
    dag=dag)


# construct the DAG by setting the dependencies
read_config >> copy_data >> pre_validation >> create_cluster >> wait_for_cluster_completion
wait_for_cluster_completion >> transformation >> terminate_cluster >> post_validation


