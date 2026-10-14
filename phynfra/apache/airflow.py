# -*- coding: utf-8 -*-

# get_sa_credentials => phynfra.google.authentication.Authentication
# get_boto3_session => Não feito. boto3.session.Session "já é " isso
# get_secretsmanager_secret => phynfra.aws.secretsmanager.SM.read
# create_service_account_credentials_file_from_secrets_manager => Não feito. Não vejo porquê gravar em arquivo as credenciais. Security reasons
# send_email => phynfra.google.mail.Mail.send
# send_success_email => phynfra.google.mail.Mail.send
# send_failure_email => phynfra.google.mail.Mail.send
# send_slack_alert => phynfra.slack.Slack.notify
# get_airflow_var => Não feito. function to function
# task_failure_callback => Não feito. Parte do Projeto e não do phhynfra

import json

import itertools
import os
import pandas as pd

from typing import List
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from airflow.hooks.base_hook import BaseHook
from airflow.models import DagRun, Variable
from airflow.models.param import Param
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import DagRunState
from airflow.utils.task_group import TaskGroup
from datahub_provider.entities import Dataset
from datetime import datetime, timedelta
from typing import List
from airflow.providers.google.common.hooks.discovery_api import GoogleDiscoveryApiHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from deltalake import DeltaTable
from airflow.exceptions import AirflowSkipException

def get_airflow_connection(conn_id) -> dict:
	conn = BaseHook.get_connection(conn_id)
	conn_info = json.loads(conn.extra)
	return conn_info

def get_most_recent_dag_run_date(dag_id):
	dag_runs = DagRun.find(dag_id=dag_id, state=DagRunState.SUCCESS)
	dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
	return dag_runs[0].start_date.strftime("%Y-%m-%d") if dag_runs else None

def add_k8s_tasks(task_id: str,dag: str,task_group: str,zone_name: str,job_name: str,table_name: str,docker_image: str,env_vars: dict,dataset_platform_in: List[str],dataset_platform_out: List[str],table_platform_in: List[str],table_platform_out: List[str],on_failure_callback=task_failure_callback):
	
	task_inlets = [
		Dataset(dataset_in, platform_in) for (dataset_in, platform_in) in zip(dataset_platform_in, table_platform_in)
	]

	task_outlets = [
		Dataset(dataset_out, platform_out)
		for (dataset_out, platform_out) in zip(dataset_platform_out, table_platform_out)
	]

	task = KubernetesPodOperator(
		dag=dag,
		task_id=task_id,
		task_group=task_group,
		kubernetes_conn_id="kubernetes_default",
		namespace="orchestrator",
		image=docker_image,
		arguments=[
			"python",
			"main.py",
			job_name,
			zone_name,
			table_name,
		],
		name=f"elecman-{zone_name}-{job_name}-{table_name}",
		random_name_suffix=True,
		env_vars=env_vars,
		in_cluster=True,
		reattach_on_restart=False,
		get_logs=True,
		image_pull_policy="Always",
		service_account_name="airflow-worker",
		log_events_on_failure=True,
		do_xcom_push=False,
		base_container_name=f"elecman-{zone_name}-{job_name}-{table_name}".replace("_", "-"),
		inlets=task_inlets,
		outlets=task_outlets,
		on_failure_callback=on_failure_callback
	)

	return task

def add_emr_serverless_tasks(task_id: str,dag: str,task_group: str,zone_name: str,job_name: str,table_name: str,application_id: str,s3_job_path: str,job_role_arn: str,default_monitoring_config: dict,env_vars: dict,dataset_platform_in: List[str] = [],dataset_platform_out: List[str] = [],table_platform_in: List[str] = [],table_platform_out: List[str] = [],timeout_seconds=EMR_JOBS_TIMEOUT_MINUTES * 60):
	
	task_inlets = [
		Dataset(dataset_in, platform_in) for (dataset_in, platform_in) in zip(dataset_platform_in, table_platform_in)
	]

	task_outlets = [
		Dataset(dataset_out, platform_out)
		for (dataset_out, platform_out) in zip(dataset_platform_out, table_platform_out)
	]

	spark_env_vars = ""
	for (env_var_name, env_var_value) in env_vars.items():
		driver_conf = f"--conf spark.emr-serverless.driverEnv.{env_var_name}={env_var_value} "
		executor_conf = f"--conf spark.emr-serverless.executorEnv.{env_var_name}={env_var_value} "
		spark_env_vars += driver_conf + executor_conf

	extra_conf = (
		"--packages io.delta:delta-spark_2.13:4.0.0" + " "
		f"--conf spark.archives={s3_job_path}/pyspark_delta_package.tar.gz#environment" + " "
		"--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python" + " "
		"--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python" + " "
		"--conf spark.emr-serverless.executorEnv.PYSPARK_PYTHON=./environment/bin/python" + " "
		"--conf spark.dynamicAllocation.maxExecutors=40" + " "
		"--conf spark.sql.parquet.int96RebaseModeInRead=LEGACY" + " "
		"--conf spark.sql.parquet.int96RebaseModeInWrite=LEGACY" + " "
	)

	emr_conf = extra_conf + spark_env_vars

	task = EmrServerlessStartJobOperator(
		dag=dag,
		task_id=task_id,
		task_group=task_group,
		application_id=application_id,
		name=f"elecman-{zone_name}-{job_name}-{table_name}".replace("_", "-"),
		execution_role_arn=job_role_arn,
		job_driver={
			"sparkSubmit": {
				"entryPoint": f"{s3_job_path}/main.py",
				"entryPointArguments": [job_name, zone_name, table_name],
				"sparkSubmitParameters": f"--conf spark.submit.pyFiles={s3_job_path}/app.zip {emr_conf}",
			}
		},
		configuration_overrides=default_monitoring_config,
		waiter_countdown=timeout_seconds + 120,
		config={"executionTimeoutMinutes": EMR_JOBS_TIMEOUT_MINUTES},
		inlets=task_inlets,
		outlets=task_outlets,
	)

	return task

class GoogleDriveSensor(BaseSensorOperator):
	
	template_fields = (
		"drive_id",
		"folder_id",
	)

	def __init__(self,path_table_processamento,gcp_sa_email,gcp_conn_id,drive_id,folder_id,ano_referencia,mes_referencia,*args,**kwargs):
		
		super(GoogleDriveSensor, self).__init__(*args, **kwargs)
		self.path_table_processamento = path_table_processamento
		self.gcp_sa_email = gcp_sa_email
		self.gcp_conn_id = gcp_conn_id
		self.drive_id = drive_id
		self.folder_id = folder_id
		self.ano_referencia = ano_referencia
		self.mes_referencia = mes_referencia

	# Comment delegate_to due to error in airflow
	# TypeError: __init__() got an unexpected keyword argument 'delegate_to'
	def get_files(self) -> List[dict]:
		try:
			print(f"Getting files from google drive {self.folder_id}")
			gcp_hook = GoogleDiscoveryApiHook(
				api_version="v3",
				api_service_name="drive",
				gcp_conn_id=self.gcp_conn_id,
				# delegate_to=self.gcp_sa_email,
			)
			service = gcp_hook.get_conn()

			files = []
			page_token = None
			while True:
				results = (
					service.files()
					.list(
						q=f"(mimeType='image/jpeg' or mimeType='image/jpg' or mimeType='image/png') and ('{self.folder_id}' in parents)",
						pageSize=1000,
						# Added nextPageToken. Otherwise, it won't work and will not paginate
						fields="nextPageToken, files(id, name, modifiedTime, size, mimeType, ownedByMe)",
						spaces="drive",
						driveId=self.drive_id,
						includeItemsFromAllDrives=True,
						corpora="drive",
						supportsAllDrives=True,
						pageToken=page_token,
					)
					.execute()
				)

				items = results.get("files", [])
				files.append(items)
				page_token = results.get("nextPageToken", None)
				if page_token is None:
					break

			if not files:
				print("No files found.")
				return [{}]

			return itertools.chain(*files)
		except Exception as error:
			print(f"An error occurred: {error}")
			raise error

	def delta_table_exists(self, s3_path: str, storage_options: dict):
		try:
			DeltaTable(s3_path, storage_options=storage_options)
			return True
		except Exception:
			# print(e)
			return False

	def get_processamento_table(self, s3_path: str, storage_options: dict) -> pd.DataFrame:
		print(f"Getting processamento table from {s3_path}")
		dt = DeltaTable(s3_path, storage_options=storage_options)
		df = dt.to_pandas()
		logger.info(f"Considerando {str(self.ano_referencia)}/{str(self.mes_referencia)}")
		logger.info(f"Serão sincronizadas {str(df.shape[0])} imagens")
		return df

	def cache_table(self, table: pd.DataFrame, local_path: str):
		print(f"Caching table at: {local_path}")
		table.to_parquet(local_path, index=False)

	def invalidate_cache(self, local_path: str):
		print(f"Invalidating cache at {local_path}")
		if self.table_exists(local_path):
			os.remove(local_path)

	def table_exists(self, local_path: str):
		return os.path.isfile(local_path)

	def poke(self, context):
		print("Poking")
		s3_path = self.path_table_processamento
		s3_options = {"AWS_S3_ALLOW_UNSAFE_RENAME": "true"}

		processamento_exists = self.delta_table_exists(s3_path, s3_options)

		local_cache_path = "/tmp/cached_processamento.parquet"
		if processamento_exists:
			processamento_is_cached = self.table_exists(local_cache_path)
			if not processamento_is_cached:
				print("Processamento is not cached")
				processamento_df: pd.DataFrame = self.get_processamento_table(s3_path, s3_options)
				print("Processamento table will be cached")
				self.cache_table(processamento_df, local_cache_path)
			else:
				print("Processamento is cached")
				processamento_df: pd.DataFrame = pd.read_parquet(local_cache_path)
		else:
			processamento_df: pd.DataFrame = pd.DataFrame({})

		incoming_drive_files = self.get_files()
		incoming_files_df: pd.DataFrame = pd.DataFrame(incoming_drive_files, columns=["id", "name"])

		print(f"Dados vindos do Google Drive da pasta {self.folder_id}")
		print(incoming_files_df.head())

		print("Comparing incoming files with processamento table")
		if not processamento_df.empty:
			files_to_process: set = set(incoming_files_df["id"]) - set(processamento_df["id"])
		else:
			files_to_process: set = set(incoming_files_df["id"])

		print(f"Dados que não estavam na tabela de processamento {self.path_table_processamento}")
		print(files_to_process)

		if len(files_to_process) > 0:
			print(f"Files to push = {files_to_process}")
			df_boolean_mask = incoming_files_df["id"].isin(files_to_process)
			df_files_to_process = incoming_files_df[df_boolean_mask]

			xcom = df_files_to_process[["id", "name"]].to_records(index=False).tolist()
			context["ti"].xcom_push(key="images_to_process", value=xcom)

			self.invalidate_cache(local_cache_path)

			return True

		print("No files to push. Skipping task.")
		raise AirflowSkipException("No new files found in Google Drive.")

from airflow import DAG
from datetime import datetime
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator

with DAG(
    dag_id="multi_emr_jobs",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
):

    job1 = EmrServerlessStartJobOperator(
        task_id="job1",
        application_id="app-123",
        execution_role_arn="arn:aws:iam::123:role/emr-serverless-exec",
        job_driver={"sparkSubmit": {"entryPoint": "s3://bucket/job1.py"}},
    )

    job2 = EmrServerlessStartJobOperator(
        task_id="job2",
        application_id="app-123",
        execution_role_arn="arn:aws:iam::123:role/emr-serverless-exec",
        job_driver={"sparkSubmit": {"entryPoint": "s3://bucket/job2.py"}},
    )

    job1 >> job2




