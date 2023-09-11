from typing import List

from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datahub_provider.entities import Dataset


class Task:
    def __init__(self, project_name: str):
        """
        Airflow class, having functions to add k8s and emr serverless tasks

        Args:
            project_name: Project name or repository name
        """
        self.project_name = project_name

    def add_k8s_tasks(
        self,
        task_id: str,
        dag: str,
        task_group: str,
        zone_name: str,
        job_name: str,
        table_name: str,
        docker_image: str,
        env_vars: dict,
        dataset_platform_in: List[str],
        dataset_platform_out: List[str],
        table_platform_in: List[str],
        table_platform_out: List[str],
    ):
        """
        Add kubernetes task to execute python jobs

        Args:
            task_id: Unique id of te task within the dag
            dag: The dag name.
            task_group: Task group name. Collection of closely related tasks on the same DAG
            zone_name: The name of the zone, exemple: landing, bronze, silver, and gold
            job_name: The name of the Job, exemple: moreapp, diagnostico, sme
            table_name: The name of the table
            docker_image: The docker image to launch
            env_vars: A dictionary of environment variables for the Pod
            dataset_platform_in:
            dataset_platform_out:
            table_platform_in:
            table_platform_out:
        """
        task_inlets = [
            Dataset(dataset_in, platform_in)
            for (dataset_in, platform_in) in zip(dataset_platform_in, table_platform_in)
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
            arguments=["python", "main.py", job_name, zone_name, table_name],
            name=f"{self.project_name}-{zone_name}-{job_name}-{table_name}",
            random_name_suffix=True,
            env_vars=env_vars,
            in_cluster=True,
            reattach_on_restart=False,
            get_logs=True,
            image_pull_policy="Always",
            service_account_name="airflow-worker",
            log_events_on_failure=True,
            do_xcom_push=False,
            base_container_name=f"{self.project_name}-{zone_name}-{job_name}-{table_name}".replace("_", "-"),
            inlets=task_inlets,
            outlets=task_outlets,
        )

        return task

    def add_emr_serverless_tasks(
        self,
        task_id: str,
        dag: str,
        task_group: str,
        zone_name: str,
        job_name: str,
        table_name: str,
        application_id: str,
        s3_job_path: str,
        job_role_arn: str,
        default_monitoring_config: dict,
        env_vars: dict,
        dataset_platform_in: List[str],
        dataset_platform_out: List[str],
        table_platform_in: List[str],
        table_platform_out: List[str],
        timeout_seconds,
    ):
        """
        Add EMR serverless task to execute pyspark jobs

        Args:
            task_id: Unique id of te task within the dag
            dag: The dag name.
            task_group: Task group name. Collection of closely related tasks on the same DAG
            zone_name: The name of the zone, exemple: landing, bronze, silver, and gold
            job_name: The name of the Job, exemple: moreapp, diagnostico, sme
            table_name: The name of the table
            application_id: ID of the EMR Serverless application to start
            s3_job_path: Path where the python and pyspark scripts are loaded
            job_role_arn: ARN of role to perform action
            default_monitoring_config: Logs configuration. Path where the logs are saved
            env_vars: A dictionary of environment variables for the Pod
            dataset_platform_in:
            dataset_platform_out:
            table_platform_in:
            table_platform_out:
            timeout_seconds: Total amount of time, in seconds, the operator will wait for the job finish
        """
        task_inlets = [
            Dataset(dataset_in, platform_in)
            for (dataset_in, platform_in) in zip(dataset_platform_in, table_platform_in)
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
            "--packages io.delta:delta-core_2.12:2.3.0" + " "
            f"--conf spark.archives={s3_job_path}/pyspark_delta_package.tar.gz#environment" + " "
            "--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python" + " "
            "--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python" + " "
            "--conf spark.emr-serverless.executorEnv.PYSPARK_PYTHON=./environment/bin/python" + " "
        )

        emr_conf = extra_conf + spark_env_vars

        task = EmrServerlessStartJobOperator(
            dag=dag,
            task_id=task_id,
            task_group=task_group,
            application_id=application_id,
            name=f"{self.project_name}-{zone_name}-{job_name}-{table_name}".replace("_", "-"),
            execution_role_arn=job_role_arn,
            job_driver={
                "sparkSubmit": {
                    "entryPoint": f"{s3_job_path}/main.py",
                    "entryPointArguments": [job_name, zone_name, table_name],
                    "sparkSubmitParameters": f"--conf spark.submit.pyFiles={s3_job_path}/app.zip {emr_conf}",
                }
            },
            configuration_overrides=default_monitoring_config,
            inlets=task_inlets,
            outlets=task_outlets,
            waiter_countdown=timeout_seconds,
        )

        return task
