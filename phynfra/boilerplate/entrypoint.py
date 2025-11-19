import argparse
import importlib
import json
import traceback
import sys

REGEX_MODULE = r'[0-9a-zA-Z_]+(\.[0-9a-zA-Z_]+)*'

from phynfra.__main__ import run

def main ():
	'''
	Default main python function from phynfra to be the 
	entrypoint for Spark Submit job invokation
	This is the same of calling --command run from phynfra in command line, except without --configuration
	due to spark environment. This is almost the same of __main__ from pyhnfra, but adaptable to spark
	NOTE: the os.environ IS the --spark.(driver|executor)Env.X=y - Every var sete in spark-submit will be visible here
	RECOMENDATION: These vars should be in Airflow as "Variable" or secrets manager.

	docker run -d --name spark -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION --network lordshark apache/spark:3.5.7 sleep infinity

	spark-submit s3://your-bucket/wheels/yourproject.whl --entrypoint entrypoint.py --module test --settings s3://.../settings.json --other args...

	aws emr-serverless start-job-run --application-id <app-id> --execution-role-arn arn:aws:iam::<account>:role/emr-serverless-exec-role --job-driver '{"sparkSubmit": {"entryPoint": "s3://my-artifacts/jobs/main_etl.py", "sparkSubmitParameters": "--py-files s3://my-artifacts/project-0.1.0-py3-none-any.whl"}}' --name "test-emr-job"

	EMRServerlessOperator(
		task_id="run_test",
		entry_point="entrypoint.py",
		entry_point_arguments=[
			"--module", "test",
			"--settings", '{"env":"prod"}'
		],
		spark_submit_params=[
			"--py-files", "s3://bucket/wheels/yourproject.whl"
			"--conf", f"spark.executorEnv.{k}={v}",
			"--conf", f"spark.driverEnv.{k}={v}",
		],
		...
	)
	'''
	
	parser = argparse.ArgumentParser()

	parser.add_argument("--module", required = True)
	parser.add_argument('--extra', nargs = '*', default = [])

	arguments = parser.parse_args()

	if re.fullmatch(REGEX_MODULE, arguments.module):

		configuration = dict(os.environ)

		extra = {}

		try:

			for pair in arguments.extra:

				key, value = pair.split('=', 1)

				extra[key] = value

		except Exception as parseError:

			print('[PHYNFRA ENTRYPOINT] Fail to execute phynfra bootstrap - Failures in extra data. Check the attributes. The pattern is key=value. Error: %s' % str(parseError))

			sys.exit(1)

		try:

			run(configuration = configuration, module = arguments.module, extra = extra)

			sys.exit(0)

		except Exception as phynfraError:
			
			print('[PHYNFRA ENTRYPOINT] Internal execution Error: %s' % str(phynfraError))

			print(traceback.format_exc())
			
			sys.exit(1)

	else:

		print('[PHYNFRA ENTRYPOINT] %s is not a valid python module dot run callable function!' % arguments.module)
	
		sys.exit(1)

if __name__ == "__main__":
	
	main()
