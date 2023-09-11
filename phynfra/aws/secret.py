from boto3 import Session
from botocore.exceptions import ClientError
from loguru import logger


def get_secretsmanager_secret(boto3_session: Session, secret_name: str) -> str:
    """
    Get secret from AWS secret manager

    Args:
        boto3_session: Boto3 session
        secret_name: Name of secret
    """
    secrets_manager_client = boto3_session.client(service_name="secretsmanager")

    try:
        get_secret_value_response = secrets_manager_client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logger.error(f"Error trying to get secret {secret_name}")
        logger.error(e)
        raise e

    return get_secret_value_response["SecretString"]
