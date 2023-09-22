import re

from datetime import datetime
from datetime import timezone

import boto3

from loguru import logger
from pandas import DataFrame as PandasDataFrame
from pandas import read_excel


def get_bucket_name_from_zone_path(zone_path: str) -> str:
    """
    Get bucket from zone path

    Args:
        zone_path: Path where the tables are loaded
    """
    return zone_path.split("/")[2]


def get_folder_from_zone_path(zone_path: str) -> str:
    """
    Get folder from zone path

    Args:
        zone_path: Path where the tables are loaded
    """
    bucket_name = get_bucket_name_from_zone_path(zone_path)
    return zone_path.replace(f"s3a://{bucket_name}/", "")


def replace_camelcase_2_snakecase(name: str):
    """
    Replace column with camel case format to snake case format

    Args:
        name: Column name. Exemple: NomeColuna -> nome_coluna
    """
    return name[0].lower() + re.sub(r"(?!^)[A-Z]", lambda x: "_" + x.group(0).lower(), name[1:])


def fill_null_string_values(df: PandasDataFrame) -> PandasDataFrame:
    """

    Args:
        df:
    """
    df_cols = df.dtypes.to_frame("type")
    string_types = df_cols[df_cols["type"] == "string[pyarrow]"]

    replace_dict = {col: "" for col in string_types.index.to_list()}

    return df.fillna(replace_dict)


def fill_null_int_values(df: PandasDataFrame, int_constant: int) -> PandasDataFrame:
    """

    Args:
        df:
        int_constant:
    """
    df_cols = df.dtypes.to_frame("type")
    integer_types = df_cols[df_cols["type"] == "int64[pyarrow]"]

    replace_dict = {col: int_constant for col in integer_types.index.to_list()}

    return df.fillna(replace_dict)


def read_xlsx(file_path: str, sheet_name: str) -> PandasDataFrame:
    """

    Args:
        file_path:
        sheet_name:
    """

    logger.info(f"Reading sheet {sheet_name} from xlsx file from {file_path}")
    try:
        df = read_excel(file_path, sheet_name=sheet_name, dtype_backend="pyarrow")
        df = fill_null_string_values(df)
        return df
    except Exception as e:
        logger.error(f"Error trying to read dataframe from {file_path}")
        logger.error(e)
        raise e


def convert_pandas_df_to_list(df: PandasDataFrame) -> list:
    """

    Args:
        df:
    """
    return [tuple(array) for array in df.values]


def get_latest_file_name(bucket_name: str, prefix: str) -> str:
    # Inicialize um cliente do S3
    s3 = boto3.client("s3")

    # Inicialize a variável para armazenar o objeto mais recente
    latest_obj = None
    latest_time = datetime(1970, 1, 1, tzinfo=timezone.utc)  # data inicial

    # Liste todos os objetos no prefixo dado
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # Se existirem objetos no prefixo especificado
    if "Contents" in objects:
        # Iterar por cada objeto
        for obj in objects["Contents"]:
            # Verifique a data de criação (última modificação) do objeto
            if obj["LastModified"] > latest_time:
                latest_time = obj["LastModified"]
                latest_obj = obj

    # Se um objeto mais recente for encontrado, retorne o nome do arquivo
    if latest_obj:
        # O nome do arquivo é a parte da key depois do último '/'
        file_name = latest_obj["Key"].split("/")[-1]
        return file_name
    else:
        return None
