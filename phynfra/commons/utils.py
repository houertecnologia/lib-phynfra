import re

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
