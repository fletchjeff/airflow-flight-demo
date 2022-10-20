from typing import Any
from airflow.models.xcom import BaseXCom
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

#import modin.pandas as pd
import pandas as pd
import uuid

class ModinXComBackend(BaseXCom):
    PREFIX = "xcom_s3://"
    BUCKET_NAME = "jf-xcom"

    @staticmethod
    def serialize_value(value: Any):
        if isinstance(value, pd.DataFrame):

            hook        = S3Hook()
            key         = "data_" + str(uuid.uuid4())
            filename    = f"{key}.parquet.gzip"

            value.to_parquet(filename,compression="gzip")
            hook.load_file(
                filename=filename,
                key=key,
                bucket_name=ModinXComBackend.BUCKET_NAME,
                replace=True
            )
            value = ModinXComBackend.PREFIX + key
        return BaseXCom.serialize_value(value)

    @staticmethod
    def deserialize_value(result) -> Any:
        result = BaseXCom.deserialize_value(result)
        if isinstance(result, str) and result.startswith(ModinXComBackend.PREFIX):
            hook    = S3Hook()
            key     = result.replace(ModinXComBackend.PREFIX, "")
            filename = hook.download_file(
                key=key,
                bucket_name=ModinXComBackend.BUCKET_NAME,
                local_path="/tmp"
            )
            result = pd.read_parquet(filename)
        return result