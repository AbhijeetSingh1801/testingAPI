from dataclasses import dataclass
import arrow
from dags.utils.pandas import Pandas
from dags.utils.s3 import S3
from dagster import AssetExecutionContext

from .client import DevXClient


@dataclass
class DevXConfig:
    token: str


@dataclass
class DevXToS3:
    config: DevXConfig
    s3_client: S3 = None
    pandas_client: Pandas = None

    def __post_init__(self):
        if self.s3_client is None:
            self.s3_client = S3()
        if self.pandas_client is None:
            self.pandas_client = Pandas()
        assert self.config.token, "Devx Config auth token is required"

    def extract_and_load_data(
        self,
        context: AssetExecutionContext,
        start_date: arrow.Arrow,
        end_date: arrow.Arrow,
        s3_bucket: str,
        s3_prefix: str,
        filters: dict = None,
    ) -> dict:
        context.log.debug("DevxToS3: Starting data extraction")
        
        api_client = DevXClient(token=self.config.token, context=context)

        params = {
            "created_at[$gte]": start_date.isoformat(),
            "created_at[$lte]": end_date.isoformat(),
        }
        df = api_client.extract_orders_data(
            params=params,
        )

        if df.empty:
            context.log.warning("DevxToS3: No data found in given range")
        else:
            context.log.debug(f"DevxToS3: Retrieved {len(df)} rows")

        local_path = self.pandas_client.to_csv(
            df,
            path="/tmp/devx_data.csv",
        )

        self.s3_client.upload_file(
            local_path=local_path,
            bucket=s3_bucket,
            key=s3_prefix,
        )

        context.log.debug("DevxToS3: Upload to S3 complete")
        return {
            "total_rows_processed": len(df),
        }
