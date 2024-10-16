from etl_utils import ingest_data_to_delta, read_data_from_delta, deduplication,run_data_validations, setup_spark_session
from transform import trade_volume_aggregator,stock_events_normalizer
from etl_utils import read_data_from_delta
from api_utils.api_factory import APIHandler
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import *
from pyspark.sql import DataFrame, SparkSession

nvd_params = {
    'function': 'TIME_SERIES_INTRADAY',
    'symbol': 'NVDA',
    'interval': '1min',
    'outputsize': 'full',
}

company_params = [
    {
    'function': 'TIME_SERIES_INTRADAY',
    'symbol': 'NVDA',
    'interval': '1min',
    'outputsize': 'full',
    },
    {
    'function': 'TIME_SERIES_INTRADAY',
    'symbol': 'TSLA',
    'interval': '1min',
    'outputsize': 'full',
    },
    {
    'function': 'TIME_SERIES_INTRADAY',
    'symbol': 'IBM',
    'interval': '1min',
    'outputsize': 'full',
    },
    {
    'function': 'TIME_SERIES_INTRADAY',
    'symbol': 'AAPL',
    'interval': '1min',
    'outputsize': 'full',
    },
]

# entry point for calling stock-etl during deployment
if __name__ == '__main__':
    spark = setup_spark_session()
    # Extract -> To Bronze (data lake staging zone)
    for param in company_params:
        trade_api = APIHandler(request_params=param)
        api_endpoint = trade_api.get_endpoint()
        data = trade_api.request_data(api_endpoint)
        df = stock_events_normalizer(data, spark)
        ingest_data_to_delta(df, 'trade')
    
    # read from bronze
    df_bronze=read_data_from_delta('trade',spark)
    # transform to agg
    df_sliver=trade_volume_aggregator(df_bronze)
    # load to silver
    ingest_data_to_delta(df_sliver,'daily_agg')
