import os
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.window import Window
import json

def trade_volume_aggregator(df):
    #TODO: create agg table
     window=Window.partitionBy('partition','symbol')
     windowOpen=Window.partitionBy('partition','symbol').orderBy('trade_timestamp')
     windowClose=Window.partitionBy('partition','symbol').orderBy(col('trade_timestamp').desc())
     df= df.withColumn('trade_date', to_date(col('trade_timestamp')))
     df=df.withColumn('open_price',first(col('open')).over(windowOpen))
     df=df.withColumn('close_price',first(col('close')).over(windowClose))
     df=df.withColumn('total_volume',sum(col('volume')).over(window))
     df=df.withColumn('daily_high',max(col('high')).over(window))
     df=df.withColumn('daily_low',max(col('low')).over(window))
     df=df.drop('open','close','low','high','volume','trade_timestamp','batch_id')
     batch_id=current_timestamp().cast("long")
     df = df.withColumn("batch_id", lit(batch_id))
     df=df.select('*').distinct()
     return df


def stock_events_normalizer(data: dict, spark: SparkSession):
    """
    take stock events and process into df!
    """
    symbol = data["Meta Data"].get('2. Symbol', 'N/A')

    # Extract time series data, 2nd element of the dict
    body = data.get(list(data.keys())[1])

    enrich_events = []
    for date, dict_value in body.items():
        dict_value['symbol'] = dict_value.get('symbol',symbol)
        dict_value['trade_timestamp'] = dict_value.get('trade_timestamp',date)
        enrich_events.append(dict_value)
       
    df_raw = spark.createDataFrame(enrich_events)
    # type casting
    df = df_raw.select(
        df_raw["trade_timestamp"].cast(TimestampType()).alias("trade_timestamp"),
        df_raw["symbol"].cast(StringType()).alias("symbol"),
        df_raw["`1. open`"].cast(DoubleType()).alias("open"),
        df_raw["`2. high`"].cast(DoubleType()).alias("high"),
        df_raw["`3. low`"].cast(DoubleType()).alias("low"),
        df_raw["`4. close`"].cast(DoubleType()).alias("close"),
        df_raw["`5. volume`"].cast(IntegerType()).alias("volume")
    )

    batch_id=current_timestamp().cast("long")
    df = df.withColumn("partition", to_date("trade_timestamp").cast("string"))
    df = df.withColumn("batch_id", lit(batch_id))
     # Show the DataFrame for testing
    # df.show()
    return df
