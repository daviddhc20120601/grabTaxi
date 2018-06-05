# coding: utf-8
from boto import kinesis
import time
import json
import pprint
import pandas as pd
from sqlalchemy import create_engine
from conf.settings import *



def main(order_or_vendor):

    engine = create_engine("mysql+mysqlconnector://root:toor@localhost:3306/grab")

    taxi_zone_lookup = pd.read_csv("data/taxi_zone_lookup.csv", index_col="Unnamed: 0")

    def timeseries2loc(msg):
        ts_df = pd.DataFrame([json.loads(i["Data"]) for i in a["Records"]])
        print(ts_df)
        order_timeseries_with_gps = ts_df.join(taxi_zone_lookup, on='locationID', how="left", lsuffix="_1")
        order_timeseries_with_gps["datetime"] = pd.to_datetime(order_timeseries_with_gps["datetime"])
        print(order_timeseries_with_gps)
        order_timeseries_with_gps.to_sql(con=engine, name=consumer_var[order_or_vendor]["sql_table_name"],
                                         if_exists="append", chunksize=1000)
        return order_timeseries_with_gps

    kinesis_get = kinesis.connect_to_region(kinesis_region)
    shard_id = 'shardId-000000000000'
    shard_it = kinesis_get.get_shard_iterator(consumer_var[order_or_vendor]["order_stream_name"], shard_id, "LATEST")[
        "ShardIterator"]
    while True:
        a = kinesis_get.get_records(shard_it)
        # print (a)
        shard_it = a["NextShardIterator"]
        print("get %d records" % len(a["Records"]))
        if len(a["Records"]) > 0:
            pprint.pprint(a["Records"][0])
            timeseries2loc(a)
        # print (">>>>>next >>>>  \t"+shard_it)
        time.sleep(2)


if __name__ == "__main__":
    main(order_or_vendor)
