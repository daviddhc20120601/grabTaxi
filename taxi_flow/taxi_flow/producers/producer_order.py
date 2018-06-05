# coding: utf-8
from boto import kinesis
import json
import pandas as pd
import time
import pprint
from conf.settings import *
from utils.tools import to_df


def main(order_or_vendor):

    order_df = to_df(producer_var[order_or_vendor]["order_df_file_path"])
    kinesis_put = kinesis.connect_to_region(producer_var[order_or_vendor]["kinesis_region"])

    order_df_json = order_df.to_dict(orient="records")

    for i in range(len(order_df_json)):
        if i > 0:
            pprint.pprint(i)
            kinesis_put.put_record(producer_var[order_or_vendor]["order_stream_name"], json.dumps(order_df_json[i]).encode("utf-8"), "partitionkey")
            sleep_time = (pd.to_datetime(order_df_json[i]['datetime'], format="%Y-%m-%d %H:%M:%S") - pd.to_datetime(
                order_df_json[i - 1]['datetime'], format="%Y-%m-%d %H:%M:%S")).total_seconds()
            time.sleep(sleep_time)
        else:
            kinesis_put.put_record(producer_var[order_or_vendor]["order_stream_name"], json.dumps(order_df_json[i]).encode("utf-8"), "partitionkey")


if __name__ == "__main__":
    main(order_or_vendor)
### python producer_order.py order
### python producer_order.py vendor