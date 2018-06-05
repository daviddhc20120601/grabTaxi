producer_var = {
    "order": {
        "order_df_file_path": "data/order_timeseries.csv",
        "kinesis_region": "ca-central-1",
        "order_stream_name": "xuqiu"
    },
    "vendor": {
        "order_df_file_path": "data/vendor_timeseries.csv",
        "kinesis_region": "ca-central-1",
        "order_stream_name": "gongying"
    },
}

consumer_var = {
    "order":{
        "sql_table_name":"order_timeseries_live",
        "order_stream_name": "xuqiu"
    },
    "vendor":{
            "sql_table_name":"vendor_timeseries_live",
            "order_stream_name": "gongying"
        }
}

kinesis_region = "ca-central-1"