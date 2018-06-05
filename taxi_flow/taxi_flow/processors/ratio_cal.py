# coding: utf-8
from sqlalchemy import create_engine
import datetime
import pandas as pd

engine = create_engine("mysql+mysqlconnector://root:toor@localhost:3306/grab")
supply_ts_df = pd.read_sql_table("order_timeseries_live", engine, index_col="index")
demand_ts_df = pd.read_sql_table("vendor_timeseries_live", engine, index_col="index")
taxi_zone_lookup = pd.read_csv("data/taxi_zone_lookup.csv", index_col="Unnamed: 0")


def ratio(supply_df, demand_df, current_time):
    ans = []

    for i in range(256):
        ans_i = {}
        supply_amount = len(supply_df[(supply_df["locationID"] == i) &
                                      (supply_df["datetime"] < current_time) &
                                      (supply_df["datetime"] > (current_time - pd.Timedelta(minutes=1000)))])
        demand_amount = float(len(demand_df[(demand_df["locationID"] == i) &
                                            (demand_df["datetime"] < current_time) &
                                            (demand_df["datetime"] > (current_time - pd.Timedelta(minutes=1000)))]))
        # print ("supply_amount:%d,demand_amount:%d"%(supply_amount,demand_amount))

        if demand_amount > 0:
            ans_i["SD_ratio"] = supply_amount / demand_amount
            # print (i,ans_datetime["SD_ratio_%.3d"%i],supply_amount,demand_amount)
        else:
            ans_i["SD_ratio"] = 0.0
        ans_i["datetime"] = current_time
        ans_i["locationID"] = i
        ans.append(ans_i)
    ans_datetime_df = pd.DataFrame(ans)
    ans_datetime_df = ans_datetime_df.join(taxi_zone_lookup, on='locationID', how="left", lsuffix="_1")
    # print (ans_datetime_df)
    ans_datetime_df.to_sql(con=engine, name="sd_ratio_timeseries_live", if_exists="append", chunksize=1000)
    print("curr:%s"%current_time.isoformat())
    return ans_datetime_df


def main():
    init_time = datetime.datetime(2017, 1, 1, 0, 0, 0)
    now_time = init_time
    while True:
        now_time += datetime.timedelta(hours=1)
        ratio(supply_ts_df, demand_ts_df, now_time)


if __name__ == "__main__":
    main()
