# coding: utf-8
import pandas as pd
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import numpy as np
import scipy.stats as ss
from sqlalchemy import create_engine

engine = create_engine("mysql+mysqlconnector://root:toor@localhost:3306/grab")
taxi_zone_lookup = pd.read_csv("data/taxi_zone_lookup.csv", index_col="Unnamed: 0")

vendor_timeseries_df = pd.read_csv("data/vendor_timeseries_with_driverID.csv")
vendor_timeseries_df["datetime"] = pd.to_datetime(vendor_timeseries_df["datetime"])


def main(vendor_timeseries_df):
    def get_cong_serviety_list(x1, x2, K):  ##x1 time cost, x2 distance, k-number of centroids
        X = np.array(list(zip(x1, x2))).reshape(len(x1), 2)
        kmeans_model = KMeans(n_clusters=K).fit(X)
        centers = kmeans_model.cluster_centers_
        labels = kmeans_model.labels_
        mapping_list = ss.rankdata([(i[1] / i[0]) for i in centers]) * 100
        mapping_dict = dict(zip(range(len(mapping_list)), mapping_list))
        return [mapping_dict[i] for i in labels]

    def get_loc_time(vendor_timeseries_df):
        time_cost_list = []
        for car_id in range(0, 11):
            time_cost = {}
            try:
                (loc_previous, loc_now) = (
                    vendor_timeseries_df[vendor_timeseries_df["carID"] == car_id].locationID.iloc[-2],
                    vendor_timeseries_df[vendor_timeseries_df["carID"] == car_id].locationID.iloc[-1])
                (time_previous, time_now) = (
                    vendor_timeseries_df[vendor_timeseries_df["carID"] == car_id].datetime.iloc[-2],
                    vendor_timeseries_df[vendor_timeseries_df["carID"] == car_id].datetime.iloc[-1])
                time_cost["loc_previous"] = loc_previous
                time_cost["loc_now"] = loc_now
                time_cost["time_cost"] = (time_now - time_previous).total_seconds()
                time_cost["time_previous"] = time_previous
                time_cost["time_now"] = time_now
                time_cost["car_id"] = car_id
                time_cost_list.append(time_cost)
            except IndexError:
                continue
        return time_cost_list

    def congestions(vendor_timeseries_df):
        cong_df = pd.DataFrame(get_loc_time(vendor_timeseries_df))
        cong_df["locationID"] = cong_df["loc_now"]
        cong_df["distance"] = cong_df["loc_previous"] - cong_df["loc_now"]
        cong_df["servity"] = get_cong_serviety_list(cong_df["time_cost"], cong_df["distance"], 5)
        cong_df = cong_df.join(taxi_zone_lookup, on='locationID', how="left", lsuffix="_1")
        cong_df.to_sql(con=engine, name="congestion_df", if_exists="append", chunksize=1000)
        return cong_df


if __name__ == "__main__":

    for i in range(100, 1000, 100):
        cong_df = main(vendor_timeseries_df[:i])
        print(cong_df[:2])


# cong_df.to_sql(con=engine,name="congestion_df",if_exists="append",chunksize=1000)
