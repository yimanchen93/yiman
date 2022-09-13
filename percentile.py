import pandas as pd
import argparse

#get input file and output file from command line arguments 
parser = argparse.ArgumentParser()
parser.add_argument('--input', type=str,required=True)
parser.add_argument('--output', type=str,required=True)
parser.add_argument('--percentile', type=float,required=True)
args = parser.parse_args()

#read parquet file and convert it to dataframe
df = pd.read_parquet(args.input)

#filter out dataframe records with data quality issue
df2 = df.query('passenger_count == passenger_count and passenger_count != 0.0 and trip_distance == trip_distance and trip_distance > 0.0 and tpep_pickup_datetime < tpep_dropoff_datetime and total_amount == total_amount and total_amount > 0.0 and VendorID == VendorID')

#filter dataframe with trip_distance greater than 0.9 percentile --> quantile = percentile/100 = 0.9/100=0.009
df_res = df2[ df2.trip_distance > df2.trip_distance.quantile(args.percentile/100)]

#convert column passenger_count to integer
df_res_ = df_res.copy()
df_res_['passenger_count'] = df_res_['passenger_count'].astype(int)

#write the result in an output file without index
df_res_.to_parquet(args.output, index = False)