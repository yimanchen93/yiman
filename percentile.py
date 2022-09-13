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
df = df.query('passenger_count == passenger_count and passenger_count != 0.0 and trip_distance == trip_distance and trip_distance > 0.0 and tpep_pickup_datetime < tpep_dropoff_datetime and total_amount == total_amount and total_amount > 0.0 and VendorID == VendorID')

#filter dataframe with trip_distance greater than 0.9 percentile --> quantile = percentile/100 = 0.9/100=0.009
df = df[ df.trip_distance > df.trip_distance.quantile(args.percentile/100)]

#convert column passenger_count to integer
df_res = df.copy()
df_res['passenger_count'] = df_res['passenger_count'].astype(int)

#write the result in an output file without index
df_res.to_parquet(args.output, index = False)