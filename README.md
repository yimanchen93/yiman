
PROBLEM:
Using NYC “Yellow Taxi” Trips Data, return all the trips over 0.9 percentile in distance traveled for any of the Parquet files you can find there.
used test file: yellow_tripdata_2022-01.parquet

USED PACKAGE: 
pandas, the reason that I have chosen pandas is because it is an open source library in python for data analysis and manipulation,in my case I have to analyze data from a parquet file and filter out records that meet the problem´s condition, and parquet is a column-oriented file, unlike csv,txt..(row-based formats), it cannot be opened with python build-in function open(), also it is very efficient to filter colums or rows without having to iterate records in a loop, which makes the process of the data faster. The other advantage is that in less code you can accomplish the same work thanks to pandas dataframe functions like query() to filter, quantile() to calculate percentile.

DATA QUALITY CHECK:
In the parquet file there are in total 19 columns and 2463931 rows, and the following columns have Data quality issues:
- passenger_count should be greater than 0 and distinct to null, and the column type should be integer, as we can not have a trip without any passenger or 1.5 passengers.
- trip_distance and total_amount should be greater than 0.0 and distinct to null, every trip should have a distance and the passenger should pay to the taxi driver.
- tpep_dropoff_datetime should be greater than tpep_pickup_datetime and both distinct to null, it is impossible that a trip has pickup datetime > dropoff datetime.
- VendorID should be distinct to null, all the taxis should have its own vendor.

SOLUTION:
** The steps I have followed in my code:
1. read parquet file
2. filter out data that has data quality issues
3. use df.column_name.quantile() to calculate trips over 0.9 percentile in distance traveled, quantile_num = percentile/100 = 0.9/100=0.009  --> dataframe.trip_distance.quantile(quantile_num)
4. filter out records with percentile over 0.9 in the result dataframe
5. write the result dataframe in an output parquet file.

** before test the code be sure you have pandas and python with version >= 3.7 installed by running 'pip show pandas' and 'python -V'.

** To reproduce the test run the following command:
python percentile.py --input input_filename.parquet --output output_filename.parquet --percentile 0.9
example: python percentile.py --input yellow_tripdata_2022-01.parquet --output test.parquet --percentile 0.9