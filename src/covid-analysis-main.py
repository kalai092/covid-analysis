from pyspark.sql import SparkSession
from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
import datetime
from datetime import datetime, timedelta
import sys


# function to get the date difference in specific format:
def date_from(days: int, input_date) -> str:
    return datetime.strftime(datetime.strptime(input_date, '%Y-%m-%d').date() + timedelta(days=days), '%Y-%m-%d')


# function to read 14 days of input data:
def read_input_data(data_path, input_date):
  input_df = spark \
			  .read \
			  .option('header', 'true') \
			  .option('inferSchema', 'true') \
			  .csv(data_path) \
			  .withColumn('date_id', F.date_format(F.col("Last_Update"), "yyyy-MM-dd"))
			  
  input_df = input_df \
			  .filter(((input_df.date_id<=input_date) & (input_df.date_id>=date_from(-14, input_date)))) \
			  .filter(input_df.Country_Region.isNotNull()) \
			  .filter(input_df.Province_State.isNotNull()) \
			  .select('Country_Region', 'Confirmed', 'Province_State', 'date_id') \
			  .withColumn('Confirmed' ,F.col('Confirmed').cast(IntegerType()))
  
  print('aggrigations will be performed for below dates')
  print(input_df.select('date_id').distinct().orderBy('date_id').show(14, False))
  return input_df.drop('date_id')


# function to get top 10 countries with decreasing positive cases in the last 14 days:
def get_top_10_country(input_df):
  top_10_country = input_df \
					.groupBy('Country_Region') \
					.sum('Confirmed') \
					.withColumnRenamed('sum(Confirmed)', 'Total_confirmed_cases') \
					.orderBy('Total_confirmed_cases') \
					.limit(10)
  
  return top_10_country


# function to get top 3 state per country(highest positive cases during the period):
def get_top_3_state_per_country(top_10_country, input_df):
  windowSpec = Window.partitionBy('Country_Region', 'Province_State')
  input_agg_df = input_df \
				.withColumn("Total_cases_state", F.sum(F.col("Confirmed")).over(windowSpec)) \
				.withColumn("state_r_no", F.row_number().over(windowSpec.orderBy(F.desc('Total_cases_state')))) \
				.filter(F.col('state_r_no')==1) \
				.withColumn("state_r_no", F.row_number().over(Window.partitionBy("Country_Region").orderBy(F.desc('Total_cases_state')))) \
				.filter(F.col('state_r_no')<=3)
  top_3_state = top_10_country \
			  .join(input_agg_df, ['Country_Region'], 'left')
  
  return top_3_state.drop('Confirmed')


def main(spark):
  
  input_df = read_input_data(data_path, input_date)
  top_10_country = get_top_10_country(input_df).persist()
  print('top 10 countries with decreasing positive cases in the last 14 days')
  top_10_country.show(10, False)

  top_3_state = get_top_3_state_per_country(top_10_country, input_df)
  print('top_3_state per each top country')
  top_3_state.orderBy('Total_confirmed_cases', 'state_r_no').show(30, False)

   
if __name__ == '__main__':
    
    spark = SparkSession \
                .builder \
                .appName("covid-analysis") \
                .getOrCreate()
    
    # Getting arguments:
    input_date = sys.argv[1]
    data_path = sys.argv[2]
    
    print(f"input_date: {input_date}")
    print(f"Data Path: {data_path}")
    
    sys.exit(main())
    
