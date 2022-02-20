from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DoubleType
import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def get_schema():
	schema = StructType([ \
		StructField("FIPS", IntegerType(), True), \
		StructField("Admin2", StringType(), True), \
		StructField("Province_State", StringType(), True), \
		StructField("Country_Region", StringType(), True), \
		StructField("Last_Update", StringType(), True), \
		StructField("Lat", DoubleType(), True), \
		StructField("Long_", DoubleType(), True), \
		StructField("Confirmed", IntegerType(), True), \
		StructField("Deaths", IntegerType(), True), \
		StructField("Recovered", IntegerType(), True), \
		StructField("Active", IntegerType(), True), \
		StructField("Combined_Key", StringType(), True), \
		StructField("Incident_Rate", DoubleType(), True), \
		StructField("Case_Fatality_Ratio", DoubleType(), True)
	  ])
	
	return schema
	
def get_input_stream_df(data_path):
	input_df = spark \
				.readStream \
				.schema(schema) \
				.csv(data_path)
				
	input_df.printSchema()
	return input_df


def main(spark):
	schema = get_schema()
	input_df = get_input_stream_df(schema, data_path)
	country_count = input_df.groupBy('Country_Region').sum('Confirmed').withColumnRenamed('sum(Confirmed)', 'Total_confirmed_cases_c')
	
	country_count.orderBy('Total_confirmed_cases').writeStream.option("numRows",10).format("console").outputMode("complete").start().awaitTermination()

   
if __name__ == '__main__':
    
    spark = SparkSession \
                .builder \
                .appName("covid-analysis-streaming") \
                .getOrCreate()
    
    # Getting arguments:
    input_date = sys.argv[1]
    data_path = sys.argv[2]
    
    print(f"input_date: {input_date}")
    print(f"Data Path: {data_path}")
    
    sys.exit(main())
    






groupDF = df.select("Zipcode").groupBy("Zipcode").count()





df.groupBy('Country_Region').sum('Confirmed').filter(df.Country_Region=='Australia').show(10, False)

.filter(df.Country_Region=='Australia')
df.groupBy('Country_Region').sum('Confirmed').withColumnRenamed('sum(Confirmed)', 'Total_confirmed_cases').orderBy(df.Total_confirmed_cases).show(10, False)








