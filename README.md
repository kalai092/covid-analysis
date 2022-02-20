# Covid-19 Analysis

---

## About

1. src/covid-analysis-main.py

It's a batch processing script for Covid-19 Analysis
Arguments:
1. Input date: Date (yyyy-mm-dd) that we are going to process the data.
2. input data path: path to the input data set

eg.
spark-submit F:\\Cars.com\\covid-analysis\\src\\covid-analysis-main.py '2021-01-30' 'F:\\test\\COVID-19-master\\csse_covid_19_data\\csse_covid_19_daily_reports'

Note: Since we are doing aggregations based on the country and state we have only considered not-null rows of these two columns.


2. src/covid-analysis-main_streaming.py
It's a spark streaming script for Covid-19 analysis, it's a partial one. As of now, it will only return to the top 10 countries for the streaming data.

Arguments:

1. Input date: Date (yyyy-mm-dd) that we are going to process the data.
2. input data path: path to the input data set

