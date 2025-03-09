#%%
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import sum, count, desc
import pandas as pd


spark = SparkSession.builder.appName('lowa analysis - know data').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+



#%%
#load data
#set schema
sales_schema = types.StructType([
    types.StructField('Store Name', types.StringType(), True),
    types.StructField('Invoice/Item Number', types.StringType(), True),
    types.StructField('Date', types.DateType(), True),
    types.StructField('Store Number', types.IntegerType(), True),
    types.StructField('Address', types.StringType(), True),
    types.StructField('City', types.StringType(), True),
    types.StructField('Zip Code', types.StringType(), True),
    types.StructField('Store Location', types.StringType(), True),
    types.StructField('County', types.StringType(), True),
    types.StructField('Category', types.IntegerType(), True),
    types.StructField('Category Name', types.StringType(), True),
    types.StructField('Vendor Number', types.IntegerType(), True),
    types.StructField('Vendor Name', types.StringType(), True),
    types.StructField('Item Number', types.StringType(), True),
    types.StructField('Item Description', types.StringType(), True),
    types.StructField('Pack', types.IntegerType(), True),
    types.StructField('Bottle Volume (ml)', types.DoubleType(), True),
    types.StructField('State Bottle Cost', types.DoubleType(), True),
    types.StructField('State Bottle Retail', types.DoubleType(), True),
    types.StructField('Bottles Sold', types.DoubleType(), True),
    types.StructField('Sale (Dollars)', types.DoubleType(), True),
    types.StructField('Volume Sold (Liters)', types.DoubleType(), True),
    
])

inputs = 'Final_cleaned_liquor_sales'
data = spark.read.csv(inputs, header=True, schema=sales_schema, inferSchema=True)

population = 'population_by_county.csv'
data_population = spark.read.csv(population, header=True, inferSchema=True)


#%%
#analysis data
#Know about the data time range
date_range = data.agg(
    functions.min('Date').alias('Earliest Date'),
    functions.max('Date').alias('Latest Date')
).show()


#%%

#basic statistics
data.describe([
    'Pack',
    'Bottle Volume (ml)',
    'State Bottle Cost',
    'State Bottle Retail',
    'Bottles Sold',
    'Sale (Dollars)',
    'Volume Sold (Liters)'
]).toPandas().to_csv('data_description.csv', index=False)