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


spark = SparkSession.builder.appName('lowa analysis - popular').getOrCreate()
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
data = spark.read.csv(inputs, header=True, inferSchema=True)

population = 'population_by_county.csv'
data_population = spark.read.csv(population, header=True, inferSchema=True)


#%%
#analysis data

#%%
#Analysis of population data during ten years
#The most popular ten vendors


# 1. The most popular ten vendors (based on sales volume)
top_vendors = data.groupBy('Vendor Name') \
    .agg(
        sum('Bottles Sold').alias('Total_Bottles_Sold'),
        sum('Sale (Dollars)').alias('Total_Sales')
    ) \
    .orderBy(desc('Total_Bottles_Sold')) \
    .limit(10) \
    .toPandas()

# 2. The most popular ten items
top_items = data.groupBy('Item Description') \
    .agg(
        sum('Bottles Sold').alias('Total_Bottles_Sold'),
        sum('Sale (Dollars)').alias('Total_Sales')
    ) \
    .orderBy(desc('Total_Bottles_Sold')) \
    .limit(10) \
    .toPandas()

# 3. The most popular ten stores
top_stores = data.groupBy('Store Name', 'City') \
    .agg(
        sum('Bottles Sold').alias('Total_Bottles_Sold'),
        sum('Sale (Dollars)').alias('Total_Sales')
    ) \
    .orderBy(desc('Total_Bottles_Sold')) \
    .limit(10) \
    .toPandas()

# 4. The most popular ten cities
top_counties = data.groupBy('City') \
    .agg(
        sum('Bottles Sold').alias('Total_Bottles_Sold'),
        sum('Sale (Dollars)').alias('Total_Sales')
    ) \
    .orderBy(desc('Total_Bottles_Sold')) \
    .limit(10) \
    .toPandas()

# Save results to different sheets in Excel file
with pd.ExcelWriter('liquor_sales_analysis.xlsx', engine='xlsxwriter') as writer:
    top_vendors.to_excel(writer, sheet_name='Top Vendors', index=False)
    top_items.to_excel(writer, sheet_name='Top Items', index=False)
    top_stores.to_excel(writer, sheet_name='Top Stores', index=False)
    top_counties.to_excel(writer, sheet_name='Top Counties', index=False)

# Visualization
import matplotlib
matplotlib.use('Agg') 

import matplotlib.pyplot as plt
import seaborn as sns

# Set style using matplotlib's built-in style
plt.style.use('ggplot')  

# Create figure and subplots
fig, axes = plt.subplots(2, 2, figsize=(20, 16))
fig.suptitle('Iowa Liquor Sales Analysis', fontsize=16)

# 1. Top Vendors Plot
sns.barplot(data=top_vendors, y='Vendor Name', x='Total_Bottles_Sold', ax=axes[0,0], color='skyblue')
axes[0,0].set_title('Top 10 Vendors by Sales Volume')
axes[0,0].set_xlabel('Bottles Sold')
axes[0,0].set_ylabel('Vendor Name')

# 2. Top Items Plot
sns.barplot(data=top_items, y='Item Description', x='Total_Bottles_Sold', ax=axes[0,1], color='lightgreen')
axes[0,1].set_title('Top 10 Popular Items')
axes[0,1].set_xlabel('Bottles Sold')
axes[0,1].set_ylabel('Item Description')

# 3. Top Stores Plot
sns.barplot(data=top_stores, y='Store Name', x='Total_Bottles_Sold', ax=axes[1,0], color='salmon')
axes[1,0].set_title('Top 10 Stores by Sales Volume')
axes[1,0].set_xlabel('Bottles Sold')
axes[1,0].set_ylabel('Store Name')

# 4. Top Cities Plot
sns.barplot(data=top_counties, y='City', x='Total_Bottles_Sold', ax=axes[1,1], color='lightcoral')
axes[1,1].set_title('Top 10 Cities by Sales Volume')
axes[1,1].set_xlabel('Bottles Sold')
axes[1,1].set_ylabel('Cities Name')

# Adjust layout
plt.tight_layout(rect=[0, 0.03, 1, 0.95])

# Save plots to file and remove plt.show()
plt.savefig('sales_analysis.png', dpi=300, bbox_inches='tight')
plt.close()  # Close graph, release memory

# Save data to Excel
with pd.ExcelWriter('liquor_sales_analysis.xlsx', engine='xlsxwriter') as writer:
    top_vendors.to_excel(writer, sheet_name='Top Vendors', index=False)
    top_items.to_excel(writer, sheet_name='Top Items', index=False)
    top_stores.to_excel(writer, sheet_name='Top Stores', index=False)
    top_counties.to_excel(writer, sheet_name='Top Counties', index=False)

#population data analysis
# population = 'population_by_county.csv'
# data_population = spark.read.csv(population, header=True, inferSchema=True)
spark.stop()
# %%
