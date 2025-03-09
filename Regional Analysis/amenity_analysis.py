import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col, count, min, max, sum, avg, when, year, datediff, round
from pyspark.sql.functions import trim, countDistinct, udf, upper, regexp_replace
import pandas as pd
import matplotlib.pyplot as plt

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
    types.StructField('Bottles Sold', types.IntegerType(), True),
    types.StructField('Sale (Dollars)', types.DoubleType(), True),
    types.StructField('Volume Sold (Liters)', types.DoubleType(), True)
])

amenity_schema = types.StructType([
    types.StructField('Store Name', types.StringType(), True),
    types.StructField('amenity', types.StringType(), True),
    types.StructField('leisure', types.StringType(), True),
    types.StructField('shop', types.StringType(), True),
    types.StructField('count', types.IntegerType(), True)
])
def plot_scatter(stat_data, x_column_name, y_column_name):
    
    stat_pd = stat_data.select(x_column_name, y_column_name).toPandas()

    plt.figure(figsize=(8, 6))
    plt.scatter(stat_pd[x_column_name], stat_pd[y_column_name], alpha=0.7, edgecolors='k')

    plt.title(f"Relationship Between {y_column_name} and {x_column_name}", fontsize=14)
    plt.xlabel(f"{x_column_name} within 2km", fontsize=12)
    plt.ylabel(y_column_name, fontsize=12)

    plt.show()

def plot_barchart(stat_data, x_column_name, y_column_name, bins, labels):
    
    stat_pd = stat_data.select(x_column_name, y_column_name).toPandas()

    stat_pd["Category"] = pd.cut(stat_pd[x_column_name], bins=bins, labels=labels, figsize=(6, 6))
    # Calculate the average and standard deviation for each category
    category_stats = stat_pd.groupby("Category")[y_column_name].agg(['mean', 'std']).reset_index()
    category_stats.columns = ["Category", "Mean", "StdDev"]

    # Plot the bar chart with error bars
    plt.figure(figsize=figsize)
    plt.bar(
        category_stats["Category"],
        category_stats["Mean"],
        yerr=category_stats["StdDev"],  # Error bars
        capsize=5,  # Size of the error bar caps
        alpha=0.7,
        edgecolor="k"
    )
    plt.title(f"{y_column_name} by {x_column_name}", fontsize=12)
    plt.xlabel(x_column_name, fontsize=10)
    plt.ylabel(f"Average {y_column_name}", fontsize=10)
    plt.xticks(rotation=45)
    plt.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.show()

def plot_barchart_remove_outliers(stat_data, x_column_name, y_column_name, bins, labels, figsize=(6, 6)):
    
    stat_pd = stat_data.select(x_column_name, y_column_name).toPandas()

    # Outlier removal using IQR method
    factor = 1.5
    Q1_x = stat_pd[x_column_name].quantile(0.25)
    Q3_x = stat_pd[x_column_name].quantile(0.75)
    IQR_x = Q3_x - Q1_x

    Q1_y = stat_pd[y_column_name].quantile(0.25)
    Q3_y = stat_pd[y_column_name].quantile(0.75)
    IQR_y = Q3_y - Q1_y

    # Define the lower and upper bounds for x and y
    lower_x = Q1_x - factor * IQR_x
    upper_x = Q3_x + factor * IQR_x
    lower_y = Q1_y - factor * IQR_y
    upper_y = Q3_y + factor * IQR_y

    if lower_x > 0:
        lower_x = 0

    if upper_x < 1:
        upper_x = 1

    # Filter out outliers
    stat_pd = stat_pd[
        (stat_pd[x_column_name] >= lower_x) & (stat_pd[x_column_name] <= upper_x) &
        (stat_pd[y_column_name] >= lower_y) & (stat_pd[y_column_name] <= upper_y)
    ]

    stat_pd["Category"] = pd.cut(stat_pd[x_column_name], bins=bins, labels=labels)
    # Calculate the average and standard deviation for each category
    category_stats = stat_pd.groupby("Category")[y_column_name].agg(['mean', 'std']).reset_index()
    category_stats.columns = ["Category", "Mean", "StdDev"]

    # Plot the bar chart with error bars
    plt.figure(figsize=figsize)
    plt.bar(
        category_stats["Category"],
        category_stats["Mean"],
        yerr=category_stats["StdDev"],  # Error bars
        capsize=5,  # Size of the error bar caps
        alpha=0.7,
        edgecolor="k",
        width=0.6
    )
    plt.title(f"{y_column_name} by {x_column_name}", fontsize=12)
    plt.xlabel(x_column_name, fontsize=10)
    plt.ylabel(f"Average {y_column_name}", fontsize=10)
    plt.xticks(rotation=45)
    plt.grid(axis="y", alpha=0.3)
    plt.tight_layout()

    plt.savefig(f"{y_column_name} by {x_column_name}.png", dpi=300, bbox_inches='tight')

    plt.show()

def main(liquor, amenity):

    # ======================== Read Data =======================================================
    # the cleaned version of Iowa liquor Sales data
    ori_data = spark.read.csv(liquor, header=True, schema=sales_schema)

    amenity_data = spark.read.csv(amenity, header=True, schema=amenity_schema)
    
    groupped_amenity = amenity_data.groupBy("Store Name").agg(\
        sum(when((col("amenity")=="restaurant") | (col("amenity")=="cafe"), col("count")).otherwise(0)).alias("Number of Restaurants"),\
        sum(when((col("amenity")=="bar") | (col("amenity")=="pub"), col("count")).otherwise(0)).alias("Number of Bars"),\
        count(when(col("leisure")!="None", 1)).alias("Number of Stadium"),\
        sum(when(col("shop")!="None", col("count")).otherwise(0)).alias("Number of Supermarkets")\
        )

    # groupped_amenity.orderBy("Store Name").write.csv("groupped_amenity.csv", header=True, mode="overwrite")

    # filter only records in POLK county, and aggregate data by store
    # we also find the earlist date and the latest date of records for each store,
    # so that we can determine approximately how many years this store has been opening
    store_data = ori_data.filter(col("County")=="POLK")\
    .groupBy("Store Name").agg(\
        count(col("Invoice/Item Number")).alias("Number of Transactions"),
        sum(col("Sale (Dollars)")).alias("Total Sales"),
        sum(col("Bottles Sold")).alias("Total Bottles"),
        sum(col("Volume Sold (Liters)")).alias("Total Volume"),
        min("Date").alias("Min Date"),
        max("Date").alias("Max Date")
        )

    # filter out stores that has opened for less than a year
    store_data = store_data.withColumn("Number of Opening Years", \
        round(datediff(col("Max Date"), col("Min Date")) / 365.0))\
    .filter(col("Number of Opening Years")>0)

    # add columns that normalize the data
    store_data = store_data.\
    withColumn("Sales per Transaction", col("Total Sales") / col("Number of Transactions"))\
    .withColumn("Volume per Transaction", col("Total Volume") / col("Number of Transactions"))\
    .withColumn("Transactions per Year", col("Number of Transactions") / col("Number of Opening Years"))\
    .withColumn("Sales per Year", col("Total Sales") / col("Number of Opening Years"))\
    .withColumn("Volume Sold per Year", col("Total Volume") / col("Number of Opening Years"))

    store_data = store_data.select("Store Name", "Transactions per Year", "Sales per Year",
        "Volume Sold per Year", "Sales per Transaction", "Volume per Transaction")


    store_data.cache()

    combined_data = store_data.join(groupped_amenity, on="Store Name", how="left")
    combined_data = combined_data.fillna({
        "Number of Restaurants": 0,
        "Number of Bars": 0,
        "Number of Stadium": 0,
        "Number of Supermarkets": 0
    })

    combined_data.cache()


    # ======================== Visualization ===========================================

    '''
    plot_scatter(combined_data, "Number of Bars", "Sales per Year")
    plot_scatter(combined_data, "Number of Bars", "Transactions per Year")
    plot_scatter(combined_data, "Number of Bars", "Volume Sold per Year")
    plot_scatter(combined_data, "Number of Bars", "Sales per Transaction")
    plot_scatter(combined_data, "Number of Bars", "Volume per Transaction") 
    '''
    
    bins = [-1, 0, 5, 10, float("inf")]
    labels = ["0 Bars", "1-5 Bars", "5-10 Bars", ">10 Bars"]
    plot_barchart_remove_outliers(combined_data, "Number of Bars", "Sales per Year", bins, labels, (5, 6))
    plot_barchart_remove_outliers(combined_data, "Number of Bars", "Transactions per Year", bins, labels, (5, 6))
    plot_barchart_remove_outliers(combined_data, "Number of Bars", "Volume Sold per Year", bins, labels, (5, 6))
    plot_barchart_remove_outliers(combined_data, "Number of Bars", "Sales per Transaction", bins, labels, (5, 6))
    plot_barchart_remove_outliers(combined_data, "Number of Bars", "Volume per Transaction", bins, labels, (5, 6))


    bins = [-1, 0, 10, 20, float("inf")]
    labels = ["0 Restaurants", "1-10 Restaurants", "10-20 Restaurants", ">20 Restaurants"]
    plot_barchart_remove_outliers(combined_data, "Number of Restaurants", "Sales per Year", bins, labels, (6, 6))
    plot_barchart_remove_outliers(combined_data, "Number of Restaurants", "Transactions per Year", bins, labels, (6, 6))
    plot_barchart_remove_outliers(combined_data, "Number of Restaurants", "Volume Sold per Year", bins, labels, (6, 6))
    plot_barchart_remove_outliers(combined_data, "Number of Restaurants", "Sales per Transaction", bins, labels, (6, 6))
    plot_barchart_remove_outliers(combined_data, "Number of Restaurants", "Volume per Transaction", bins, labels, (6, 6))
    
    
    bins = [-1, 0, float("inf")]
    labels = ["No Stadium", "Has Stadium"]
    plot_barchart_remove_outliers(combined_data, "Number of Stadium", "Sales per Year", bins, labels, (4, 6))
    plot_barchart_remove_outliers(combined_data, "Number of Stadium", "Transactions per Year", bins, labels, (4, 6))
    plot_barchart_remove_outliers(combined_data, "Number of Stadium", "Volume Sold per Year", bins, labels, (4, 6))
    plot_barchart_remove_outliers(combined_data, "Number of Stadium", "Sales per Transaction", bins, labels, (4, 6))
    plot_barchart_remove_outliers(combined_data, "Number of Stadium", "Volume per Transaction", bins, labels, (4, 6))
    
    bins = [-1, 0, 2, float("inf")]
    labels = ["0 Supermarkets", "1-2 Supermarkets", ">2 Supermarkets"]
    plot_barchart_remove_outliers(combined_data, "Number of Supermarkets", "Sales per Year", bins, labels, (4, 6))
    plot_barchart_remove_outliers(combined_data, "Number of Supermarkets", "Transactions per Year", bins, labels, (4, 6))
    plot_barchart_remove_outliers(combined_data, "Number of Supermarkets", "Volume Sold per Year", bins, labels, (4, 6))
    plot_barchart_remove_outliers(combined_data, "Number of Supermarkets", "Sales per Transaction", bins, labels, (4, 6))
    plot_barchart_remove_outliers(combined_data, "Number of Supermarkets", "Volume per Transaction", bins, labels, (4, 6))

    
if __name__ == '__main__':
    #inputs = sys.argv[1]
    spark = SparkSession.builder.appName('Iowa liquor sales etl').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    liquor = 'Final_cleaned_liquor_sales'
    amenity = 'poi_counts_2km.csv'

    main(liquor, amenity)
