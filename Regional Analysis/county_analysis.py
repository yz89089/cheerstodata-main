import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col, count, min, max, sum, avg, year
from pyspark.sql.functions import trim, countDistinct, udf, upper, regexp_replace
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from shapely.geometry import Point
from shapely.wkt import loads

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

population_schema = types.StructType([
    types.StructField('Geographic Area', types.StringType(), True),
    types.StructField('Estimates Base', types.StringType(), True)
])

metropolitan_schema = types.StructType([
    types.StructField('CBSA Title', types.StringType(), True),
    types.StructField('Metropolitan/Micropolitan Statistical Area', types.StringType(), True),
    types.StructField('CSA Title', types.StringType(), True),
    types.StructField('County/County Equivalent', types.StringType(), True),
    types.StructField('State Name', types.StringType(), True)
])

def plot_stores(ax, store_data, counties_gdf):
    # figure: fig, ax (optional)
    # spark dataframe: store_data (to retrieve store locations)
    # geopandas dataframe: counties_gdf (to check county boundary)
    
    store_pd = store_data.toPandas()
    store_pd["geometry"] = store_pd["Store Location"].apply(loads)

    store_gdf = gpd.GeoDataFrame(store_pd, geometry="geometry", crs="EPSG:4326")
    
    # keep only stores with location inside Iowa state (some data might be wrong)
    stores_in_iowa = store_gdf[store_gdf.geometry.within(counties_gdf.unary_union)]

    # counties_gdf.plot(ax=ax, color="white", edgecolor="black", alpha=0.5, label="Counties")
    stores_in_iowa.plot(ax=ax, color="blue", markersize=5, label="Liquor Stores", legend=True, alpha=0.6)

def plot_county_stats(stats_data, column_name, counties_gdf, legend_label, plot_title):
    # figure: fig, ax (optional)
    # spark dataframe: stats_data (the statics to plot with county map)
    # geopandas dataframe: counties_gdf (to check county boundary)
    # the column for plotting heat map: column_name
    
    stats_pd = stats_data.toPandas()

    merged_gdf = counties_gdf.merge(stats_pd, on="County", how="left")

    fig, ax = plt.subplots(1, 1, figsize=(12, 8))
    merged_gdf.plot(column=column_name,
                    cmap="Blues",  # Color map
                    linewidth=0.8,
                    ax=ax,
                    edgecolor="0.8",
                    legend=True,
                    legend_kwds={"label": legend_label})

    top5_counties = stats_data.orderBy(column_name, ascending=False)\
    .select("County").limit(5).toPandas()["County"].tolist()

    top5_counties = [county.title() for county in top5_counties]
    merged_gdf['County'] = merged_gdf['County'].str.title()

    for _, row in merged_gdf.iterrows():
        if row["County"] in top5_counties:
            # Get the centroid of the county for labeling
            centroid = row["geometry"].centroid
            ax.text(centroid.x, centroid.y+0.1, row["County"], fontsize=12, color="orangered")
    
    ax.set_title(plot_title, fontsize=16)

    return fig, ax
    
def plot_histogram(stats_data, column_name, unit=None):

    stats_pd = stats_data.toPandas()

    plt.figure(figsize=(8, 6))
    plt.hist(stats_pd[column_name], bins=10, color='skyblue', edgecolor='black')
    plt.title(f"Histogram of {column_name}", fontsize=16)
    plt.xlabel(f"{column_name} {unit}", fontsize=12)
    plt.ylabel("Count", fontsize=12)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.show()


def main(liquor, population):

    # ======================== Read Data =======================================================
    # the cleaned version of Iowa liquor Sales data
    ori_data = spark.read.csv(liquor, header=True, schema=sales_schema)

    # filter records between 2014-01-01 and 2023-12-31 (10 years span)
    ori_filtered = ori_data.filter(col("Date").between("2014-01-01", "2023-12-31"))

    # the population by county data from US Census Bureau
    # https://www.census.gov/data/datasets/time-series/demo/popest/2020s-counties-total.html
    pop_data = spark.read.csv(population, header=True, schema=population_schema)
    clean_pop_data = pop_data.withColumn("County",\
            upper(regexp_replace(col("Geographic Area"), r"^\.\s*| County, Iowa$", "")))\
        .withColumn("Population",\
            regexp_replace(col("Estimates Base"), ",", "").cast("int"))
    clean_pop_data = clean_pop_data.select("County", "Population")

    '''
    # Catagorization of metropolitan area and micropolitan area from US Census Bureau
    # https://www.census.gov/geographies/reference-files/time-series/demo/metro-micro/delineation-files.html
    metro_data = spark.read.csv(metropolitan, header=True, schema=metropolitan_schema)
    clean_metro_data = metro_data.filter(col("State Name")=="Iowa")\
    .withColumn("County",upper(trim(regexp_replace(col("County/County Equivalent"), " County$", ""))))
    clean_metro_data = clean_metro_data.select(\
        col("Metropolitan/Micropolitan Statistical Area").alias("Metropolitan"),
        col("county"))
    '''
    
    # Load the Iowa county shapefile
    shapefile_path = "Iowa_County_Boundaries/IowaCounties.shp"
    counties_gdf = gpd.read_file(shapefile_path)
    counties_gdf = counties_gdf.to_crs("EPSG:4326") # Ensure both GeoDataFrames use the same CRS
    counties_gdf["County"] = counties_gdf["CountyName"].str.upper()
    

    # ======================== Analysis ===========================================================
    # Summarize the number of stores, sales, liquor volumne by county, add population data
    summary_by_county = ori_filtered.groupBy("County").agg(\
        countDistinct("Store Name").alias("Number of Stores"),\
        (sum("Sale (Dollars)")).alias("Total Sales"),\
        (sum("Volume Sold (Liters)").alias("Total Volume"))
        )

    # normalize the sales and volume by year (10 years)
    summary_by_county = summary_by_county.withColumn("Sales per Year", col("Total Sales")/10)\
    .withColumn("Volume Sold per Year", col("Total Volume")/10)

    # join the population data for each county
    summary_by_county = summary_by_county.join(clean_pop_data, on="County", how="inner")

    # cache for future analysis
    summary_by_county.cache()



    # Analysis 1: How many liquor stores in each county
    num_stores = summary_by_county.select("County", "Number of Stores")
    fig, ax = plot_county_stats(num_stores, "Number of Stores", counties_gdf, "Number of Stores", "Number of Liquor Stores")    
    store_data = ori_filtered.dropDuplicates(["Store Name"])\
    .select("Store Name", "Store Location")
    plot_stores(ax, store_data, counties_gdf)

    # Analysis 2: Which county has better performing stores?
    average_sales = summary_by_county.select(col("County"),\
        ((col("Sales per Year")/col("Number of Stores"))/1000000).alias("Sales per Store"))
    fig, ax = plot_county_stats(average_sales, "Sales per Store", counties_gdf, "Million Dollars",
        "Average Yearly Sales per Store (in Million Dollars)")

    # Analysis 3: Who drinks more each year?
    average_volumne = summary_by_county.select(col("County"),\
        (col("Volume Sold per Year")/col("Population")).alias("Volume per Person")) 
    fig, ax = plot_county_stats(average_volumne, "Volume per Person", counties_gdf, "Volume per Person (in Liters)",
        "Cosumed Liquor Volume (in Liters) per Person per Year")

    # Analysis 4: Which county spend more money on liquor yearly?
    yearly_sales = summary_by_county.select(col("County"),
        (col("Sales per Year")/1000000).alias("Yearly Sales"))
    fig, ax = plot_county_stats(yearly_sales, "Yearly Sales", counties_gdf, "Million Dollars",
        "Yearly Sales Amount (in Million Dollars)")
    plt.show()

    
    
    
if __name__ == '__main__':
    #inputs = sys.argv[1]
    spark = SparkSession.builder.appName('Iowa liquor sales etl').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    liquor = 'Final_cleaned_liquor_sales'
    population = 'population_by_county.csv'
    # metropolitan = 'metropolitan.csv'

    main(liquor, population)
