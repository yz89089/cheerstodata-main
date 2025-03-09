import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col, count, min, max, trim, countDistinct, sum, avg, udf, lower
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from shapely.geometry import Point
from shapely.wkt import loads
import re
import osmnx as ox
import contextily as ctx

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

def main(inputs):
    

    ori_data = spark.read.csv(inputs, header=True, schema=sales_schema, dateFormat='MM/dd/yyyy')

    city_data = ori_data.filter(lower(col("County")) == "polk")
    store_locations = city_data.dropDuplicates(["Store Name"])\
    .select("Store Name", "Store Location")

    # generate the store geopandas dataframe
    store_pd = store_locations.toPandas()
    store_pd["geometry"] = store_pd["Store Location"].apply(loads)
    store_gdf = gpd.GeoDataFrame(store_pd, geometry="geometry", crs="EPSG:4326")

    # double check that all store locations are within Polk County
    polk_boundary = ox.geocode_to_gdf("Polk County, Iowa, USA")
    polk_boundary = polk_boundary.to_crs(store_gdf.crs)
    store_gdf = store_gdf[store_gdf.geometry.within(polk_boundary.unary_union)]
    print(f"Total number of stores: {len(store_gdf)}")

    # generate the Polk County amenity geopandas dataframe
    tags = {
        "amenity": ["bar", "pub", "restaurant", "cafe"],
        "leisure": ["stadium"],
        "shop": ["supermarket"]
    }
    city_pois = ox.features_from_place("Polk, Iowa, USA", tags=tags)
    city_pois = city_pois.to_crs("EPSG:4326")
    print(f"Total POIs retrieved: {len(city_pois)}")

    # reprojecting before plotting
    store_gdf = store_gdf.to_crs(epsg=3857)
    city_pois = city_pois.to_crs(epsg=3857)


    # ================== Visualize the liquor store and amanities ========================
    fig, ax = plt.subplots(figsize=(10, 8))

    # Plot liquor stores in red
    store_gdf.plot(ax=ax, marker="o", color="blue", markersize=50, label="Liquor Stores", alpha=0.8)

    # Plot POIs, coloring by their amenity type
    for item in ["amenity", "leisure", "shop"]:
        if item in city_pois.columns:
            for poi_type, group in city_pois.groupby(item):
                group.plot(ax=ax, marker="x", label=f"{item}: {poi_type}", markersize=80)

    # Add a basemap
    ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, crs=store_gdf.crs.to_string())

    plt.title("Liquor Stores and POIs in Polk County", fontsize=16)
    plt.legend(title="POI Types")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.grid(True)

    plt.show()

    # =============== Spatial join to calculate number of amenities within 2km =================

    # Add a buffer of 2km (2000 meters) around each liquor store
    store_gdf["buffer"] = store_gdf.geometry.buffer(2000)
    
    # Perform a spatial join: Find POIs within each liquor store buffer
    pois_within_2km = gpd.sjoin(city_pois, store_gdf.set_geometry("buffer"), how="inner", predicate="within")
    pois_within_2km.fillna({"amenity": "None", "leisure": "None", "shop": "None"}, inplace=True)

    # Group by store name and POI category to calculate counts
    poi_counts = pois_within_2km.groupby(["Store Name", "amenity", "leisure", "shop"]).size().reset_index(name="count")
    poi_counts.sort_values(by="Store Name").to_csv("poi_counts_2km.csv", index=False)

    
        

  
if __name__ == '__main__':
    #inputs = sys.argv[1]
    spark = SparkSession.builder.appName('Iowa liquor sales etl').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    inputs = 'Final_cleaned_liquor_sales'

    main(inputs)
