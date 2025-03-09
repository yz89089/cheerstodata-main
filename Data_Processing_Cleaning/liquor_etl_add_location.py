import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col, count, min, max, trim, when

import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time

# Initialize the geocoder
geolocator = Nominatim(user_agent="store_locator", timeout=10)

sales_schema = types.StructType([
    types.StructField('Invoice/Item Number', types.StringType(), True),
    types.StructField('Date', types.DateType(), True),
    types.StructField('Store Number', types.IntegerType(), True),
    types.StructField('Store Name', types.StringType(), True),
    types.StructField('Address', types.StringType(), True),
    types.StructField('City', types.StringType(), True),
    types.StructField('Zip Code', types.StringType(), True),
    types.StructField('Store Location', types.StringType(), True),
    types.StructField('County Number', types.IntegerType(), True),
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
    types.StructField('Volume Sold (Liters)', types.DoubleType(), True),
    types.StructField('Volume Sold (Gallons)', types.DoubleType(), True)
])

location_schema = types.StructType([
    types.StructField('Store Name', types.StringType(), True),
    types.StructField('Store Location', types.StringType(), True)
])   

def geocode_address(store_name, address, city, retries=3, delay=1):
    """
    Geocode an address using Nominatim and return a String.
    Args:
        address (str): Street address.
        city (str): City name.
        retries (int): Number of retries for timeouts.
        delay (int): Delay between retries (in seconds).
    Returns:
        str: Point(Longitude, Latitude) or None if geocoding fails.
    """
    query = f"{address}, {city}, Iowa, USA"
    for attempt in range(retries):
        try:
            location = geolocator.geocode(query)
            if location:
                print(f"Match found for {store_name}.")
                return f"POINT ({location.longitude} {location.latitude})"
            else:
                print("No match found.")
                return None
        except GeocoderTimedOut:
            print(f"Timeout while geocoding '{store_name}', attempt {attempt + 1}/{retries}")
            time.sleep(delay)
    return None

def geocode_row(row):
    return geocode_address(row["Store Name"], row["Address"], row["City"])


def update_location_column(ori_data, reference_data):
    # this is the function of updating the 'Store Location' column
    # ori_data is the original data with all columns
    # reference_data contains retrieved 'Store Location' column and 'Store Name'
    # return is the updated dataframe contains all columns
    joined_data = ori_data.join(\
        reference_data.select(col("Store Name"), col("Store Location").alias("Location Update")), \
        on="Store Name", how="left"\
        )

    updated_data = joined_data.withColumn(
        "Store Location",
        when(
            (col("Store Location").isNull()) | (trim(col("Store Location")) == ""),  # Condition: Null or blank
            col("Location Update")  # Use 'Location Update'
        ).otherwise(col("Store Location"))  # Keep the original 'Store Location'
    ).drop("Location Update")  # Drop the helper column

    return updated_data


def main(inputs, output):
    # main logic starts here  
    

    ori_data = spark.read.csv(inputs, header=True, schema=sales_schema, dateFormat='MM/dd/yyyy')
    # ori_data.cache()
    print(f'total number of records: {ori_data.count()}')

    # ===================== Step 1: Update location information with existing data ====================
    # (for the case that location information is available for some stores but missed for some records)
    distinct_store = ori_data.select('Store Name', 'Store Location').dropDuplicates(["Store Name"])
    print(f'total number of stores: {distinct_store.count()}')

    store_with_loactions = distinct_store.filter(col("Store Location").isNotNull())
    print(f'number of stores with locations: {store_with_loactions.count()}')

    updated_data = update_location_column(ori_data, store_with_loactions)

    count_before = ori_data.filter(col("Store Location").isNull()).count()
    count_after = updated_data.filter(col("Store Location").isNull()).count()
    print(f'number of records updated by Step 1: {count_before-count_after}')


    # =================== Step 2: Update the rest missing locations with geocoding API from OSM ==========
    # ===================(Use Address and City to retrieve the longitude and latitude values) ============
    store_without_loactions = updated_data\
    .filter(col('Store Location').isNull())\
    .select('Store Name', 'Address', 'City').dropDuplicates(["Store Name"])
    print(f'number of stores without locations: {store_without_loactions.count()}')

    store_pd = store_without_loactions.toPandas()

    store_pd["Store Location"] = store_pd.apply(geocode_row, axis=1)
    valid_locations = store_pd[store_pd["Store Location"].notna()]
    valid_locations_filtered = valid_locations[["Store Name", "Store Location"]]

    reference_df = spark.createDataFrame(valid_locations_filtered, schema=location_schema)

    final_data = update_location_column(updated_data, reference_df)

    count_before = updated_data.filter(col("Store Location").isNull()).count()
    count_after = final_data.filter(col("Store Location").isNull()).count()
    print(f'number of records updated by Step 2: {count_before-count_after}')   

    final_data.repartition(100).write.csv(output, header=True, mode='overwrite')
    

    
    
if __name__ == '__main__':
    # inputs = sys.argv[1]
    # output = sys.argv[2]

    spark = SparkSession.builder.appName('Iowa liquor sales etl').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    inputs = 'liquor-original'
    output = 'liquor-add-locations'

    main(inputs, output)
