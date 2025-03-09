from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, to_date, trim  
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Data Cleaning for Iowa Liquor Sales") \
    .getOrCreate()

# Data path
data_path = "/Users/zhangweiwei/liquor-add-locations/*.csv"

# Define Schema
schema = StructType([
    StructField("Store Name", StringType(), True),
    StructField("Invoice/Item Number", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Store Number", IntegerType(), True),
    StructField("Address", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Zip Code", StringType(), True),
    StructField("Store Location", StringType(), True),
    StructField("County Number", StringType(), True),
    StructField("County", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Category Name", StringType(), True),
    StructField("Vendor Number", StringType(), True),
    StructField("Vendor Name", StringType(), True),
    StructField("Item Number", StringType(), True),
    StructField("Item Description", StringType(), True),
    StructField("Pack", IntegerType(), True),
    StructField("Bottle Volume (ml)", DoubleType(), True),
    StructField("State Bottle Cost", DoubleType(), True),
    StructField("State Bottle Retail", DoubleType(), True),
    StructField("Bottles Sold", DoubleType(), True),
    StructField("Sale (Dollars)", DoubleType(), True),
    StructField("Volume Sold (Liters)", DoubleType(), True),
    StructField("Volume Sold (Gallons)", DoubleType(), True)
])

# Load data
data = spark.read.csv(data_path, header=True, schema=schema)

# Data cleaning steps
# 1. Drop unnecessary columns
data = data.drop("County Number", "Volume Sold (Gallons)")

# 2. Convert data types and filter invalid data
data = data.withColumn("Sale (Dollars)", col("Sale (Dollars)").cast("double")) \
           .withColumn("Date", to_date(col("Date"), "yyyy-MM-dd")) \
           .filter((col("Sale (Dollars)") > 0) & (col("Bottles Sold") > 0))

# 3. Remove rows with null values in specified columns
data = data.filter(
    col("Invoice/Item Number").isNotNull() &
    col("Date").isNotNull() &
    col("Store Number").isNotNull()
)

# 4. Standardize string fields
data = data.withColumn("City", trim(lower(col("City"))))

# 5. Remove duplicate rows based on key column
data = data.dropDuplicates(["Invoice/Item Number"])

# Cache data to avoid redundant computation
data.cache()

# Validate cleaning results: Count total rows after cleaning and describe numeric fields
print(f"Total rows after cleaning: {data.count()}")
data.describe(["Bottle Volume (ml)", "Sale (Dollars)", "Bottles Sold", "Volume Sold (Liters)"]).show()
data.show(5, truncate=False)

# Save cleaned data to partitioned files
cleaned_data_path = "/Users/zhangweiwei/Again_Final_cleaned_liquor_sales"
data.write.csv(cleaned_data_path, header=True, mode="overwrite")

print(f"Cleaned data saved to {cleaned_data_path}")

# Stop SparkSession
spark.stop()
