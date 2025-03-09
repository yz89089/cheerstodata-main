from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year, sum, to_date
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import pandas as pd

# Initialize Spark
spark = SparkSession.builder \
    .appName("Iowa Sales Prediction") \
    .getOrCreate()

# Data path
data_path = "/Users/zhangweiwei/Again_Final_cleaned_liquor_sales/*.csv"

# Load data
data = spark.read.csv(data_path, header=True, inferSchema=True)

# Ensure the date column is correctly formatted
data = data.withColumn("Date", to_date(col("Date"), "yyyy/MM/dd"))

# Extract time-related fields
data = data.withColumn("Month", month(col("Date")))
data = data.withColumn("Year", year(col("Date")))

# Filter data range: 2013 to 2023
data = data.filter((col("Year") >= 2013) & (col("Year") <= 2023))

# Aggregate historical data: by year and month
agg_data = data.groupBy("Year", "Month").agg(
    sum("Sale (Dollars)").alias("Total Sales")
).orderBy("Year", "Month")

# Convert to Pandas DataFrame
agg_data_pd = agg_data.toPandas()

# Feature engineering: build model data
agg_data_pd["Quarter"] = ((agg_data_pd["Month"] - 1) // 3) + 1
features = ["Year", "Quarter", "Month"]
X = agg_data_pd[features]
y = agg_data_pd["Total Sales"]

# Split data into training and testing sets (80% training, 20% testing)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train the random forest model
rf = RandomForestRegressor(n_estimators=100, random_state=42)
rf.fit(X_train, y_train)

# Test model performance
if not X_test.empty:
    y_pred = rf.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    print("Model Performance on Test Data:")
    print(f"MSE: {mse:.2f}, R²: {r2:.2f}")

# Retrain the model using the full dataset
rf_full = RandomForestRegressor(n_estimators=100, random_state=42)
rf_full.fit(X, y)

# Predict total sales for December 2024
december_2024_data = pd.DataFrame({"Year": [2024], "Quarter": [4], "Month": [12]})
december_2024_sales = rf_full.predict(december_2024_data)[0]
print(f"Predicted Total Sales for December 2024: {december_2024_sales:.2f}")

# Stop Spark
spark.stop()
