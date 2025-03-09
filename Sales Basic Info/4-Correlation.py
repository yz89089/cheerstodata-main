import matplotlib
matplotlib.use('Agg')
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import re
from pyspark.sql import types
from pyspark.sql.functions import col, year as spark_year  # 重命名 year 函数
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split

def create_spark_session():
    return SparkSession.builder \
        .appName("Liquor Correlation Analysis") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "1") \
        .config("spark.driver.maxResultSize", "1g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.memory.storageFraction", "0.5") \
        .config("spark.default.parallelism", "4") \
        .getOrCreate()

def extract_coordinates(df):
    # Extract longitude and latitude from Store Location
    # Format: "POINT (-93.60168 41.53657)"
    extract_lon = regexp_extract(col('Store Location'), r'POINT \(([-\d.]+)', 1).cast('double')
    extract_lat = regexp_extract(col('Store Location'), r'POINT \([-\d.]+ ([-\d.]+)', 1).cast('double')
    
    return df.withColumn('longitude', extract_lon) \
             .withColumn('latitude', extract_lat)

def load_data(spark, path):
    # Define schema as provided
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
    
    df = spark.read.csv(path, header=True, schema=sales_schema)
    return extract_coordinates(df)

def calculate_correlations_by_year(df, feature_cols, target_year):
    # Filter data for a specific year
    year_df = df.filter(spark_year(col('Date')) == target_year)
    
    # Sampling and processing
    sample_ratio = 0.005
    sampled_df = year_df.sample(False, sample_ratio, seed=42) \
                       .select(feature_cols) \
                       .na.drop() \
                       .repartition(4)
    
    pandas_df = sampled_df.toPandas()
    correlation_matrix = pandas_df.corr()
    
    return correlation_matrix

def plot_heatmap(correlation_matrix, title):
    plt.figure(figsize=(10, 8))
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(f"{title}.png")
    plt.close()

def random_forest_analysis_by_year(df, feature_cols, target_col, target_year):
    # use spark_year instead of year
    year_df = df.filter(spark_year(col('Date')) == target_year)
    
    sample_ratio = 0.005
    sampled_df = year_df.sample(False, sample_ratio, seed=42) \
                       .select([target_col] + feature_cols) \
                       .na.drop() \
                       .repartition(4)
    
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip"
    )
    
    vector_df = assembler.transform(sampled_df)
    
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol=target_col,
        numTrees=20,
        maxDepth=5
    )
    
    model = rf.fit(vector_df)
    
    importance = pd.DataFrame({
        'Feature': feature_cols,
        'Importance': model.featureImportances.toArray()
    })
    
    return importance.sort_values('Importance', ascending=False)

def plot_feature_importance(importance_df, year):
    plt.figure(figsize=(10, 6))
    sns.barplot(x='Importance', y='Feature', data=importance_df)
    plt.title(f'Random Forest Feature Importance - {year}')
    plt.tight_layout()
    plt.savefig(f'Feature_Importance_{year}.png')
    plt.close()

def analyze_feature_importance(df, year):
    # Filter data for a specific year
    year_df = df.filter(spark_year(col('Date')) == year)
    
    # modify the features and target
    features = [
        'State Bottle Retail',
        'Pack',
        'Bottle Volume (ml)'
    ]
    target = 'Bottles Sold'  
    
    # Sampling and data preparation
    sample_ratio = 0.005
    sampled_df = year_df.sample(False, sample_ratio, seed=42) \
                       .select(features + [target]) \
                       .na.drop() \
                       .toPandas()
    
    # prepare data
    X = sampled_df[features].values
    y = sampled_df[target].values
    
    # Standardize features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    X_scaled = pd.DataFrame(X_scaled, columns=features)
    
    # Random Forest Analysis
    rf = RandomForestRegressor(n_estimators=100, random_state=42)
    rf.fit(X_scaled, y)
    rf_importance = pd.DataFrame({
        'Feature': features,
        'Importance': rf.feature_importances_.tolist()
    })
    
    return rf_importance

def main():
    spark = create_spark_session()
    
    try:
        df = load_data(spark, "Final_cleaned_liquor_sales")
        
        # modify the features list for correlation analysis
        features = [
            'Bottles Sold',
            'State Bottle Retail',
            'Pack',
            'Bottle Volume (ml)',
            'Sale (Dollars)'  
        ]
        
        # 1. first, do correlation analysis
        for year in [2018, 2020, 2022]:
            print(f"\nAnalyzing correlations for year {year}")
            corr_matrix = calculate_correlations_by_year(df, features, year)
            plot_heatmap(corr_matrix, f'Liquor_Sales_Correlation_Heatmap_{year}')
        
        # 2.modify the features list for feature importance analysis
        features_for_importance = [
            'State Bottle Retail',
            'Pack',
            'Bottle Volume (ml)',
            'Bottles Sold' 
        ]
        
        for year in [2018, 2020, 2022]:
            print(f"\nAnalyzing feature importance for year {year}")
            
            year_df = df.filter(spark_year(col('Date')) == year)
            sample_ratio = 0.005
            sampled_df = year_df.sample(False, sample_ratio, seed=42) \
                               .select(features_for_importance + ['Sale (Dollars)']) \
                               .na.drop() \
                               .toPandas()
            
            # prepare data
            X = sampled_df[features_for_importance].values
            y = sampled_df['Sale (Dollars)'].values  
            
            # Standardize features
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)
            X_scaled = pd.DataFrame(X_scaled, columns=features_for_importance)
            
            # Random Forest Analysis
            rf = RandomForestRegressor(n_estimators=100, random_state=42)
            rf.fit(X_scaled, y)
            rf_importance = pd.DataFrame({
                'Feature': features_for_importance,
                'Importance': rf.feature_importances_.tolist()
            })
            
            print(f"\nRandom Forest Feature Importance - {year}:")
            print(rf_importance)
            
            # visualize
            plot_feature_importance(rf_importance, year)
            
            # convert to Spark DataFrame for saving
            rf_spark = spark.createDataFrame(rf_importance)
            
            # save the results
            rf_spark.write.csv(f'results/rf_importance_{year}.csv', header=True, mode='overwrite')
            
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
