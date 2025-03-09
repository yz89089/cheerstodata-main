import sys
import os
import time
from py4j.protocol import Py4JJavaError
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession, functions, types
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import sum, count, desc
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats

# Try to create SparkSession
def create_spark_session(max_attempts=5, wait_time=10):
    for attempt in range(max_attempts):
        try:
            # Use a more conservative configuration
            spark = SparkSession.builder \
                .appName('lowa analysis - special') \
                .config('spark.driver.memory', '2g') \
                .config('spark.driver.host', '10.255.255.254') \
                .config('spark.driver.bindAddress', '10.255.255.254') \
                .config('spark.network.timeout', '600s') \
                .master('local[*]')\
                .getOrCreate()
            
            # Set the log level to view more information
            spark.sparkContext.setLogLevel('INFO')
            
            # Verify that the Spark session is working
            test_df = spark.createDataFrame([(1,)], ['test'])
            test_df.count()  # Perform a simple operation to verify
            
            print("Successfully created Spark session")
            return spark
            
        except Exception as e:
            print(f"Attempt {attempt + 1} failed with error: {str(e)}")
            print(f"Error type: {type(e).__name__}")
            
            if attempt < max_attempts - 1:
                print(f"Waiting {wait_time} seconds before retrying...")
                
                # Clean up any residual resources before retrying
                try:
                    if 'spark' in locals():
                        spark.stop()
                except:
                    pass
                
                time.sleep(wait_time)
            else:
                print("All attempts to create Spark session failed.")
                print("Please check your Spark installation and environment variables:")
                print(f"JAVA_HOME: {os.environ.get('JAVA_HOME', 'Not Set')}")
                print(f"SPARK_HOME: {os.environ.get('SPARK_HOME', 'Not Set')}")
                print(f"Python version: {sys.version}")
                raise Exception("Failed to create Spark session after multiple attempts")

def analyze_liquor_trends(data, target_years, top_liquors):
    # Standardize Fireball product names
    data = data.withColumn('Item Description', 
        functions.when(
            functions.col('Item Description').like('%FIREBALL%'), 
            'FIREBALL CINNAMON WHISKEY'
        ).otherwise(functions.col('Item Description'))
    )
    
    monthly_sales = data.filter(
        (data.year.isin(target_years)) & 
        (data['Item Description'].isin(top_liquors))
    ).groupBy('year', 'month', 'Item Description').agg(
        functions.sum('Bottles Sold').alias('total_bottles')
    ).orderBy('year', 'month')
    
    return monthly_sales.toPandas()

def analyze_pack_trends(data, target_years):
    target_packs = [6, 12, 48]
    monthly_pack_sales = data.filter(
        (data.year.isin(target_years)) & 
        (data['Pack'].isin(target_packs))
    ).groupBy('year', 'month', 'Pack').agg(
        functions.sum('Bottles Sold').alias('total_bottles')
    ).orderBy('year', 'month')
    return monthly_pack_sales.toPandas()

def analyze_city_distribution(data):
    city_distribution = data.groupBy('City', 'Item Description').agg(
        functions.sum('Bottles Sold').alias('total_bottles')
    )
    return city_distribution.toPandas()

def plot_liquor_trends(liquor_trends_df):
    # Create 3 subplots
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(15, 18))
    
    # Define colors and line styles
    colors = {
        'FIREBALL CINNAMON WHISKEY': '#1f77b4',  # 蓝色
        'BLACK VELVET': '#ff7f0e',               # 橙色
        'HAWKEYE VODKA': '#2ca02c',              # 绿色
        'BARTON VODKA': '#d62728',               # 红色
        'CROWN ROYAL': '#9467bd',                # 紫色
        'FIREBALL CINNAMON': '#8c564b'           # 棕色
    }
    
    # Define line styles for each liquor
    linestyles = {
        'FIREBALL CINNAMON WHISKEY': '-',    # Solid line
        'BLACK VELVET': '--',                # Dashed line
        'HAWKEYE VODKA': '-.',               # Dash-dot line
        'BARTON VODKA': ':',                 # Dotted line
        'CROWN ROYAL': '--',                 # Dashed line
        'FIREBALL CINNAMON': '-.'            # Dash-dot line
    }
    
    axes = {
        2013: ax1,
        2017: ax2,
        2021: ax3
    }
    
    # Plot data for each year
    for year in [2013, 2017, 2021]:
        ax = axes[year]
        year_data = liquor_trends_df[liquor_trends_df['year'] == year]
        
        for liquor in colors.keys():
            liquor_data = year_data[year_data['Item Description'] == liquor]
            if not liquor_data.empty:
                ax.plot(liquor_data['month'], 
                       liquor_data['total_bottles'],
                       label=liquor,
                       color=colors[liquor],
                       linestyle=linestyles[liquor],
                       marker='o',
                       markersize=6)
        
        ax.set_xlabel('Month')
        ax.set_ylabel('Bottles Sold')
        ax.set_title(f'Liquor Sales Trends in {year}')
        ax.grid(True, linestyle='--', alpha=0.7)
        ax.set_xticks(range(1, 13))
        
        # Add legend, and place it outside the chart
        ax.legend(bbox_to_anchor=(1.05, 1), 
                 loc='upper left', 
                 borderaxespad=0.,
                 frameon=True,
                 fancybox=True,
                 shadow=True)
    
    plt.tight_layout()  # Automatically adjust spacing between subplots
    plt.savefig('liquor_trends.png', bbox_inches='tight', dpi=300)
    plt.close()

def plot_pack_trends(pack_trends_df):
    # Create three subplots
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(15, 18))
    
    # Define color scheme
    color = '#1f77b4'  # Use a consistent blue
    
    # Pack 6 trend
    for year in [2013, 2017, 2021]:
        year_data = pack_trends_df[
            (pack_trends_df['year'] == year) & 
            (pack_trends_df['Pack'] == 6)
        ]
        ax1.plot(year_data['month'], 
                year_data['total_bottles'],
                label=f'{year}',
                marker='o',
                alpha=0.7)
    ax1.set_title('Pack 6 Sales Trends')
    
    # Pack 12 trend
    for year in [2013, 2017, 2021]:
        year_data = pack_trends_df[
            (pack_trends_df['year'] == year) & 
            (pack_trends_df['Pack'] == 12)
        ]
        ax2.plot(year_data['month'], 
                year_data['total_bottles'],
                label=f'{year}',
                marker='o',
                alpha=0.7)
    ax2.set_title('Pack 12 Sales Trends')
    
    # Pack 48 trend
    for year in [2013, 2017, 2021]:
        year_data = pack_trends_df[
            (pack_trends_df['year'] == year) & 
            (pack_trends_df['Pack'] == 48)
        ]
        ax3.plot(year_data['month'], 
                year_data['total_bottles'],
                label=f'{year}',
                marker='o',
                alpha=0.7)
    ax3.set_title('Pack 48 Sales Trends')
    
    # Add common settings for all subplots
    for ax in [ax1, ax2, ax3]:
        ax.set_xlabel('Month')
        ax.set_ylabel('Bottles Sold')
        ax.grid(True, linestyle='--', alpha=0.7)
        ax.legend()
        ax.set_xticks(range(1, 13))
    
    plt.tight_layout()
    plt.savefig('pack_trends.png', bbox_inches='tight', dpi=300)
    plt.close()

def plot_city_distribution(data, spark):
    # Define fixed color mapping, using standardized names
    liquor_colors = {
        'FIREBALL CINNAMON WHISKEY': '#98DFD6',  # Mint green
        'BLACK VELVET': '#FFEB85',               # Light yellow
        'HAWKEYE VODKA': '#B4B4DC',             # Light purple
        'BARTON VODKA': '#B4B4DC',              # Light purple
        'TITOS HANDMADE VODKA': '#7FB5DA',      # Sky blue
        "FIVE O'CLOCK VODKA": '#FFB366',        # Orange
        'FIVE OCLOCK VODKA': '#FFB366',         # Orange (alternative name)
        'FIREBALL CINNAMON': '#FFB366',         # Orange
        'CAPTAIN MORGAN SPICED RUM': '#FFB366', # Orange
        'CAPTAIN MORGAN ORIGINAL SPICED': '#FFB366', # Orange
        'Others': '#90EE90'                     # Light green
    }

    try:
        # Get the top 10 liquors by sales
        top_10_liquors = data.groupBy('Item Description').agg(
            functions.sum('Bottles Sold').alias('total_bottles')
        ).orderBy('total_bottles', ascending=False).limit(10)
        
        # Get the top 6 cities by sales
        top_cities = data.groupBy('City').agg(
            functions.sum('Bottles Sold').alias('total_bottles')
        ).orderBy('total_bottles', ascending=False).limit(6)
        
        # Get city liquor distribution data
        city_liquor_dist = data.join(
            top_10_liquors.select('Item Description'),
            on='Item Description'
        ).join(
            top_cities.select('City'),
            on='City'
        ).groupBy('City', 'Item Description').agg(
            functions.sum('Bottles Sold').alias('total_bottles')
        )
        
        # Convert to pandas
        city_dist_pd = city_liquor_dist.toPandas()
        
        # Create a 2x3 subplot layout
        fig, axes = plt.subplots(2, 3, figsize=(24, 16))
        axes = axes.ravel()
        
        # Create pie charts for each city
        for i, city in enumerate(city_dist_pd['City'].unique()):
            city_data = city_dist_pd[city_dist_pd['City'] == city]
            
            # Sort by sales and only show the top 5
            city_data = city_data.nlargest(5, 'total_bottles')
            
            # Calculate the total of other liquors
            other_total = city_dist_pd[
                (city_dist_pd['City'] == city) & 
                (~city_dist_pd['Item Description'].isin(city_data['Item Description']))
            ]['total_bottles'].sum()
            
            # Add "Others" category
            if other_total > 0:
                city_data = pd.concat([
                    city_data,
                    pd.DataFrame({
                        'City': [city],
                        'Item Description': ['Others'],
                        'total_bottles': [other_total]
                    })
                ])
            
            # Get the corresponding color list, using a safe color retrieval method
            colors = []
            for item in city_data['Item Description']:
                if item in liquor_colors:
                    colors.append(liquor_colors[item])
                else:
                    # If no color is defined, use the default color
                    print(f"Warning: No color defined for {item}, using default color")
                    colors.append('#CCCCCC')  # Use gray as default
            
            # Draw pie charts
            wedges, texts, autotexts = axes[i].pie(
                city_data['total_bottles'],
                labels=city_data['Item Description'],
                colors=colors,
                autopct='%1.1f%%',
                pctdistance=0.85,
                wedgeprops=dict(width=0.5)
            )
            
            # Set label styles
            plt.setp(autotexts, size=9, weight="bold")
            plt.setp(texts, size=8)
            
            # Add title
            axes[i].set_title(f'{city.lower()}\nTotal Sales: {city_data["total_bottles"].sum():,.0f} bottles', 
                             pad=20, size=12, weight='bold')
        
        # Delete extra subplots
        for j in range(i + 1, len(axes)):
            fig.delaxes(axes[j])
        
        plt.suptitle('Liquor Sales Distribution by City', size=16, weight='bold', y=1.02)
        plt.tight_layout()
        plt.savefig('city_distribution.png', bbox_inches='tight', dpi=300)
        plt.close()
        
    except Exception as e:
        print(f"Error in plot_city_distribution: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        raise

try:
    spark = create_spark_session()
    
    try:
        data = spark.read.csv('Final_cleaned_liquor_sales', header=True, inferSchema=True)
        data = data.withColumn('year', functions.year('date'))
        data = data.withColumn('month', functions.month('date'))
        
        target_years = [2013, 2017, 2021]
        top_liquors = ['FIREBALL CINNAMON WHISKEY', 'BLACK VELVET', 'HAWKEYE VODKA',
                      'BARTON VODKA', 'CROWN ROYAL', 'FIREBALL CINNAMON']
        
        # Pass data to analysis functions
        liquor_trends_df = analyze_liquor_trends(data, target_years, top_liquors)
        pack_trends_df = analyze_pack_trends(data, target_years)
        city_distribution_df = analyze_city_distribution(data)
        
        with pd.ExcelWriter('liquor_analysis_results.xlsx', engine='xlsxwriter') as writer:
            liquor_trends_df.to_excel(writer, sheet_name='Liquor Sales Trends', index=False)
            pack_trends_df.to_excel(writer, sheet_name='Pack Size Trends', index=False)
            city_distribution_df.to_excel(writer, sheet_name='City Distribution Analysis', index=False)
            
        # Add plotting calls
        plot_liquor_trends(liquor_trends_df)
        plot_pack_trends(pack_trends_df)
        plot_city_distribution(data, spark)
        
        print("Visualization charts have been generated successfully!")
            
    except Exception as e:
        print(f"Data processing error: {str(e)}")
        raise
        
except Exception as e:
    print(f"Unexpected error: {str(e)}")
    
finally:
    if 'spark' in locals():
        spark.stop()