#%%
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
                .appName('lowa analysis - distribution') \
                .config('spark.driver.memory', '2g') \
                .config('spark.driver.host', '10.255.255.254') \
                .config('spark.driver.bindAddress', '10.255.255.254') \
                .config('spark.network.timeout', '600s') \
                .master('local[*]')\
                .getOrCreate()
            
            # Set log level to view more information
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
                
                # Clean up possible residual resources before retrying
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

# Create SparkSession
try:
    # Create Spark session
    spark = create_spark_session()
    
    # Verify Spark version
    print(f"Using Spark version: {spark.version}")
    
    # 加载数据
    try:
        inputs = 'Final_cleaned_liquor_sales'
        data = spark.read.csv(inputs, header=True, inferSchema=True)
        
        population = 'population_by_county.csv'
        data_population = spark.read.csv(population, header=True, inferSchema=True)
        
        #%%
        #analysis data

        #%%
        #Analysis of population data during ten years
        #The most popular ten vendors

          #Create a new chart
        fig, axes = plt.subplots(2, 2, figsize=(20, 16))
        fig.suptitle('Iowa Liquor Price and Volume Analysis', fontsize=16)

        # 1. Cost price distribution
        cost_data = data.select('State Bottle Cost').sample(False, 0.1).toPandas()
        max_cost = np.percentile(cost_data['State Bottle Cost'], 99)  # Use 99th percentile as the upper limit
        cost_bins = np.linspace(0, max_cost, 50)  # More reasonable interval division

        cost_hist, cost_bins_edges = np.histogram(cost_data['State Bottle Cost'], bins=cost_bins, density=True)
        max_cost_idx = np.argmax(cost_hist)
        max_cost_x = (cost_bins_edges[max_cost_idx] + cost_bins_edges[max_cost_idx + 1]) / 2
        max_cost_y = cost_hist[max_cost_idx]
        
        axes[0,0].hist(cost_data['State Bottle Cost'], bins=cost_bins, color='skyblue', density=True, alpha=0.7)
        axes[0,0].set_title('Distribution of Bottle Cost')
        axes[0,0].set_xlabel('Cost ($)')
        axes[0,0].set_ylabel('Density')

        # Add normal distribution curve
        cost_mean = cost_data['State Bottle Cost'].mean()
        cost_std = cost_data['State Bottle Cost'].std()
        x_cost = np.linspace(0, max_cost, 100)
        y_cost = stats.norm.pdf(x_cost, cost_mean, cost_std)
        axes[0,0].plot(x_cost, y_cost, 'r-', lw=2, label='Normal Distribution')
        axes[0,0].legend()

        axes[0,0].annotate(f'Peak: ({max_cost_x:.1f}, {max_cost_y:.4f})',
                          xy=(max_cost_x, max_cost_y),
                          xytext=(10, 10), textcoords='offset points',
                          bbox=dict(boxstyle='round,pad=0.5', fc='yellow', alpha=0.5),
                          arrowprops=dict(arrowstyle='->'))

        # 2. Retail price distribution
        retail_data = data.select('State Bottle Retail').sample(False, 0.1).toPandas()
        max_retail = np.percentile(retail_data['State Bottle Retail'], 99)
        retail_bins = np.linspace(0, max_retail, 50)

        retail_hist, retail_bins_edges = np.histogram(retail_data['State Bottle Retail'], bins=retail_bins, density=True)
        max_retail_idx = np.argmax(retail_hist)
        max_retail_x = (retail_bins_edges[max_retail_idx] + retail_bins_edges[max_retail_idx + 1]) / 2
        max_retail_y = retail_hist[max_retail_idx]
        
        axes[0,1].hist(retail_data['State Bottle Retail'], bins=retail_bins, color='lightgreen', density=True, alpha=0.7)
        axes[0,1].set_title('Distribution of Bottle Retail Price')
        axes[0,1].set_xlabel('Price ($)')
        axes[0,1].set_ylabel('Density')

        retail_mean = retail_data['State Bottle Retail'].mean()
        retail_std = retail_data['State Bottle Retail'].std()
        x_retail = np.linspace(0, max_retail, 100)
        y_retail = stats.norm.pdf(x_retail, retail_mean, retail_std)
        axes[0,1].plot(x_retail, y_retail, 'r-', lw=2, label='Normal Distribution')
        axes[0,1].legend()

        axes[0,1].annotate(f'Peak: ({max_retail_x:.1f}, {max_retail_y:.4f})',
                          xy=(max_retail_x, max_retail_y),
                          xytext=(10, 10), textcoords='offset points',
                          bbox=dict(boxstyle='round,pad=0.5', fc='yellow', alpha=0.5),
                          arrowprops=dict(arrowstyle='->'))

        # 3. Pack distribution (discrete data)
        pack_dist = data.groupBy('Pack') \
            .agg(count('*').alias('count')) \
            .orderBy('Pack') \
            .toPandas()

        # Filter out outliers, only show major pack sizes
        pack_threshold = pack_dist['count'].sum() * 0.001  # Show only those with >0.1% of total volume
        pack_dist_filtered = pack_dist[pack_dist['count'] >= pack_threshold]

        # Create bar chart
        bars = axes[1,0].bar(pack_dist_filtered['Pack'], 
                           pack_dist_filtered['count'], 
                           color='salmon', 
                           alpha=0.7,
                           width=0.8)  # Adjust bar width

        # Add value labels to the bars
        for bar in bars:
            height = bar.get_height()
            axes[1,0].text(bar.get_x() + bar.get_width()/2., height,
                         f'{int(height):,}',  # Format number display
                         ha='center', va='bottom', rotation=0)

        axes[1,0].set_title('Distribution of Pack Sizes\n(Showing sizes with >0.1% of total volume)', 
                          pad=20)  # Add explanatory title
        axes[1,0].set_xlabel('Pack Size')
        axes[1,0].set_ylabel('Count')
        
        # Set x-axis ticks
        axes[1,0].set_xticks(pack_dist_filtered['Pack'])
        axes[1,0].tick_params(axis='x', rotation=45)  # Rotate x-axis labels
        
        # Add grid lines for better readability
        axes[1,0].grid(True, axis='y', linestyle='--', alpha=0.7)
        
        # Set y-axis format to scientific notation
        axes[1,0].yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))

        max_pack_idx = pack_dist_filtered['count'].idxmax()
        max_pack_x = pack_dist_filtered.loc[max_pack_idx, 'Pack']
        max_pack_y = pack_dist_filtered.loc[max_pack_idx, 'count']
        
        axes[1,0].annotate(f'Peak: ({int(max_pack_x)}, {int(max_pack_y):,})',
                          xy=(max_pack_x, max_pack_y),
                          xytext=(10, 10), textcoords='offset points',
                          bbox=dict(boxstyle='round,pad=0.5', fc='yellow', alpha=0.5),
                          arrowprops=dict(arrowstyle='->'))

        # 4. Volume distribution
        volume_data = data.select('Bottle Volume (ml)').sample(False, 0.1).toPandas()
        max_volume = np.percentile(volume_data['Bottle Volume (ml)'], 99)
        volume_bins = np.linspace(0, max_volume, 50)

        volume_hist, volume_bins_edges = np.histogram(volume_data['Bottle Volume (ml)'], bins=volume_bins, density=True)
        max_volume_idx = np.argmax(volume_hist)
        max_volume_x = (volume_bins_edges[max_volume_idx] + volume_bins_edges[max_volume_idx + 1]) / 2
        max_volume_y = volume_hist[max_volume_idx]
        
        axes[1,1].hist(volume_data['Bottle Volume (ml)'], bins=volume_bins, color='lightcoral', density=True, alpha=0.7)
        axes[1,1].set_title('Distribution of Bottle Volumes')
        axes[1,1].set_xlabel('Volume (ml)')
        axes[1,1].set_ylabel('Density')

        axes[1,1].annotate(f'Peak: ({max_volume_x:.1f}, {max_volume_y:.4f})',
                          xy=(max_volume_x, max_volume_y),
                          xytext=(10, 10), textcoords='offset points',
                          bbox=dict(boxstyle='round,pad=0.5', fc='yellow', alpha=0.5),
                          arrowprops=dict(arrowstyle='->'))

        # Adjust layout
        plt.tight_layout(rect=[0, 0.03, 1, 0.95])

        # Save chart
        plt.savefig('price_volume_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()

        # Perform normality test
        from scipy import stats

        # Perform normality test on cost
        cost_statistic, cost_pvalue = stats.normaltest(cost_data['State Bottle Cost'].dropna())
        print("\nNormality Test for Bottle Cost:")
        print(f"Statistic: {cost_statistic:.4f}")
        print(f"P-value: {cost_pvalue:.4f}")

        # Perform normality test on retail price
        retail_statistic, retail_pvalue = stats.normaltest(retail_data['State Bottle Retail'].dropna())
        print("\nNormality Test for Bottle Retail Price:")
        print(f"Statistic: {retail_statistic:.4f}")
        print(f"P-value: {retail_pvalue:.4f}")

        # Calculate descriptive statistics
        cost_stats = cost_data.describe()
        retail_stats = retail_data.describe()
        volume_stats = volume_data.describe()

        # Save statistics to Excel
        with pd.ExcelWriter('distribution_analysis.xlsx', engine='xlsxwriter') as writer:
            cost_stats.to_excel(writer, sheet_name='Cost Statistics')
            retail_stats.to_excel(writer, sheet_name='Retail Statistics')
            volume_stats.to_excel(writer, sheet_name='Volume Statistics')
            pack_dist.to_excel(writer, sheet_name='Pack Distribution')
    except AnalysisException as e:
        print(f"Error reading data: {str(e)}")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
    
finally:
    if 'spark' in locals():
        try:
            spark.stop()
            print("Spark session successfully stopped")
        except:
            print("Error while stopping Spark session")