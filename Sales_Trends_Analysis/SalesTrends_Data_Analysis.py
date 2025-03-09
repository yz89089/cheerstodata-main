from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year, sum, expr, udf, to_date
import pandas as pd
import matplotlib.pyplot as plt
import os
import datetime

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Time and Sales Analysis with Visualization") \
    .getOrCreate()

# Data path
cleaned_data_path = "/Users/zhangweiwei/Again_Final_cleaned_liquor_sales/*.csv"
output_dir = "/Users/zhangweiwei/Again_Final_1_analysis_results"

# Load cleaned data
data = spark.read.csv(cleaned_data_path, header=True, inferSchema=True)

# Force convert the "Date" column to a standard date format
data = data.withColumn("Date", to_date(col("Date"), "yyyy/M/d"))

# Extract time-related fields
data = data.withColumn("Month", month(col("Date")))
data = data.withColumn("Year", year(col("Date")))
data = data.withColumn("Quarter", expr("FLOOR((Month - 1) / 3) + 1"))

# ************ Line Chart - Annual Monthly Sales Trends *********************
def line_chart_monthly_sales(data, output_dir):
    # Filter data from 2018 to 2024
    recent_data = data.filter((col("Year") >= 2018) & (col("Year") <= 2024))
    monthly_sales = recent_data.groupBy("Year", "Month").agg(
        sum("Sale (Dollars)").alias("Monthly Total Sales")
    ).orderBy("Year", "Month")
    
    # Convert to Pandas DataFrame and pivot
    monthly_sales_pdf = monthly_sales.toPandas().pivot(index="Month", columns="Year", values="Monthly Total Sales").fillna(0)

    # Plot line chart
    plt.figure(figsize=(14, 8))
    for year in monthly_sales_pdf.columns:
        plt.plot(monthly_sales_pdf.index, monthly_sales_pdf[year], marker='o', label=f"Year {year}")

    # Set intervals and range
    plt.xticks(range(1, 13), [f"Month {i}" for i in range(1, 13)], rotation=45, fontsize=10, color='black')
    plt.xlim(0.5, 12.5)  # Leave space on both sides of the horizontal axis
    plt.ylim(0, monthly_sales_pdf.values.max() * 1.2)  # Ensure top margin space

    # Add title and axis labels
    plt.title("Monthly Sales Trends (2018-2024)", fontsize=14, color='black')
    plt.xlabel("Month", fontsize=12, color='black')
    plt.ylabel("Total Sales (Dollars)", fontsize=12, color='black')
    plt.yticks(fontsize=10, color='black')

    # Add gridlines and legend
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.legend(title="Year", fontsize=10, bbox_to_anchor=(1.05, 1), loc='upper left')

    # Save the chart
    plt.tight_layout(pad=3)
    plt.savefig(os.path.join(output_dir, "line_chart_monthly_sales_centered.png"))
    print("Line Chart for Monthly Sales Trends saved.")

# ************ Stacked Bar Chart - Quarterly Sales Trends *********************
def stacked_bar_chart_quarterly_sales(data, output_dir):
    # Filter data from 2018 to 2024
    recent_data = data.filter((col("Year") >= 2018) & (col("Year") <= 2024))
    quarterly_sales = recent_data.groupBy("Year", "Quarter").agg(
        sum("Sale (Dollars)").alias("Quarterly Total Sales")
    ).orderBy("Quarter", "Year")
    
    # Convert to Pandas DataFrame and pivot
    quarterly_sales_pdf = quarterly_sales.toPandas().pivot(index="Quarter", columns="Year", values="Quarterly Total Sales").fillna(0)

    # Plot stacked bar chart
    quarterly_sales_pdf.plot(kind='bar', stacked=True, figsize=(14, 8), colormap="viridis", alpha=0.85)
    plt.title("Stacked Quarterly Sales Trends (2018-2024)", fontsize=14, color='black')
    plt.xlabel("Quarter", fontsize=12, color='black')
    plt.ylabel("Total Sales (Dollars)", fontsize=12, color='black')
    plt.xticks(rotation=0, fontsize=10, color='black')
    plt.yticks(fontsize=10, color='black')
    plt.legend(title="Year", fontsize=10, bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout(pad=3)
    plt.savefig(os.path.join(output_dir, "stacked_bar_chart_quarterly_sales.png"))
    print("Stacked Bar Chart for Quarterly Sales Trends saved.")

# ************ Holiday Sales Analysis - Grouped Bar Chart *********************
def holiday_sales_analysis(data, output_dir):
    def get_holidays(year):
        thanksgiving = datetime.date(year, 11, 1) + datetime.timedelta(days=(3 - datetime.date(year, 11, 1).weekday() + 7) % 7 + 21)
        holidays = {
            "New Year": [(datetime.date(year, 1, 1) + datetime.timedelta(days=i)).strftime('%Y-%m-%d') for i in range(-3, 4)],
            "Valentine's Day": [(datetime.date(year, 2, 14) + datetime.timedelta(days=i)).strftime('%Y-%m-%d') for i in range(-3, 4)],
            "Halloween": [(datetime.date(year, 10, 31) + datetime.timedelta(days=i)).strftime('%Y-%m-%d') for i in range(-3, 4)],
            "Thanksgiving": [(thanksgiving + datetime.timedelta(days=i)).strftime('%Y-%m-%d') for i in range(-3, 4)],
            "Christmas": [(datetime.date(year, 12, 25) + datetime.timedelta(days=i)).strftime('%Y-%m-%d') for i in range(-3, 4)]
        }
        return holidays

    def add_holiday_column(df):
        distinct_years = [row["Year"] for row in df.select("Year").distinct().collect()]
        holidays_dict = {
            datetime.datetime.strptime(date, "%Y-%m-%d").date(): holiday
            for year in distinct_years
            for holiday, dates in get_holidays(year).items()
            for date in dates
        }
        holidays_udf = udf(lambda date: holidays_dict.get(date, None))
        return df.withColumn("Holiday", holidays_udf(col("Date")))

    # Filter data from 2018 to 2024
    data_filtered = data.filter((col("Year") >= 2018) & (col("Year") <= 2024))
    data_with_holidays = add_holiday_column(data_filtered)

    holiday_sales = data_with_holidays.filter(col("Holiday").isNotNull()).groupBy("Holiday", "Year").agg(
        sum("Sale (Dollars)").alias("Total Sales")
    ).orderBy("Year", col("Holiday"))

    holiday_sales_pdf = holiday_sales.toPandas()

    if holiday_sales_pdf.empty:
        print("No data available for holiday sales!")
        return

    # Plot grouped bar chart
    plt.figure(figsize=(14, 8))
    holidays = sorted(holiday_sales_pdf["Holiday"].unique())
    x = range(len(holidays))
    width = 0.12  # Bar width for each year

    for i, year in enumerate(sorted(holiday_sales_pdf["Year"].unique())):
        year_data = holiday_sales_pdf[holiday_sales_pdf["Year"] == year]
        sales = [year_data[year_data["Holiday"] == holiday]["Total Sales"].sum() if holiday in year_data["Holiday"].values else 0 for holiday in holidays]
        plt.bar([p + i * width for p in x], sales, width=width, label=f"Year {year}")

    plt.title("Holiday Sales Comparison (2018-2024)", fontsize=14, color='black')
    plt.xlabel("Holiday", fontsize=12, color='black')
    plt.ylabel("Total Sales (Dollars)", fontsize=12, color='black')
    plt.xticks([p + width * (len(holiday_sales_pdf["Year"].unique()) / 2) for p in x], holidays, rotation=45, ha="right", fontsize=10, color='black')
    plt.yticks(fontsize=10, color='black')
    plt.legend(title="Year", fontsize=10, bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout(pad=3)
    plt.savefig(os.path.join(output_dir, "filtered_holiday_sales_comparison.png"))
    print("Filtered Holiday Sales Comparison plot saved.")

# ************ Main Function Call *********************
if __name__ == "__main__":
    print("Starting analysis...")
    os.makedirs(output_dir, exist_ok=True)
    line_chart_monthly_sales(data, output_dir)  # Monthly line chart
    stacked_bar_chart_quarterly_sales(data, output_dir)  # Quarterly stacked bar chart
    holiday_sales_analysis(data, output_dir)  # Holiday grouped bar chart
    spark.stop()
    print("Analysis Completed!")
