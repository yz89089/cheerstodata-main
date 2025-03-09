# SalesTrends_Analysis
## Analysis Objectives

- The main objectives of this analysis are to combine exploratory data analysis with machine learning to comprehensively explore sales trends in Iowa Liquor Sales data and forecast future sales. These objectives aim to support business decision-making. Specifically, the goals include:

  - Exploratory Analysis: Analyze sales patterns from 2018 to 2024, including monthly, quarterly, and holiday-specific trends, to identify seasonal and annual variations.
  - Data Visualization: Present sales data through graphical representations to make trends and patterns more intuitive and interpretable.
  - Sales Forecasting: Use machine learning techniques to predict future sales, with a particular focus on December 2024 sales performance.

## Methods and Tools

- Data Extraction and Preprocessing

  - Using PySpark for Data Processing:
    
     1. Utilized SparkSession to efficiently load the cleaned dataset, suitable for processing large-scale data.
    
     2. Extracted temporal features such as Month, Year, and Quarter using Spark SQL functions (e.g., month() and year()).
    
     3. Filtered data from 2018 to 2024 to ensure the analysis focuses on recent historical trends.
    
  - Data Aggregation
  
     1. Aggregated sales data by year and month (sum("Sale (Dollars")) to create a structured dataset for analysis.
     
     2. Converted the aggregated Spark DataFrame into a Pandas DataFrame for easier visualization and modeling.
    
- Exploratory Visualization Analysis
 
  - Monthly Sales Trend Analysis:
  
    1. Used Pandas pivot tables to transform monthly sales data, comparing monthly sales across years.
    
    2. Created line charts using Matplotlib to illustrate seasonal fluctuations and annual trends in monthly sales.
    
  - Quarterly Sales Trend Analysis:

    1. Aggregated sales data by quarter and year, visualized with stacked bar charts to show quarterly contributions to annual sales.
    
    2. Provided insights into the impact of quarterly sales on overall annual performance.
    
  - Holiday Sales Analysis:
  
    1. Defined key holidays (e.g., New Year, Valentine’s Day, Thanksgiving, and Christmas) and their time ranges.
   
    2. Used custom User-Defined Functions (UDFs) to match sales data with holiday periods.
   
    3. Aggregated sales data for holiday periods and visualized them using grouped bar charts to compare holiday sales across years.
   
- Machine Learning Forecasting Analysis
  
  - Objective:

    1. Use historical sales data to predict future sales trends and specific values.
   
    2. Focus on forecasting December 2024 sales, quantifying changes in future trends.

  - Feature Engineering:

    1. Constructed features based on monthly sales data, including Year, Quarter, and Month.
    
    2. Combined these features with historical total sales (Total Sales) to create a training dataset for machine learning.

  - Model Selection:

    1. Selected Random Forest Regressor (RandomForestRegressor) for the forecasting task due to its ability to handle non-linear relationships and provide feature importance insights.
    
    2. Split the data into training (80%) and testing (20%) sets and trained the model on the complete dataset for final predictions.

  - Model Evaluation:

    1. Evaluated model performance using Mean Squared Error (MSE) and R² score:
    
        MSE reflects the average error between predicted and actual values.
    
        R² measures the model's ability to explain the variability in the target variable.

  - Forecasting Process:

    1. Retrained the Random Forest model on the complete dataset.
    
    2. Predicted December 2024 sales using the trained model to provide actionable insights for future decision-making.


## Reasons for Tool Selection

- PySpark:

  - Efficiently processes large-scale data with distributed computing capabilities.
  
  - Built-in SQL-style API simplifies feature extraction and data filtering.

- Pandas:

  - Offers flexible data manipulation, especially for constructing pivot tables and preparing features for visualization and modeling.

- Matplotlib:

  - Provides highly customizable charting tools to effectively communicate trends and patterns in sales data.

- Scikit-learn:

  - Offers robust machine learning models and evaluation metrics, making it suitable for regression tasks.

- Custom Functions:

  - Leveraged custom User-Defined Functions (UDFs) to map holidays to specific dates, enabling deeper analysis of sales data related to special events.

## Challenge

- Data Loading Issues:

  - Problem: When using PySpark to load CSV files, automatic type inference caused mismatched data types in some fields, such as the date field being incorrectly identified as a string.

  - Solution: A custom schema was explicitly defined to specify the correct types for each field (e.g., StringType, DoubleType, DateType) during data loading, preventing issues caused by automatic inference.

- Complexity of Holiday Matching:

  - Problem: Analyzing holidays like Thanksgiving, which has a variable date, made it challenging to write accurate date range matching logic.

  - Solution: A user-defined function (UDF) was designed to dynamically generate date ranges for holidays and map them to the sales data, ensuring accurate holiday matching.

- Errors in Pivot Table Generation:

  - Problem: When converting a Spark DataFrame to a Pandas DataFrame and generating a pivot table, some fields contained missing values, causing the pivot table generation to fail.

  - Solution: Missing values were filled with 0 using fillna(0) before generating the pivot table, ensuring its completeness and preventing errors.

- Uneven Data Distribution:
  
  - Problem: Due to the seasonality of sales data, records for some months and quarters were significantly fewer than others, resulting in imbalanced data distribution for training.

  - Solution: Feature engineering, such as adding the Quarter feature, was applied to capture seasonal patterns. Additionally, care was taken to balance data distribution between training and testing datasets.

- Runtime Environment Errors:
  - Problem: Running Spark and Scikit-learn together caused script errors due to Python environment version mismatches or dependency conflicts.

  - Solution: A virtual environment was reconfigured to ensure compatibility between Spark and machine learning libraries, and dependency issues were resolved by tracking errors through detailed logs.

## Result Analysis:

- ![image](https://media.github.sfu.ca/user/3461/files/f25dd2ce-addf-4c9e-ba9d-f554f698e778)

  - Chart 1: Monthly Sales Trends (2018-2024)
Summary: The monthly sales trends indicate a steady growth in sales across most months of different years, with a noticeable spike in sales during the end of the year (November and December). This trend could be attributed to holiday seasons like Thanksgiving and Christmas. For certain years, such as 2024, data for November is missing, potentially due to data collection issues.
Insights: Sales exhibit seasonal fluctuations, with peaks at the end of the year and relatively stable performance during other months.

- ![image](https://media.github.sfu.ca/user/3461/files/d9ecbfaf-ccc2-4252-a8fa-6065fe090247)

  - Chart 2: Stacked Quarterly Sales Trends (2018-2024)
Summary: The stacked bar chart of quarterly sales shows a relatively even distribution across all quarters, with the fourth quarter (October to December) typically outperforming the others.
Insights: The fourth quarter contributes the highest portion to annual sales, likely due to increased holiday-related consumption. The trends across quarters remain consistent between years.

- ![image](https://media.github.sfu.ca/user/3461/files/00ae59b1-0cc7-404b-8be1-9b4ce2208d7a)

  - Chart 3: Holiday Sales Comparison (2018-2024)
Summary: The holiday sales comparison demonstrates that Thanksgiving and Christmas have significantly higher sales compared to other holidays. Sales during these holidays exhibit minor variations across years, but the overall trend is stable.
Insights: Thanksgiving and Christmas play a pivotal role in yearly sales performance, suggesting businesses could benefit from intensified promotional efforts in the lead-up to these holidays.

- ![image](https://media.github.sfu.ca/user/3461/files/3d4ba59c-4872-44e5-a2ee-3dd7a8e4f3a4)

  - Chart 4: Model Performance and Prediction
Summary: The Random Forest model demonstrates good performance, with an 𝑅*2=0.83, indicating that the model explains 83% of the variance in the target variable. However, the Mean Squared Error (MSE) is relatively high, likely due to the large scale of the sales data. The predicted total sales for December 2024 are 41,553,747.55 USD.
Insights: The model provides reasonable predictive value for December 2024 sales.








