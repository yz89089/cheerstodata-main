# Data_Processing_Cleaning

## Overview
The objective of this data cleaning task is to preprocess the Iowa Liquor Sales dataset to ensure data integrity, consistency, and accuracy, providing a reliable foundation for subsequent data analysis and modeling. The original data was exported from the official website, and the cleaned data has been saved to a designated local directory for further processing.

## Overview of the Original Data
- Source: The data was sourced from the official Iowa Liquor Sales website and downloaded using its official export functionality.
- Format: CSV file.
- Fields: The dataset contains 24 fields, including Store Name, Date, Sale (Dollars), Bottles Sold, City, etc.
- Number of Records: The dataset contains a total of 30,522,849 rows.

## Data Cleaning Process
### Loading Data Using a Custom Schema
- To ensure the accuracy of field data types, a custom schema was used when loading the data. The schema explicitly defines the name and data type of each field (e.g., StringType, IntegerType, DoubleType), avoiding errors that might arise from automatic type inference. This step lays the foundation for data formatting and subsequent processing.

### Removing Redundant Fields
- To optimize data processing efficiency and save storage space, redundant fields that are not directly relevant to the analysis were removed:
  - County Number: Duplicative information that is not necessary to retain.
  - Volume Sold (Gallons): Redundant with Volume Sold (Liters).

### Data Type Conversion and Filtering
- Explicit data type conversion was performed on key fields, and invalid data was filtered out:
  - Type Conversion:
    - Converted Sale (Dollars) to DoubleType.
    - Converted Date to the yyyy-MM-dd date format.
  - Filtering Rules:
Removed records where Sale (Dollars) <= 0 or Bottles Sold <= 0.

### Removing Null Values
- Records with null values in key fields were removed to ensure data integrity. Key fields include:
  - Invoice/Item Number
  - Date
  - Store Number

### Standardizing Field Values
- The City field was standardized to ensure data consistency:
  - Removed extra spaces.
  - Converted all text to lowercase.

### Removing Duplicate Records
- Duplicate records were removed based on the "Invoice/Item Number" field to ensure the uniqueness of each record in the dataset.

### Caching Data
- The cleaned data was cached to optimize processing efficiency, facilitating subsequent operations and validations.

## Validation of Data Cleaning
### Row Count:
The total number of rows in the cleaned dataset was verified using the .count() method.
### Field Statistics:
The .describe() method was used to generate statistical summaries for numerical fields, including:
- Bottle Volume (ml)
- Sale (Dollars)
- Bottles Sold
- Volume Sold (Liters)
### Data Preview: 
The first five rows of the cleaned data were displayed using .show() to verify the format and content of the fields.

## Add Locations from OpenStreetMap (OSM)
There is a column in our dataset called “Store Location” which gives us the longitude and latitude coordinates of the liquor store in each record. This information could be useful if we want to perform regional and geospatial analysis. However, not all records have this information. We have three observations on the missing data:

* **Observation 1:** A same store might have location info from some records, but missing in other records. For these records, we can simply fill in the missing data with existing dataset by a few Spark dataframe operations.
* **Observation 2:** Some stores do not have the location coordinates, but have other location information such as Address and City. For these stores, we might use the address and search the longitude and latitude coordinates from external sources such as the [OpenStreetMap](https://www.openstreetmap.org/#map=19/41.586263/-93.627269).
* **Observation 3:** The other stores have no location info at all, no street address, no city. All we have is the name of the store. For these stores, hopefully we can find some information from external sources. If not, we would just simply drop them.

With these observations, we implemented our filling location data algorithm in two steps.

### Step 1: Fill Location Data from Existing Dataset
We selected a table of unique stores with name and location columns, and filtered out the stores of which the locations are empty. This table serves as a reference of existing stores and their locations. We then join back this reference table with the original dataset, and fill in the location info from the reference table whenever it is empty. This step successfully filled in 487,380 records.

### Step 2: Fill Location Data from OSM with Address and City Info
We filtered the records with missing locations and grouped by store name, and found out that there are altogether 249 stores not having location coordinates. We used the python library [geopy](https://geopy.readthedocs.io/en/stable/) to access OpenStreetMap’s geocoding service through its [Nominatim API](https://geopy.readthedocs.io/en/stable/#nominatim). To increase the geocoding accuracy, we used the query that concatenated the address with city information and a string of ‘Iowa, USA’. After retrieving the location coordinates, we saved them in the correct format to a reference table as described in Step 1, and joined back to the original dataset to update the location column. This step successfully filled in 1,381,871 records.

### Step 3 (Abandoned): Fill Location Data from OSM with only Store Name
We also tried to search the address, city, and location info via geopy by only querying the store name in Iowa. However, the retrieved data is too noisy to use, so we abandoned this step and just dropped all records without locations after Step 1 and 2.

## Summary
- Through the above cleaning steps, redundant information and invalid records in the original data were successfully removed, retaining only fields and records directly related to sales analysis. The cleaned dataset has the following features:
Clear fields and data types, meeting the requirements for analysis and modeling.
Significantly improved data consistency and integrity.
Optimized data volume for efficient storage and computation.
The cleaned data has been saved to a designated local directory and is ready for further analysis and modeling tasks.


