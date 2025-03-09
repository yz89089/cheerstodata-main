import pandas as pd
from sklearn.linear_model import LinearRegression
from io import StringIO
import numpy as np
import matplotlib.pyplot as plt

# Load the sales data
file_path = "part-0.csv"  # Update this to match your file path
data = pd.read_csv(file_path)

# Convert county names to uppercase
data['County'] = data['County'].str.upper()

# Group by county to calculate order counts and total gallons sold
county_stats = data.groupby('County').agg(
    order_count=('Invoice/Item Number', 'count'),
    total_gallons=('Volume Sold (Gallons)', 'sum')
).reset_index()


# Load the vandalism data
vandalism_data = """
County	2022
Lyon	26
Osceola	9
Dickinson	45
Emmet	17
Kossuth	3
Winnebago	0
Worth	5
Mitchell	12
Howard	14
Winneshiek	5
Allamakee	26
Sioux	35
O'Brien	42
Clay	46
Palo Alto	18
Kossuth	3
Cerro Gordo	364
Floyd	30
Chickasaw	11
Fayette	31
Clayton	14
Plymouth	58
Cherokee	26
Buena Vista	81
Pocahontas	19
Humboldt	17
Wright	13
Franklin	10
Butler	1
Bremer	64
Woodbury	1164
Ida	13
Sac	22
Calhoun	12
Webster	466
Hamilton	33
Hardin	75
Grundy	12
Black Hawk	627
Buchanan	38
Delaware	22
Dubuque	533
Monona	21
Crawford	31
Carroll	32
Greene	30
Boone	11
Story	488
Marshall	192
Tama	9
Benton	47
Linn	1336
Jones	39
Jackson	110
Harrison	25
Shelby	3
Audubon	6
Guthrie	15
Dallas	208
Polk	1598
Jasper	115
Poweshiek	46
Iowa	52
Johnson	717
Cedar	25
Clinton	373
Scott	1961
Pottawattamie	467
Cass	53
Adair	4
Madison	16
Warren	145
Marion	99
Mahaska	91
Keokuk	27
Washington	127
Louisa	24
Mills	41
Montgomery	29
Adams	4
Union	45
Clarke	38
Lucas	25
Monroe	16
Wapello	338
Jefferson	21
Henry	64
Des Moines	330
Fremont	14
Page	87
Taylor	4
Ringgold	4
Decatur	0
Wayne	12
Appanoose	76
Davis	4
Van Buren	13
Lee	176
"""

# Convert vandalism data to DataFrame
vandalism_df = pd.read_csv(StringIO(vandalism_data), sep="\t")
vandalism_df['County'] = vandalism_df['County'].str.upper()

# Merge the sales and vandalism data (normal method)
merged_data = pd.merge(county_stats, vandalism_df, on='County', how='inner').sort_values('County')

# Split data into training (first 60 counties) and testing (rest)
train_data = merged_data.iloc[:60]
test_data = merged_data.iloc[60:]
X_train = train_data[['order_count', 'total_gallons']].values
y_train = train_data['2022'].values
X_test = test_data[['order_count', 'total_gallons']].values
y_test = test_data['2022'].values

# Fit the model on training data (Standard Method)
model = LinearRegression()
model.fit(X_train, y_train)

# Get weights, intercept, and predictions for the test set
w1, w2 = model.coef_
b = model.intercept_
y_pred_test = model.predict(X_test)
loss_test_standard = np.mean((y_test - y_pred_test) ** 2)

# EWMA Method
data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
data = data.dropna(subset=['Date'])  # Remove rows with invalid dates
data['Year'] = data['Date'].dt.year  # Extract year for grouping

# Aggregate yearly data
ewma_data = data.groupby(['County', 'Year']).agg(
    order_count=('Invoice/Item Number', 'count'),
    total_gallons=('Volume Sold (Gallons)', 'sum')
).reset_index()

# Calculate EWMA for each county
ewma_grouped = ewma_data.groupby('County', group_keys=False).apply(
    lambda x: pd.DataFrame({
        'County': x['County'],
        'Year': x['Year'],
        'ewma_order_count': x['order_count'].ewm(span=3).mean(),
        'ewma_total_gallons': x['total_gallons'].ewm(span=3).mean()
    }, index=x.index)
).reset_index(drop=True)

# Extract latest EWMA values for each county
ewma_summary = ewma_grouped.groupby('County').agg(
    ewma_order_count=('ewma_order_count', 'last'),
    ewma_total_gallons=('ewma_total_gallons', 'last')
).reset_index()

# Merge EWMA with vandalism data
merged_ewma_data = pd.merge(ewma_summary, vandalism_df, on='County', how='inner').sort_values('County')

# Split data for EWMA method
train_ewma_data = merged_ewma_data.iloc[:60]
test_ewma_data = merged_ewma_data.iloc[60:]
X_ewma_train = train_ewma_data[['ewma_order_count', 'ewma_total_gallons']].values
y_ewma_train = train_ewma_data['2022'].values
X_ewma_test = test_ewma_data[['ewma_order_count', 'ewma_total_gallons']].values
y_ewma_test = test_ewma_data['2022'].values

# Fit the model on training data (EWMA Method)
model_ewma = LinearRegression()
model_ewma.fit(X_ewma_train, y_ewma_train)

# Get weights, intercept, and predictions for the test set
w1_ewma, w2_ewma = model_ewma.coef_
b_ewma = model_ewma.intercept_
y_ewma_pred_test = model_ewma.predict(X_ewma_test)
loss_test_ewma = np.mean((y_ewma_test - y_ewma_pred_test) ** 2)

# Print results
print("Linear Regression (Normal Method):")
print(f"w1: {w1}, w2: {w2}, b: {b}")
print(f"Loss (Test Set - Normal Method): {loss_test_standard}")

print("\nLinear Regression (EWMA Method):")
print(f"w1_ewma: {w1_ewma}, w2_ewma: {w2_ewma}, b_ewma: {b_ewma}")
print(f"Loss (Test Set - EWMA Method): {loss_test_ewma}")

# Plotting the results
plt.figure(figsize=(14, 6))

# Plot for Standard Linear Regression
plt.subplot(1, 2, 1)
plt.plot(y_test, label="True Values", marker='o')
plt.plot(y_pred_test, label="Predicted Values (Linear Regression)", marker='x')
plt.title("Standard Linear Regression")
plt.xlabel("Test Sample Index")
plt.ylabel("Vandalism Rate")
plt.legend()

# Plot for EWMA Method
plt.subplot(1, 2, 2)
plt.plot(y_ewma_test, label="True Values", marker='o')
plt.plot(y_ewma_pred_test, label="Predicted Values (EWMA)", marker='x')
plt.title("EWMA Method")
plt.xlabel("Test Sample Index")
plt.ylabel("Vandalism Rate")
plt.legend()

# Show the plots
plt.tight_layout()
plt.show()
