# ML.py

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split

# ---------- Step 1: Load raw dataset ----------
# Replace with your actual dataset path
RAW_DATA_PATH = "data_warehouse/processed_sales_data.csv"
ENRICHED_DATA_PATH = "data_warehouse/VIP_Sales.csv"

df_raw = pd.read_csv("data_warehouse/processed_sales_data.csv")

# ---------- Step 2: Feature Engineering ----------
# Assume columns in df_raw: CustomerID, PurchaseAmount, PurchaseDate
# Calculate total purchase amount, purchase frequency, and average transaction value
feature_df = df_raw.groupby("customer_id").agg(
    TotalPurchase=("sale_price", "sum"),
    PurchaseFrequency=("sale_price", "count"),
    AvgTransactionValue=("sale_price", "mean")
).reset_index()

# ---------- Step 3: Handle Missing Values ----------
feature_df.fillna(0, inplace=True)

# ---------- Step 4: Normalize Features ----------
scaler = StandardScaler()
scaled_features = scaler.fit_transform(feature_df[["TotalPurchase", "PurchaseFrequency", "AvgTransactionValue"]])

# ---------- Step 5: Model Training ----------
# Example 1: Clustering approach (K-Means)
kmeans = KMeans(n_clusters=2, random_state=42, n_init=10)
feature_df["Cluster"] = kmeans.fit_predict(scaled_features)

# Determine which cluster is VIP (higher spending/frequency)
cluster_means = feature_df.groupby("Cluster")[["TotalPurchase", "PurchaseFrequency"]].mean()
vip_cluster = cluster_means["TotalPurchase"].idxmax()

feature_df["VIP_Status"] = np.where(feature_df["Cluster"] == vip_cluster, "VIP", "Non-VIP")
feature_df.drop(columns=["Cluster"], inplace=True)

# ---------- Step 6: Reverse ETL - Merge back to original dataset ----------
df_enriched = df_raw.merge(feature_df[["customer_id", "VIP_Status"]], on="customer_id", how="left")

# ---------- Step 7: Export enriched dataset ----------
df_enriched.to_csv(ENRICHED_DATA_PATH, index=False)

print(f"Enriched data saved to {"ENRICHED_DATA_PATH"}")
print(df_enriched.head())
