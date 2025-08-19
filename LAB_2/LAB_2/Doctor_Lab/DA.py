# -------------------------------
#  Data Analyst Task
# -------------------------------

import pandas as pd
import matplotlib.pyplot as plt
import os

# Define processed file path
processed_path = os.path.join("data_warehouse", "processed_treatment_data.csv")

# 1. Access processed data
df = pd.read_csv(processed_path)

# 2. Analyse: top 5 treatments by total cost
top_treatments = (
    df.groupby('treatment_id')['total_cost']
    .sum()
    .sort_values(ascending=False)
    .head(5)
    .reset_index()
)

# Merge to get average satisfaction
avg_satisfaction = (
    df.groupby('treatment_id')['patient_feedback_score']
    .mean()
    .reset_index()
    .rename(columns={'patient_feedback_score': 'avg_satisfaction'})
)

# Merge for final report
report_df = pd.merge(top_treatments, avg_satisfaction, on='treatment_id', how='left')

# 3. Communicate: Display separate charts

# Plot 1: Bar chart for Total Treatment Cost
plt.figure(figsize=(8, 5))
plt.bar(report_df['treatment_id'], report_df['total_cost'])
plt.title('Top 5 Treatments by Total Cost')
plt.xlabel('Treatment ID')
plt.ylabel('Total Cost')
plt.grid(True)
plt.tight_layout()
plt.show()


# Plot 2: Line plot for Average Satisfaction Score (only if data exists)
if report_df['avg_satisfaction'].notna().any():
    plt.figure(figsize=(8, 5))
    plt.plot(report_df['treatment_id'], report_df['avg_satisfaction'], color='green', marker='o', linewidth=2)
    plt.title('Average Satisfaction Score for Top 5 Treatments')
    plt.xlabel('Treatment ID')
    plt.ylabel('Average Satisfaction Score')
    plt.grid(True)
    plt.tight_layout()
    plt.show()
else:
    print("No satisfaction score data available for top treatments. Skipping satisfaction plot.")

# 4. Feedback
print("\n--- Feedback to Data Engineer ---")
if df['patient_feedback_score'].isnull().sum() > 0:
    print("Some satisfaction scores are missing. Consider collecting more feedback data.")
if df['treatment_date'].isnull().sum() > 0:
    print("Some treatment_date entries couldn't be parsed. Please ensure date consistency.")

print("\n--- Final Report ---")
print(report_df)
