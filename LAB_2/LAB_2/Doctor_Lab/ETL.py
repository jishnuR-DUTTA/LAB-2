# -------------------------------
# Stage 1: Business Analyst Task
# -------------------------------
# Business Question:
# What are the top 5 treatments by total cost in the last quarter,
# and how do patient satisfaction scores vary for these treatments?
# Required data points:
# treatment_id, patient_id, doctor_id, treatment_type,
# treatment_cost, room_cost, treatment_date, satisfaction_score

# -------------------------------
# Stage 2: Data Engineer Task
# -------------------------------

import pandas as pd
import matplotlib.pyplot as plt
import os

# Define file paths directly (full path or relative path with raw string)
treatment_file = r"Raw_data\patients_data_with_doctor.csv"
feedback_file = r"Raw_data\patient_feedback.json"

# 1. Ingestion
treatment_df = pd.read_csv(treatment_file)
feedback_df = pd.read_json(feedback_file)

# 2. Cleansing

# Remove currency symbols from treatment_cost and room_cost, convert to float
treatment_df['treatment_cost'] = treatment_df['treatment_cost'].replace(r'[\$,]', '', regex=True).astype(float)
treatment_df['room_cost'] = treatment_df['room_cost'].replace(r'[\$,]', '', regex=True).astype(float)

# Standardize treatment_date and review_date formats
treatment_df['treatment_date'] = pd.to_datetime(treatment_df['treatment_date'], format='mixed', errors='coerce')
feedback_df['review_date'] = pd.to_datetime(feedback_df['review_date'], format='mixed', errors='coerce')

# 3. Transformation

# Compute total treatment expense
treatment_df['total_cost'] = treatment_df['treatment_cost'] + treatment_df['room_cost']

# Keep the latest feedback per (treatment_id, patient_id)
feedback_df = feedback_df.sort_values('review_date').drop_duplicates(
    subset=['treatment_id', 'patient_id'], keep='last'
)


# Merge treatments and feedback (will keep all treatments, feedback may be missing)
merged_df = pd.merge(
    treatment_df,
    feedback_df,
    on=['treatment_id', 'patient_id'],
    how='left'
)

# Remove invalid entries (zero or missing cost)
merged_df = merged_df[
    (merged_df['total_cost'] > 0)
]

# 4. Loading to warehouse
processed_path = r"data_warehouse\processed_treatment_data.csv"
merged_df.to_csv(processed_path, index=False)

print(f"Processed data saved to {processed_path}")
