"""
Batch Data Pipeline Simulation
--------------------------------
This program simulates a batch processing pipeline without Airflow or Spark.
It runs sequential tasks like:
1. Extract data
2. Validate data
3. Transform data
4. Aggregate results
5. Save output
"""

from datetime import datetime
import time
import random
import statistics

# -----------------------------
# Utility Functions
# -----------------------------

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def sleep_short():
    time.sleep(0.5)


# -----------------------------
# Step 1: Extract Data
# -----------------------------

def extract_data():
    log("Starting data extraction...")
    sleep_short()

    # Simulate reading data
    data = [random.randint(10, 100) for _ in range(20)]

    log(f"Extracted {len(data)} records")
    return data


# -----------------------------
# Step 2: Validate Data
# -----------------------------

def validate_data(data):
    log("Validating data...")
    sleep_short()

    valid_data = []
    for value in data:
        if isinstance(value, int) and value > 0:
            valid_data.append(value)

    log(f"Validated {len(valid_data)} records")
    return valid_data


# -----------------------------
# Step 3: Transform Data
# -----------------------------

def transform_data(data):
    log("Transforming data...")
    sleep_short()

    transformed = []
    for value in data:
        transformed.append(value * 2)

    log("Data transformation completed")
    return transformed


# -----------------------------
# Step 4: Aggregate Data
# -----------------------------

def aggregate_data(data):
    log("Aggregating data...")
    sleep_short()

    result = {
        "count": len(data),
        "min": min(data),
        "max": max(data),
        "average": round(statistics.mean(data), 2)
    }

    log("Aggregation completed")
    return result


# -----------------------------
# Step 5: Save Output
# -----------------------------

def save_results(results):
    log("Saving results...")
    sleep_short()

    print("\n===== FINAL REPORT =====")
    for key, value in results.items():
        print(f"{key.upper():10}: {value}")
    print("========================\n")

    log("Results saved successfully")


# -----------------------------
# Pipeline Controller
# -----------------------------

def run_pipeline():
    log("Batch pipeline started")

    data = extract_data()
    valid_data = validate_data(data)
    transformed_data = transform_data(valid_data)
    results = aggregate_data(transformed_data)
    save_results(results)

    log("Batch pipeline completed success     fully")


# -----------------------------
# Main Entry Point
# -----------------------------

if __name__ == "__main__":
    start_time = datetime.now()
    run_pipeline()
    end_time = datetime.now()

    duration = (end_time - start_time).total_seconds()
    log(f"Total execution time: {duration} seconds")
