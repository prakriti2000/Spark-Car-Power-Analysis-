# PySpark Car Analysis

This project demonstrates basic PySpark DataFrame operations including:

- Creating a Spark session
- Defining a schema
- Creating a DataFrame
- Adding new columns
- Performing transformations
- Displaying results

## Technologies Used

- Python
- Apache Spark (PySpark)
- PyCharm

## Problem Statement

As a data engineer, you are given the task to transform the given car dataset.

The transformed dataset should meet the following requirements:

- Correct the mis-spelled column name **`carr`** to **`car`**
- Add a new column named **`AvgWeight`** with a constant value of **200**
- Add another column named **`kilowatt_power`**, which should be **1000 times the horsepower value**

---


## Sample Data

The dataset contains car information:

| Car | Horsepower | Weight | Origin |
|----|----|----|----|
| Ford Torino | 140 | 3449 | US |
| Chevrolet Monte Carlo | 150 | 3761 | US |
| BMW | 113 | 2234 | Europe |

## Required Transformations

1. Rename column **`carr` → `car`**
2. Add a new column **`AvgWeight`** with a constant value **200**
3. Create a new column **`kilowatt_power` = horsepower * 1000**

---

## Expected Output Columns

| Column | Description |
|------|------|
| car | Corrected column name |
| horsepower | Engine horsepower |
| weight | Weight of the car |
| origin | Country/region of origin |
| AvgWeight | Constant value 200 |
| kilowatt_power | horsepower × 1000 |

---

## How to Run

Run the Spark job using:

```bash
spark-submit car.py
