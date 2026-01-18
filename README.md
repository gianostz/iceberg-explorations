# Iceberg Explorations

## Overview
The goal of this demo is to simulate the creation, population, and maintenance of Iceberg tables.

## Components
### **ParquetWriter**
The `ParquetWriter` simulates a streaming application responsible for writing files in **Parquet format** to populate the tables. The key challenge addressed in this demo is **duplicate management**.

### **Maintainer**
The `Maintainer` simulates a **Spark job** that runs periodically to **commit** files present in the Iceberg warehouse, handling duplication and performing maintenance operations such as **retention policies, vacuuming**, and more.

## Key Observations
By running the code, you can observe how **file writing** and **metadata management** are handled separately.

