# Data Fragmentation & Partitioning in PostgreSQL – CSE 511

This project provides a **Python**-based solution for implementing and simulating horizontal data fragmentation strategies in **PostgreSQL**. Developed for CSE 511 (Data Processing at Scale), it demonstrates how partitioning can be used to optimize query performance and manage large datasets.

## Key Features

* **Efficient Data Loading:** A function to bulk-load relational data from CSV files into PostgreSQL tables.

* **Declarative Range Partitioning:** Implements range-based partitioning using PostgreSQL's native declarative features to segment data based on specific value ranges.

* **Simulated Round-Robin Partitioning:** Demonstrates a custom round-robin distribution scheme using **table inheritance** and **PL/pgSQL triggers**, a technique for databases that lack this native feature.

* **Validation and Integrity:** Includes methods for validating correct data distribution and ensuring data integrity across all table fragments.

## Environment

The entire project is designed to run within a **Dockerized Linux environment**, ensuring a consistent, reproducible, and isolated PostgreSQL instance for development and testing.
