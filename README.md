# Data Fragmentation & Partitioning in PostgreSQL – CSE 511

This repository contains Python script, that demonstrates **horizontal fragmentation** of relational datasets in PostgreSQL using **range-based** and **round-robin partitioning**.

The script provides functions to:
- Load data from CSV files into PostgreSQL tables.
- Perform **range partitioning** using PostgreSQL declarative partitioning.
- Simulate **round-robin partitioning** with **triggers** and table inheritance.
- Insert and manage data across partitioned tables.
- Validate query results and ensure correctness.

The project also supports deploying PostgreSQL in a **Dockerized Linux environment** for scalable data management.
