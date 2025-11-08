# 🎬 Movie Data Engineering Pipeline

## 📌 Overview
This project is a **take-home assignment** for the **Data Engineer (Fresher)** position at **TSWorks Technologies**.  
It demonstrates the design and implementation of a simple **ETL data pipeline** that:
1. Extracts movie and rating data from the [MovieLens dataset](https://grouplens.org/datasets/movielens/latest/)
2. Enriches it using the **OMDb API**
3. Loads the cleaned and transformed data into a **SQLite database**
4. Answers key analytical questions using **SQL queries**

---

## 🧠 Objective
To design and implement a functional data pipeline that:
- Integrates multiple data sources (CSV + API)
- Cleans, transforms, and enriches raw data
- Loads structured data into a relational database
- Performs analytical queries for insights

---

## 🧩 Project Structure

ts works assignment/
│
├── etl.py # Main ETL pipeline (Extract, Transform, Load)
├── schema.sql # Database schema (table creation)
├── queries.sql # Analytical SQL queries
├── run_queries.py # Utility script to execute and display SQL results
├── movies.csv # Input movie dataset (MovieLens)
├── ratings.csv # Input rating dataset (MovieLens)
├── omdb_cache.json # Cached API responses (for faster re-runs)
├── movies.db # SQLite database (auto-created)
├── requirements.txt # Python dependencies
└── README.md # Project documentation


---

## ⚙️ Setup Instructions

### 1️⃣ Create and Activate a Virtual Environment
python -m venv tsenv
tsenv\Scripts\activate      # For Windows

### 2️⃣ Install Required Libraries
pip install -r requirements.txt



## 🚀 Running the Project
### 1️⃣ Run the ETL Pipeline
python etl.py --movies movies.csv --ratings ratings.csv --omdb-key YOUR_API_KEY


This will:

Read movies.csv and ratings.csv

Fetch additional metadata (Director, Plot, BoxOffice, etc.) from OMDb

Cache the results locally in omdb_cache.json

Create tables automatically in movies.db and insert data

🧩 Note:
If you hit the OMDb API rate limit, you can re-run later — cached data will prevent re-fetching.

### 2️⃣ Verify Database Creation

To check that data has been loaded successfully:

python
>>> import sqlite3
>>> conn = sqlite3.connect("movies.db")
>>> cursor = conn.cursor()
>>> cursor.execute("SELECT COUNT(*) FROM movies;").fetchone()
>>> cursor.execute("SELECT COUNT(*) FROM ratings;").fetchone()
>>> conn.close()

### 3️⃣ Run Analytical Queries
python run_queries.py


This script will:

Execute all queries in queries.sql

Display results neatly in the terminal
