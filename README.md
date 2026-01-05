# MongoDB Assignment - Retail Pricing Data

A Python project for transforming retail pricing data from CSV to MongoDB document format and demonstrating MongoDB CRUD operations and aggregation pipelines.

## Overview

This project processes retail product pricing data from multiple e-commerce platforms (Ajio, Amazon, Flipkart, Myntra, etc.), transforms it into structured MongoDB documents, and provides examples of database operations.

## Features

- **CSV to MongoDB Transformation**: Converts retail pricing CSV data into structured JSON documents
- **Data Processing**: Extracts product metadata, calculates pricing statistics (cheapest platform, price ranges, averages)
- **CRUD Operations**: Examples of Create, Read, Update, and Delete operations
- **Aggregation Pipelines**: Demonstrates complex queries for category analysis, platform comparison, and price variations
- **SQL Comparison**: Includes comparisons between MongoDB queries and equivalent SQL statements

## Project Structure

```
assignment4_mongodb/
├── data_transformer.py       # CSV to MongoDB document transformer
└── mongodb_examples.ipynb    # MongoDB operations examples and demos
```

## Dependencies

- `pandas` - CSV data processing
- `pymongo` - MongoDB driver
- `jupyter` - For running the notebook

## Usage

### Data Transformation

Transform CSV data to MongoDB documents:

```python
from data_transformer import transform_csv_to_documents

documents = transform_csv_to_documents('path/to/data.csv', limit=100)
```

### MongoDB Operations

See `mongodb_examples.ipynb` for complete examples of:
- Connecting to MongoDB Atlas
- CRUD operations (Create, Read, Update, Delete)
- Aggregation pipelines (category stats, platform analysis, price comparisons)
- MongoDB vs SQL query comparisons

## Document Structure

Each MongoDB document follows this structure:

```json
{
  "sku": "product_sku",
  "style_id": "style_id",
  "product": {
    "catalog": "catalog_name",
    "category": "category_name",
    "weight": 0.3,
    "size": "S"
  },
  "pricing": {
    "tp": 500,
    "mrp_old": 2000,
    "final_mrp_old": 2100
  },
  "ecommerce_platforms": {
    "ajio": 2100,
    "amazon": 2100,
    "flipkart": 2100
  },
  "metadata": {
    "cheapest_platform": "ajio",
    "cheapest_price": 2100,
    "price_range": 0,
    "min_price": 2100,
    "max_price": 2100,
    "avg_price": 2100,
    "created_at": "2024-01-01T00:00:00",
    "data_source": "May-2022"
  }
}
```

