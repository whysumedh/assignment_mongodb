import pandas as pd
import json
import os
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
import re


def extract_size_from_sku(sku: str) -> str:
    if pd.isna(sku) or not isinstance(sku, str):
        return None
    size_pattern = r'_([SMXL\d]+XL?)$'
    match = re.search(size_pattern, sku)
    if match:
        return match.group(1)
    return None


def standardize_platform_name(platform_col: str) -> str:
    platform = platform_col.replace(' MRP', '').lower().replace(' ', '_')
    return platform


def safe_float(value, default=0.0):
    if value is None:
        return default
    try:
        # Check if it's pandas NaN
        if hasattr(value, '__class__') and pd.isna(value):
            return default
    except (TypeError, ValueError):
        pass
    try:
        # Try to convert to float
        result = float(value)
        # Check for NaN/Inf after conversion
        import math
        if math.isnan(result) or math.isinf(result):
            return default
        return result
    except (ValueError, TypeError):
        return default


def calculate_cheapest_platform(row: pd.Series, platform_cols: List[str]) -> tuple:
    prices = {}
    for col in platform_cols:
        val = safe_float(row[col], default=None)
        if val is not None and val > 0:
            platform_name = standardize_platform_name(col)
            prices[platform_name] = val
    
    if prices:
        cheapest = min(prices.items(), key=lambda x: x[1])
        return cheapest[0], cheapest[1]
    return None, None


def transform_row_to_document(row: pd.Series) -> Dict[str, Any]:
    """
    
    Structure:
    {
        "sku": "...",
        "style_id": "...",
        "product": {
            "catalog": "...",
            "category": "...",
            "weight": ...
        },
        "pricing": {
            "tp": ...,
            "mrp_old": ...,
            "final_mrp_old": ...
        },
        "ecommerce_platforms": {
            "ajio": ...,
            "amazon": ...,
            ...
        },
        "metadata": {
            "cheapest_platform": "...",
            "cheapest_price": ...,
            "price_range": ...,
            "created_at": "..."
        }
    }
    """
    platform_cols = [col for col in row.index if 'MRP' in col and col not in ['MRP Old', 'Final MRP Old']]
    
    ecommerce_platforms = {}
    for col in platform_cols:
        val = safe_float(row[col], default=None)
        if val is not None and val > 0:
            platform_name = standardize_platform_name(col)
            ecommerce_platforms[platform_name] = val
    
    cheapest_platform, cheapest_price = calculate_cheapest_platform(row, platform_cols)
    
    platform_prices = []
    for col in platform_cols:
        val = safe_float(row[col], default=None)
        if val is not None and val > 0:
            platform_prices.append(val)
    price_range = max(platform_prices) - min(platform_prices) if platform_prices else 0
    
    document = {
        "sku": str(row.get('Sku', '')),
        "style_id": str(row.get('Style Id', '')),
        "product": {
            "catalog": str(row.get('Catalog', '')),
            "category": str(row.get('Category', '')),
            "weight": safe_float(row.get('Weight', 0)),
            "size": extract_size_from_sku(str(row.get('Sku', '')))
        },
        "pricing": {
            "tp": safe_float(row.get('TP', 0)),
            "mrp_old": safe_float(row.get('MRP Old', 0)),
            "final_mrp_old": safe_float(row.get('Final MRP Old', 0))
        },
        "ecommerce_platforms": ecommerce_platforms,
        "metadata": {
            "cheapest_platform": cheapest_platform,
            "cheapest_price": cheapest_price,
            "price_range": float(price_range),
            "min_price": float(min(platform_prices)) if platform_prices else 0,
            "max_price": float(max(platform_prices)) if platform_prices else 0,
            "avg_price": float(sum(platform_prices) / len(platform_prices)) if platform_prices else 0,
            "created_at": datetime.now().isoformat(),
            "data_source": "May-2022"
        }
    }
    
    return document


def transform_csv_to_documents(csv_path: str, limit: int = None) -> List[Dict[str, Any]]:

    print(f"Loading CSV from {csv_path}...")
    df = pd.read_csv(csv_path)
    
    if limit:
        df = df.head(limit)
        print(f"Limiting to {limit} records")
    
    print(f"Transforming {len(df)} records to MongoDB documents...")
    
    documents = []
    for idx, row in df.iterrows():
        try:
            doc = transform_row_to_document(row)
            documents.append(doc)
        except Exception as e:
            print(f"Error transforming row {idx}: {e}")
            continue
    
    print(f"Successfully transformed {len(documents)} documents")
    return documents


def save_documents_to_json(documents: List[Dict[str, Any]], output_path: str):
    """Save documents to JSON file for inspection."""
    with open(output_path, 'w') as f:
        json.dump(documents, f, indent=2, default=str)
    print(f"Documents saved to {output_path}")


def main():
    """Main function to transform data."""
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    csv_file = project_root / 'data' / 'May-2022.csv'
    
    documents = transform_csv_to_documents(str(csv_file), limit=100)  
    
    output_file = script_dir / 'sample_documents.json'
    save_documents_to_json(documents[:10], str(output_file))
    
    print(f"\nSample document structure:")
    print(json.dumps(documents[0], indent=2))
    
    return documents


if __name__ == "__main__":
    documents = main()

