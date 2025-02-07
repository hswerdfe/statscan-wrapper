import polars as pl
import requests
import zipfile
import os
from pathlib import Path
import json
from typing import Optional

def get_cache_dir(custom_cache_dir: Optional[str] = None) -> Path:
    """Get or create cache directory"""
    cache_dir = Path(custom_cache_dir) if custom_cache_dir else Path.home() / ".statscan_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir

def get_table_url(table_id: str, language: str = 'eng') -> str:
    """
    Convert table ID to download URL
    
    Args:
        table_id (str): CANSIM table number (e.g., "14-10-0287")
        language (str): Language of the table ('eng' or 'fra'). Defaults to 'eng'
    
    Returns:
        str: URL to download the table
    """
    base_url = "https://www150.statcan.gc.ca/n1/tbl/csv/"
    clean_id = table_id.replace("-", "").replace(" ", "")
    return f"{base_url}{clean_id}-{language}.zip"

def download_table(table_id: str, cache_dir: Optional[str] = None, language: str = 'eng') -> Path:
    """
    Download and extract table data
    
    Args:
        table_id (str): CANSIM table number
        cache_dir (str, optional): Custom cache directory path
        language (str): Language of the table ('eng' or 'fra'). Defaults to 'eng'
    
    Returns:
        Path: Path to the main CSV file
    """
    cache_path = get_cache_dir(cache_dir)
    zip_path = cache_path / f"{table_id}-{language}.zip"
    table_dir = cache_path / f"{table_id}-{language}"
    csv_path = table_dir / f"{table_id}-{language}.csv"
    
    # Check if CSV already exists in cache
    if csv_path.exists():
        return csv_path
    
    # Create directory for table files
    table_dir.mkdir(parents=True, exist_ok=True)
    
    # Download zip file
    url = get_table_url(table_id, language)
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    # Save zip file
    with open(zip_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    # Extract all files
    with zipfile.ZipFile(zip_path) as zip_ref:
        zip_ref.extractall(table_dir)
        
        # Find the main CSV file (usually the first one)
        csv_filename = None
        for filename in zip_ref.namelist():
            if filename.lower().endswith('.csv'):
                csv_filename = filename
                break
        
        if not csv_filename:
            raise ValueError(f"No CSV file found in the zip file for table {table_id}")
            
        extracted_path = table_dir / csv_filename
        if extracted_path != csv_path:
            extracted_path.rename(csv_path)
    
    # Clean up zip file
    zip_path.unlink()
    
    return csv_path

def get_table(table_id: str, cache_dir: Optional[str] = None, language: str = 'eng') -> pl.DataFrame:
    """
    Fetch a StatsCan table and return it as a Polars DataFrame
    
    Args:
        table_id (str): CANSIM table number (e.g., "14-10-0287")
        cache_dir (str, optional): Custom cache directory path
        language (str): Language of the table ('eng' or 'fra'). Defaults to 'eng'
            
    Returns:
        pl.DataFrame: Table data as a Polars DataFrame
        
    Examples:
        >>> # Get Labour Force Survey data in English
        >>> df = get_table("14-10-0287")
        >>> 
        >>> # Get data in French with custom cache directory
        >>> df_fr = get_table("14-10-0287", cache_dir="/tmp/statscan", language="fra")
        >>> 
        >>> # Print first few rows
        >>> print(df.head())
        >>> 
        >>> # test french 
        >>> df_fr2 = get_table("14-10-0287", language="fra")
        >>> print(df_fr2.head())
   """
    try:
        csv_path = download_table(table_id, cache_dir, language)

        # Use semicolon delimiter for French files, comma for English
        delimiter = ";" if language == "fra" else ","
        df = pl.read_csv(csv_path, separator=delimiter)
        return df
    except Exception as e:
        raise Exception(f"Error fetching table {table_id}: {str(e)}") 