# statscan-wrapper

A Python wrapper for downloading and working with Statistics Canada data tables.

## Installation

bash
pip install statscan-wrapper

## Usage

```python
from statscan_wrapper import get_table

# Get Labour Force Survey data in English
df = get_table("14-10-0287")

# Get data in French
df_fr = get_table("14-10-0287", language="fra")

# Use custom cache directory
df = get_table("14-10-0287", cache_dir="/tmp/statscan")
```


## Features

- Download Statistics Canada tables directly as Polars DataFrames
- Automatic caching of downloaded files
- Support for both English and French tables
- Custom cache directory support

## Requirements

- Python 3.7+
- polars
- requests

## License

This project is licensed under the MIT License - see the LICENSE file for details.