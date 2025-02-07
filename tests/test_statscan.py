import pytest
import polars as pl
from pathlib import Path
import responses
import zipfile
import io
from statscan_wrapper.statscan import (
    get_cache_dir,
    get_table_url,
    download_table,
    get_table
)

@pytest.fixture
def temp_cache_dir(tmp_path):
    """Fixture to provide a temporary cache directory"""
    cache_dir = tmp_path / "statscan_test_cache"
    cache_dir.mkdir()
    return str(cache_dir)

@pytest.fixture
def sample_csv_content():
    """Fixture to provide sample CSV content"""
    return (
        "REF_DATE,GEO,VALUE\n"
        "2020-01,Canada,100\n"
        "2020-02,Canada,101\n"
    )

@pytest.fixture
def mock_zip_file(sample_csv_content):
    """Fixture to create a mock ZIP file with CSV content"""
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('14100287.csv', sample_csv_content)
    return zip_buffer.getvalue()

def test_get_cache_dir_default():
    """Test get_cache_dir with default settings"""
    cache_dir = get_cache_dir()
    assert cache_dir == Path.home() / ".statscan_cache"
    assert cache_dir.exists()
    assert cache_dir.is_dir()

def test_get_cache_dir_custom(temp_cache_dir):
    """Test get_cache_dir with custom path"""
    cache_dir = get_cache_dir(temp_cache_dir)
    assert str(cache_dir) == temp_cache_dir
    assert cache_dir.exists()
    assert cache_dir.is_dir()

def test_get_table_url():
    """Test URL generation for different table IDs"""
    test_cases = [
        ("21-10-0033", "eng", "21100033-eng"),
        ("21 10 0033", "fra", "21100033-fra"),
        ("21100033", "eng", "21100033-eng"),
    ]
    
    for input_id, lang, expected_id in test_cases:
        url = get_table_url(input_id, lang)
        expected_url = f"https://www150.statcan.gc.ca/n1/tbl/csv/{expected_id}.zip"
        assert url == expected_url

@responses.activate
def test_download_table(temp_cache_dir, mock_zip_file):
    """Test downloading and extracting table data"""
    table_id = "14-10-0287"
    url = get_table_url(table_id)
    
    # Mock the HTTP response
    responses.add(
        responses.GET,
        url,
        body=mock_zip_file,
        status=200,
        content_type='application/zip'
    )
    
    # Test downloading
    csv_path = download_table(table_id, temp_cache_dir)
    assert csv_path.exists()
    assert csv_path.suffix == '.csv'
    
    # Test caching
    csv_content = csv_path.read_text()
    assert "REF_DATE,GEO,VALUE" in csv_content
    
    # Test that ZIP file was cleaned up
    zip_path = Path(temp_cache_dir) / f"{table_id}.zip"
    assert not zip_path.exists()

@responses.activate
def test_get_table(temp_cache_dir, mock_zip_file, sample_csv_content):
    """Test getting table as Polars DataFrame"""
    table_id = "14-10-0287"
    url = get_table_url(table_id)
    
    # Mock the HTTP response
    responses.add(
        responses.GET,
        url,
        body=mock_zip_file,
        status=200,
        content_type='application/zip'
    )
    
    # Get the table
    df = get_table(table_id, temp_cache_dir)
    
    # Verify DataFrame contents
    assert isinstance(df, pl.DataFrame)
    assert df.shape == (2, 3)  # 2 rows, 3 columns
    assert list(df.columns) == ["REF_DATE", "GEO", "VALUE"]

@responses.activate
def test_get_table_error_handling(temp_cache_dir):
    """Test error handling when fetching table"""
    table_id = "invalid-table"
    url = get_table_url(table_id)
    
    # Mock a failed HTTP response
    responses.add(
        responses.GET,
        url,
        status=404,
        json={"error": "Not found"}
    )
    
    # Test that appropriate exception is raised
    with pytest.raises(Exception) as exc_info:
        get_table(table_id, temp_cache_dir)
    assert "Error fetching table" in str(exc_info.value)

def test_cache_reuse(temp_cache_dir, sample_csv_content):
    """Test that cached files are reused"""
    table_id = "14-10-0287"
    csv_path = Path(temp_cache_dir) / f"{table_id}.csv"
    
    # Create a cached CSV file
    csv_path.write_text(sample_csv_content)
    
    # Get table should use cached file without downloading
    df = get_table(table_id, temp_cache_dir)
    assert isinstance(df, pl.DataFrame)
    assert df.shape == (2, 3)

def test_get_table_real_api():
    """Test getting a real table from Statistics Canada"""
    table_id = "21-10-0033"  # A known working table
    df = get_table(table_id)
    
    # Basic validation of the returned DataFrame
    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] > 0  # Should have some rows
    assert df.shape[1] > 0  # Should have some columns
    assert "REF_DATE" in df.columns  # Standard column in StatsCan tables
    assert "GEO" in df.columns      # Standard column in StatsCan tables 