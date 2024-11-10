import pytest
import pandas as pd
import os
from pathlib import Path
import sys

# Add the project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from function_app import RentRollProcessor

# Create fixture for test files
@pytest.fixture
def test_files_dir():
    """Fixture to provide path to test files directory"""
    return Path(__file__).parent / "test_files"

@pytest.fixture
def sample_rent_roll(test_files_dir):
    """Fixture to provide path to a sample rent roll file"""
    file_path = test_files_dir / "sample_rent_roll.xlsx"
    if not file_path.exists():
        pytest.skip(f"Test file not found: {file_path}")
    return str(file_path)

class TestRentRollProcessor:
    
    @pytest.mark.asyncio
    async def test_header_detection(self, sample_rent_roll):
        """Test header row detection for each tab."""
        processor = RentRollProcessor(sample_rent_roll)
        
        try:
            # Process the file
            result = await processor.process_file(sample_rent_roll)
            
            # Assert the result contains expected data
            assert result is not None
            # Add more specific assertions based on your expected output
            
        except Exception as e:
            pytest.fail(f"Test failed with exception: {str(e)}")

    @pytest.mark.asyncio
    async def test_row_grouping(self, sample_rent_roll):
        """Test row grouping functionality"""
        processor = RentRollProcessor(sample_rent_roll)
        
        try:
            # Process the file
            result = await processor.process_file(sample_rent_roll)
            
            # Add assertions to verify row grouping
            assert result is not None
            # Add more specific assertions based on your expected output
            
        except Exception as e:
            pytest.fail(f"Test failed with exception: {str(e)}")