import logging
import pandas as pd
from typing import Dict, List
from src.models import RowGroup
from src.row_grouper import RowGrouper
from .config import TEST_CONFIG, setup_test_env
import os

def setup_logging():
    """Configure logging for tests"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

async def process_and_export_data(df: pd.DataFrame, column_mapping: Dict[str, str]):
    """Process data and export results"""
    # Debug: Show all columns
    print("\nAll available columns:")
    for idx, col in enumerate(df.columns):
        print(f"  {idx}: {col}")
    
    print("\nFinal column mapping being used:")
    for key, value in column_mapping.items():
        print(f"  {key}: {value}")
    
    # Process groups
    grouper = RowGrouper()
    groups = grouper.group_rows(df, column_mapping)
    
    print(f"\nFound {len(groups)} row groups")
    
    # Display sample groups
    display_sample_groups(groups, column_mapping)
    
    # Export and show statistics
    await export_to_csv(groups)
    display_statistics(groups)

def display_sample_groups(groups: List[RowGroup], column_mapping: Dict[str, str]):
    """Display sample of first 3 groups"""
    print("\nSample of first 3 groups (with column details):")
    for i, group in enumerate(groups[:3]):
        print_group_info(group, column_mapping, i)

def print_group_info(group: RowGroup, column_mapping: Dict[str, str], index: int):
    """Print formatted group information"""
    print(f"\nGroup {index + 1}:")
    print("Primary row data:")
    for col in column_mapping.values():
        if col in group.primary_row:
            print(f"  {col}: {group.primary_row[col]}")
    
    print(f"Secondary rows: {len(group.secondary_rows)}")
    for j, secondary in enumerate(group.secondary_rows[:2]):
        print(f"  Secondary {j + 1}:")
        for col in column_mapping.values():
            if col in secondary:
                print(f"    {col}: {secondary[col]}")

async def export_to_csv(groups: List[RowGroup]):
    """Export processed data to CSV"""
    setup_test_env()
    csv_data = prepare_csv_data(groups)
    output_path = os.path.join(TEST_CONFIG['OUTPUT_DIR'], TEST_CONFIG['DEFAULT_OUTPUT'])
    
    df_output = pd.DataFrame(csv_data)
    df_output.to_csv(output_path, index=False)
    print(f"\nData exported to: {output_path}")

def display_statistics(groups: List[RowGroup]):
    """Display group statistics"""
    groups_with_secondary = sum(1 for g in groups if g.secondary_rows)
    print("\nGroup Statistics:")
    print(f"Total groups: {len(groups)}")
    print(f"Groups with secondary residents: {groups_with_secondary}")
    if len(groups) > 0:
        avg_secondary = sum(len(g.secondary_rows) for g in groups)/len(groups)
        print(f"Average secondary residents per group: {avg_secondary:.2f}")

def prepare_csv_data(groups: List[RowGroup]) -> List[Dict]:
    """Convert groups to CSV-ready format"""
    csv_data = []
    
    for group in groups:
        # Process primary resident
        primary_data = create_resident_data(group, group.primary_row, True)
        csv_data.append(primary_data)
        
        # Process secondary residents
        for sec_row in group.secondary_rows:
            secondary_data = create_resident_data(group, sec_row, False)
            csv_data.append(secondary_data)
            
    return csv_data

def create_resident_data(group: RowGroup, row: pd.Series, is_primary: bool) -> Dict:
    """Create a data dictionary for a resident"""
    data = {
        'unit_number': group.unit_info.get('number', ''),
        'unit_type': group.unit_info.get('type', ''),
        'unit_rate': group.unit_info.get('rate', ''),
        'primary_resident': is_primary,
    }
    
    # Add non-empty columns from row
    for col in row.index:
        if (col not in data and 
            not str(col).startswith('Unnamed:') and 
            pd.notna(row[col])):
            data[col] = row[col]
            
    return data 