import pandas as pd
from typing import List, Dict, Any, Optional
import logging
from src.models import RowGroup
from src.utils.logging import setup_logging

class RowGrouper:
    def __init__(self):
        self.logger = setup_logging(__name__)
        self.config = {
            'primary_row_indicators': {
                'required_fields': ['unit', 'rate']
            },
            'grouping_fields': {
                'unit_number': ['unit', 'apt', 'room', 'apartment', 'apt#', 'unit#']
            }
        }
    
    def get_unit_info(self, row: pd.Series, column_mapping: Dict[str, str]) -> Dict[str, Any]:
        unit_fields = self.config['grouping_fields']['unit_number']
        unit_number = []
        
        # Get the unit number from mapped column if it exists
        if 'unit' in column_mapping and column_mapping['unit'] is not None:
            unit_value = row[column_mapping['unit']]
            if pd.notna(unit_value):
                unit_number.append(str(unit_value))
        
        # Build unit info dict with safe handling of None values in mapping
        unit_info = {
            'number': '-'.join(unit_number) if unit_number else None,
            'type': str(row[column_mapping['type']]) if 'type' in column_mapping and column_mapping['type'] is not None else '',
            'rate': str(row[column_mapping['rate']]) if 'rate' in column_mapping and column_mapping['rate'] is not None else '',
            'resident': str(row[column_mapping['resident']]) if 'resident' in column_mapping and column_mapping['resident'] is not None else '',
            'move_in_date': str(row[column_mapping['date']]) if 'date' in column_mapping and column_mapping['date'] is not None else ''
        }
        
        return unit_info
        
    def group_rows(self, df: pd.DataFrame, column_mapping: Dict[str, str]) -> List[RowGroup]:
        groups = []
        current_group = None
        current_unit_number = None
        
        def should_start_new_group(row: pd.Series, mapping: Dict[str, str]) -> bool:
            """Check if this row should start a new group"""
            # If it has a unit number and either rate or resident info, it's a primary row
            unit_field = mapping.get('unit')
            rate_field = mapping.get('rate')
            resident_field = mapping.get('resident')
            
            has_unit = unit_field and pd.notna(row[unit_field])
            has_rate = rate_field and pd.notna(row[rate_field])
            has_resident = resident_field and pd.notna(row[resident_field])
            
            return has_unit and (has_rate or has_resident)
        
        for idx, row in df.iterrows():
            unit_info = self.get_unit_info(row, column_mapping)
            unit_number = unit_info['number']
            
            # Start new group if this looks like a primary row
            if should_start_new_group(row, column_mapping):
                if current_group is not None:
                    groups.append(current_group)
                
                current_group = RowGroup(
                    unit_info=unit_info,
                    primary_row=row,
                    secondary_rows=[]
                )
                current_unit_number = unit_number
            
            # Add as secondary row if we have a current group
            elif current_group is not None:
                # Don't filter out rows, just add them as secondary
                current_group.secondary_rows.append(row)
        
        # Don't forget the last group
        if current_group is not None:
            groups.append(current_group)
        
        return groups