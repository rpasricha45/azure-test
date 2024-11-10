from dataclasses import dataclass
from typing import Dict, List, Optional, Any
import pandas as pd

@dataclass
class TabAnalysis:
    """Represents the analysis results of an Excel tab"""
    score: float
    header_row_index: Optional[int]
    matched_patterns: Dict[str, List[str]]
    column_mapping: Optional[Dict[str, str]] = None

@dataclass
class RowGroup:
    """Represents a group of related rows in the rent roll"""
    unit_info: Dict[str, Any]
    primary_row: pd.Series
    secondary_rows: List[pd.Series]

@dataclass
class ProcessingConfig:
    """Configuration for rent roll processing"""
    min_tab_score: int = 25
    header_search_rows: int = 20
    column_patterns: Dict[str, List[str]] = None
    pattern_weights: Dict[str, int] = None
    min_header_score: int = 4

    def __post_init__(self):
        if self.column_patterns is None:
            self.column_patterns = {
                'unit': ['unit', 'apt', 'room', 'apartment', 'number', 'suite'],
                'resident': ['resident', 'tenant', 'name', 'occupant'],
                'rate': ['rate', 'rent', 'charge', 'fee', 'payment'],
                'date': ['move', 'date', 'admission'],
                'care': ['care', 'level', 'service']
            }
        
        if self.pattern_weights is None:
            self.pattern_weights = {
                'unit': 10,
                'resident': 10,
                'rate': 10,
                'date': 5,
                'care': 5
            } 