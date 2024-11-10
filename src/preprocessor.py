import pandas as pd
import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from openai import AsyncOpenAI
import json
import os

@dataclass
class TabAnalysis:
    score: float
    header_row_index: Optional[int]
    matched_patterns: Dict[str, List[str]]
    column_mapping: Optional[Dict[str, str]] = None

@dataclass
class RowGroup:
    unit_info: Dict[str, Any]
    primary_row: pd.Series
    secondary_rows: List[pd.Series]

class RentRollPreprocessor:
    def __init__(self, excel_path: str):
        self.excel_path = str(excel_path).replace('\\', '/')
        self.config = self._get_default_config()
        self.logger = logging.getLogger(__name__)
        self.workbook = None
        self.tab_analyses = {}
        self.client = AsyncOpenAI()
        
        try:
            self.workbook = pd.ExcelFile(self.excel_path)
        except Exception as e:
            self.logger.error(f"Failed to load Excel file: {str(e)}")
            raise

    def _get_default_config(self) -> Dict:
        return {
            'min_tab_score': 25,
            'header_search_rows': 20,
            'column_patterns': {
                'unit': ['unit', 'apt', 'room', 'apartment', 'number', 'suite'],
                'resident': ['resident', 'tenant', 'name', 'occupant'],
                'rate': ['rate', 'rent', 'charge', 'fee', 'payment'],
                'date': ['move', 'date', 'admission'],
                'care': ['care', 'level', 'service']
            },
            'pattern_weights': {
                'unit': 10,
                'resident': 10,
                'rate': 10,
                'date': 5,
                'care': 5
            },
            'min_header_score': 4
        }

    async def analyze_tabs(self) -> Dict[str, TabAnalysis]:
        """Analyze all tabs in the workbook."""
        self.tab_analyses = {}
        
        for sheet_name in self.workbook.sheet_names:
            try:
                df = pd.read_excel(self.workbook, sheet_name, header=None, nrows=None, skiprows=0)
                analysis = await self._analyze_single_tab(df)
                
                if analysis.header_row_index is not None:
                    clean_headers = self.get_clean_headers(df, analysis.header_row_index)
                    df_with_header = pd.read_excel(
                        self.workbook, 
                        sheet_name, 
                        header=analysis.header_row_index
                    )
                    analysis.column_mapping = await self.map_columns_with_ai(clean_headers, df_with_header)
                
                self.tab_analyses[sheet_name] = analysis
                
            except Exception as e:
                self.logger.error(f"Error analyzing tab {sheet_name}: {str(e)}")
                continue
                
        return self.tab_analyses

    async def _analyze_single_tab(self, df: pd.DataFrame) -> TabAnalysis:
        score = 0
        header_row_index = None
        matched_patterns = {pattern: [] for pattern in self.config['column_patterns'].keys()}

        try:
            df = df.astype(str).replace('nan', '')
            header_row_index = self._find_header_row(df)
            
            if header_row_index is not None:
                headers = df.iloc[header_row_index].astype(str).str.lower()
                
                for col, header in enumerate(headers):
                    for pattern_type, patterns in self.config['column_patterns'].items():
                        if any(pattern in header for pattern in patterns):
                            weight = self.config['pattern_weights'][pattern_type]
                            score += weight
                            matched_patterns[pattern_type].append(header)

            return TabAnalysis(score=score, header_row_index=header_row_index, matched_patterns=matched_patterns, column_mapping=None)
                
        except Exception as e:
            self.logger.error(f"Error in _analyze_single_tab: {str(e)}")
            return TabAnalysis(score=0, header_row_index=None, matched_patterns=matched_patterns, column_mapping=None)

    def _find_header_row(self, df: pd.DataFrame) -> Optional[int]:
        max_score = 0
        best_row = None
        
        for idx in range(min(self.config['header_search_rows'], len(df))):
            row = df.iloc[idx].astype(str).str.lower()
            score = 0
            
            for pattern_type, patterns in self.config['column_patterns'].items():
                if any(row.str.contains(pattern).any() for pattern in patterns):
                    score += self.config['pattern_weights'][pattern_type] // 5
                    
            if row.str.match(r'^\$?\d+\.?\d*$').any():
                score -= 2
                
            if score > max_score:
                max_score = score
                best_row = idx
                
        return best_row if max_score >= self.config['min_header_score'] else None

    def get_clean_headers(self, df: pd.DataFrame, header_row_index: int) -> List[str]:
        """Get non-blank headers from the detected header row."""
        headers = df.iloc[header_row_index].astype(str)
        # Filter out blank or NaN headers
        return [h for h in headers if h and h.strip() and h.lower() != 'nan']

    async def map_columns_with_ai(self, headers: List[str], df: pd.DataFrame) -> Dict[str, str]:
        """Use OpenAI to intelligently map column headers and sample data."""
        print("\nStarting AI column mapping...")
        
        clean_headers = [str(h).strip() for h in headers if h]
        print(f"Clean headers: {clean_headers}")
        
        # Get first 3 rows of data as examples, using column indices instead of names
        sample_data = []
        df_columns = df.columns.tolist()
        for _, row in df.head(3).iterrows():
            row_data = {}
            for header in clean_headers:
                # Find the closest matching column name
                matching_col = next(
                    (col for col in df_columns if str(col).strip() == header),
                    None
                )
                if matching_col is not None:
                    row_data[header] = str(row[matching_col]).strip()
                else:
                    print(f"Warning: Could not find matching column for header: {header}")
                    row_data[header] = "N/A"
            sample_data.append(row_data)
        
        print(f"\nSample data prepared: {json.dumps(sample_data, indent=2)}")
        
        prompt = f"""Given these column headers and sample data from a senior living rent roll:

Headers:
{clean_headers}

Sample Data (first 3 rows):
{json.dumps(sample_data, indent=2)}

Map the columns to these standard senior living categories, analyzing both header names AND sample data values:

1. unit: Column containing unique unit/room/apartment identifiers
   - Look for: Room numbers, unit IDs, apartment numbers 
   - Usually short alphanumeric codes (e.g., "101A", "2B", "U203")
   - Should have low repetition (typically <3 times in sample)
   - Example headers: RoomNumber, Unit, AptNum, UnitID

2. resident: Column containing resident names
   - Look for: Full names, typically First/Last name combinations
   - May include titles (Mr., Mrs., etc.)
   - Example headers: ResidentName, Tenant, OccupantName, Name

3. rate: Column containing monthly rent/rate amounts 
   - Look for: Numeric values with typical senior living price ranges ($2000-$10000)
   - May include currency symbols ($) or decimal points
   - Example headers: MonthlyRate, RentAmount, BaseRate, Rate

4. type: Column indicating level of care
   - Look for these specific values:
     - IL (Independent Living)
     - AL (Assisted Living)
     - MC (Memory Care)
     - NC (Nursing Care)
     - EAL (Enhanced Assisted Living)
   - Example headers: CareType, ResidentType, LevelOfCare, ServiceType, Type

5. date: Column containing resident move-in dates
   - Look for: Date formats (YYYY-MM-DD, MM/DD/YYYY, etc.)
   - Should contain valid calendar dates
   - Example headers: MoveInDate, AdmissionDT, StartDate, ResidencyDate

Rules:
- Return exactly ONE best matching column for each category
- Only use column headers that exist in the provided data
- If no good match exists for a category, set it to null
- Base matches on both header names AND actual data content
- Prioritize accuracy over completeness

Return a JSON object in this exact format:
{{
    "unit": "exact_matching_column_name",
    "resident": "exact_matching_column_name",
    "rate": "exact_matching_column_name", 
    "type": "exact_matching_column_name",
    "date": "exact_matching_column_name"
}}
"""

        print(f"\nPrompt being sent to OpenAI:\n{prompt}")

        try:
            print("\nCalling OpenAI API...")
            response = await self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system", 
                        "content": "You are a specialized assistant that maps rent roll columns exactly. Analyze both header names and data content. Return only the JSON object, no markdown formatting."
                    },
                    {
                        "role": "user", 
                        "content": prompt
                    }
                ],
                temperature=0
            )

            print("\nReceived response from OpenAI")
            raw_response = response.choices[0].message.content
            print(f"\nAI Response Raw: {raw_response}")

            # Clean and parse the response
            cleaned_response = raw_response.strip()
            if cleaned_response.startswith('```'):
                cleaned_response = cleaned_response.split('\n', 1)[1]
                cleaned_response = cleaned_response.rsplit('\n', 1)[0]
                cleaned_response = cleaned_response.replace('```json', '').replace('```', '').strip()

            print(f"Cleaned response: {cleaned_response}")

            try:
                mapping = json.loads(cleaned_response)
                print(f"Parsed mapping: {mapping}")
            except json.JSONDecodeError as e:
                print(f"JSON parsing error: {e}")
                return {}

            # Validate the mapping and find exact column matches
            valid_categories = {'unit', 'resident', 'rate', 'type', 'date'}
            df_columns = df.columns.tolist()
            validated_mapping = {}
            
            for k, v in mapping.items():
                if k in valid_categories:
                    if v is None:
                        # Keep the null value in the mapping
                        validated_mapping[k] = None
                    else:
                        # Find the exact column name from DataFrame that best matches the AI suggestion
                        matching_col = next(
                            (col for col in df_columns if str(col).strip() == str(v).strip()),
                            None
                        )
                        validated_mapping[k] = matching_col
            
            print(f"Final validated mapping: {validated_mapping}")
            return validated_mapping

        except Exception as e:
            self.logger.error(f"Error in AI column mapping: {str(e)}")
            print(f"\nFull error in AI mapping: {str(e)}")
            print("Falling back to rule-based mapping...")
            # Fall back to rule-based mapping
            return self._create_column_mapping(headers)

    def _create_column_mapping(self, columns) -> Dict[str, str]:
        column_mapping = {}
        columns_lower = [str(col).lower() for col in columns]
        
        for pattern_type, patterns in self.config['column_patterns'].items():
            for col, col_lower in zip(columns, columns_lower):
                if any(pattern in col_lower for pattern in patterns):
                    column_mapping[pattern_type] = col
                    break
        
        return column_mapping 

    async def extract_property_info(self) -> Tuple[str, str]:
        """Extract property name and as of date from the file and its contents."""
        print("\nExtracting property info...")
        
        # Get filename without extension for fallback
        file_name = os.path.basename(self.excel_path)
        base_name = os.path.splitext(file_name)[0]
        
        # Collect data from first 20 rows of each tab
        tab_data = []
        for sheet_name in self.workbook.sheet_names:
            try:
                df = pd.read_excel(self.workbook, sheet_name, header=None, nrows=20)
                # Convert to string and replace NaN/empty cells with empty string
                cleaned_rows = []
                for _, row in df.iterrows():
                    cleaned_row = []
                    for cell in row:
                        # Skip empty or NaN cells
                        if pd.isna(cell) or str(cell).strip() == '':
                            continue
                        cleaned_row.append(str(cell).strip())
                    if cleaned_row:  # Only add rows that have content
                        cleaned_rows.append(cleaned_row)
                
                if cleaned_rows:  # Only add tabs that have content
                    tab_data.append({
                        'tab_name': sheet_name,
                        'rows': cleaned_rows
                    })
            except Exception as e:
                self.logger.error(f"Error reading tab {sheet_name}: {str(e)}")
                continue

        prompt = f"""Analyze this information from a senior living rent roll Excel file to identify the property name and as of date:

File name: {base_name}

Tab data from first 20 rows of each sheet:
{json.dumps(tab_data, indent=2)}

Rules for property name:
- Look for phrases like "Property:", "Community:", "Facility:"
- Common locations: file name, sheet names, top rows of sheets
- Should be a real property name (e.g., "Heartis Peoria", "Harbor Court")
- Ignore generic text like "Rent Roll", "Report"

Rules for as of date:
- Look for phrases like "As of", "As of Period:", "Period:", "For Month Of"
- Common date formats: MM/DD/YYYY, YYYY-MM-DD, Month DD YYYY
- Focus on most recent/relevant date
- Ignore future dates or move-in dates

Return a JSON object in this exact format:
{{
    "property_name": "extracted property name",
    "as_of_date": "extracted date in MM-DD-YYYY format"
}}

If either value cannot be determined with confidence, use null."""

        print("\nData being sent to OpenAI:")
        print("=" * 80)
        print(f"File name: {base_name}")
        print("\nTab data:")
        print(json.dumps(tab_data, indent=2))
        print("=" * 80)

        try:
            response = await self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a specialized assistant that extracts property names and dates from rent roll files. Return only the JSON object, no additional text."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0
            )

            print("\nOpenAI Response:")
            print(response.choices[0].message.content)

            result = json.loads(response.choices[0].message.content)
            
            # Use original filename if AI extraction fails
            property_name = result.get('property_name')
            as_of_date = result.get('as_of_date')
            
            if not property_name or not as_of_date:
                print(f"\nFalling back to original filename: {base_name}")
                return base_name, ""  # Empty string for date when using filename
                
            return property_name, as_of_date

        except Exception as e:
            self.logger.error(f"Error extracting property info: {str(e)}")
            print(f"\nFalling back to original filename due to error: {base_name}")
            return base_name, ""  # Empty string for date when using filename