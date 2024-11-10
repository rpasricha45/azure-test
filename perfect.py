import pandas as pd
import os
from src.preprocessor import RentRollPreprocessor
from src.row_grouper import RowGrouper
from src.utils.logging import setup_logging
import asyncio
from typing import Optional, Dict, List

logger = setup_logging()

class RentRollProcessor:
    @staticmethod
    async def analyze_headers(file_path: str):
        """Analyze header rows for each tab."""
        processor = RentRollPreprocessor(file_path)
        
        for sheet_name in processor.workbook.sheet_names:
            print(f"\nAnalyzing Tab: {sheet_name}")
            print("=" * 60)
            
            try:
                df = pd.read_excel(processor.workbook, sheet_name,
                                 header=None, 
                                 nrows=None,
                                 skiprows=0)
                
                print("\nFirst 5 rows:")
                print(df.head().to_string())
                
                analysis = await processor._analyze_single_tab(df)
                print(f"\nScore: {analysis.score}")
                print(f"Header Row Index: {analysis.header_row_index}")
                
                if analysis.header_row_index is not None:
                    print("\nMatched Patterns:")
                    for pattern_type, matches in analysis.matched_patterns.items():
                        if matches:
                            print(f"{pattern_type}: {matches}")
                
            except Exception as e:
                logger.error(f"Error analyzing tab {sheet_name}: {str(e)}")

    @staticmethod
    async def process_file_to_df(file_path: str) -> pd.DataFrame:
        """Process a rent roll file and return DataFrame."""
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")
            
        processor = RentRollPreprocessor(file_path)
        logger.info(f"Processing file: {file_path}")
        
        try:
            # Initialize variables to track best tab
            best_score = -1
            best_tab_name = None
            best_analysis = None
            
            # Analyze all tabs to find the best one
            logger.info("Analyzing tabs to find best candidate...")
            for sheet_name in processor.workbook.sheet_names:
                logger.debug(f"Analyzing tab: {sheet_name}")
                
                df = pd.read_excel(processor.workbook, sheet_name,
                                 header=None, 
                                 nrows=None,
                                 skiprows=0)
                
                analysis = await processor._analyze_single_tab(df)
                logger.debug(f"Tab {sheet_name} score: {analysis.score}")
                
                if analysis.score > best_score:
                    best_score = analysis.score
                    best_tab_name = sheet_name
                    best_analysis = analysis
                    logger.info(f"New best tab found: {sheet_name} (score: {best_score})")

            if not best_tab_name or not best_analysis:
                raise ValueError("No valid tabs were found in the Excel file")

            logger.info(f"Selected best tab: {best_tab_name}")
            
            # Generate column mapping if needed
            if best_analysis.column_mapping is None:
                logger.info("Generating column mapping...")
                # First read without headers to get the header row
                df_raw = pd.read_excel(
                    processor.workbook,
                    best_tab_name,
                    header=None
                )
                clean_headers = processor.get_clean_headers(df_raw, best_analysis.header_row_index)
                
                # Read again with proper headers for data
                df = pd.read_excel(
                    processor.workbook,
                    best_tab_name,
                    header=best_analysis.header_row_index
                )
                
                # Try AI mapping first
                best_analysis.column_mapping = await processor.map_columns_with_ai(clean_headers, df)
                
                # Fall back to rule-based if AI mapping fails
                if not best_analysis.column_mapping:
                    logger.warning("AI mapping failed, falling back to rule-based mapping")
                    best_analysis.column_mapping = processor._create_column_mapping(df.columns)

            if not best_analysis.column_mapping:
                raise ValueError("Could not generate valid column mapping")

            # Process the best tab
            logger.info("Processing selected tab with column mapping...")
            df = pd.read_excel(
                processor.workbook,
                best_tab_name,
                header=best_analysis.header_row_index
            )
            
            # Group rows and prepare export data
            logger.info("Grouping rows and preparing export data...")
            export_data = []
            row_grouper = RowGrouper()
            groups = row_grouper.group_rows(df, best_analysis.column_mapping)
            
            for group in groups:
                # Add primary row
                row_data = {
                    'unit_number': group.unit_info['number'],
                    'unit_type': group.unit_info['type'],
                    'rate': group.unit_info['rate'],
                    'resident': group.unit_info['resident'],
                    'move_in_date': group.unit_info['move_in_date'],
                    'is_primary': True
                }
                for col in group.primary_row.index:
                    if (str(col) not in row_data and 
                        pd.notna(group.primary_row[col]) and 
                        str(group.primary_row[col]).strip()):
                        row_data[str(col)] = group.primary_row[col]
                export_data.append(row_data)
                
                # Add secondary rows
                for sec_row in group.secondary_rows:
                    sec_data = {
                        'unit_number': group.unit_info['number'],
                        'unit_type': group.unit_info['type'],
                        'rate': group.unit_info['rate'],
                        'resident': group.unit_info['resident'],
                        'move_in_date': group.unit_info['move_in_date'],
                        'is_primary': False
                    }
                    for col in sec_row.index:
                        if (str(col) not in sec_data and 
                            pd.notna(sec_row[col]) and 
                            str(sec_row[col]).strip()):
                            sec_data[str(col)] = sec_row[col]
                    export_data.append(sec_data)
            
            # Create and clean DataFrame
            logger.info("Creating final DataFrame...")
            if not export_data:
                raise ValueError("No data to export after processing")
                
            df_export = pd.DataFrame.from_records(export_data)
            
            # Clean up DataFrame
            logger.info("Cleaning DataFrame...")
            # Drop entirely empty columns
            df_export = df_export.dropna(axis=1, how='all')
            
            # Drop columns that are all empty strings
            columns_to_drop = []
            for col in df_export.columns:
                if df_export[col].astype(str).str.strip().eq('').all():
                    columns_to_drop.append(col)
            
            if columns_to_drop:
                df_export = df_export.drop(columns=columns_to_drop)
                logger.info(f"Dropped {len(columns_to_drop)} empty columns")
            
            logger.info("Processing completed successfully")
            return df_export
            
        except Exception as e:
            logger.error(f"Error processing rent roll: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    @staticmethod
    async def process_file(file_path: str) -> Optional[pd.DataFrame]:
        """Process a rent roll file and return the DataFrame."""
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return None
            
        try:
            logger.info(f"Starting processing of file: {file_path}")
            return await RentRollProcessor.process_file_to_df(file_path)
            
        except Exception as e:
            logger.error(f"Failed to process file: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None

if __name__ == "__main__":
    # Example usage
    file_path = "data/test/sample.xlsx"
    asyncio.run(RentRollProcessor.process_file(file_path))