import logging
import json
import tempfile
import os
import asyncio
import azure.functions as func
from app import RentRollProcessor

# Add version info
__version__ = "1.0.0"

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_storage_connection():
    """Verify storage connection settings"""
    connection = os.environ.get("AzureWebJobsStorage")
    if not connection:
        raise ValueError("AzureWebJobsStorage connection string not found in environment variables")
    logging.info("Storage connection string verified")
    return connection

async def process_rent_roll_blob(blob_data: bytes, filename: str) -> str:
    """Process rent roll data from a blob and return the output path"""
    os.makedirs('output', exist_ok=True)
    
    temp_path = None
    try:
        # Save to temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(filename)[1]) as temp_file:
            temp_file.write(blob_data)
            temp_path = temp_file.name
        
        # Process the file
        await RentRollProcessor.process_file(temp_path)
        
        # Get output path
        output_filename = os.path.splitext(filename)[0] + '.csv'
        return os.path.join('output', output_filename)
        
    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
            except Exception as e:
                logger.warning(f"Failed to delete temporary file {temp_path}: {str(e)}")

app = func.FunctionApp()

@app.function_name(name="RentRollProcessor")
@app.blob_trigger(arg_name="blob", 
                 path="rentrolls/{name}",
                 connection="AzureWebJobsStorage")
@app.blob_output(arg_name="outputblob",
                path="processed/{name}.csv",
                connection="AzureWebJobsStorage")
async def process_rent_roll(blob: func.InputStream, outputblob: func.Out[str]) -> None:
    # Verify storage connection at startup
    get_storage_connection()
    
    try:
        logging.info(f"Processing blob: {blob.name}")
        
        # Read blob data
        blob_data = blob.read()
        
        # Process file
        output_path = await process_rent_roll_blob(blob_data, blob.name)
        
        # Write to output blob
        if os.path.exists(output_path):
            with open(output_path, 'r') as f:
                outputblob.set(f.read())
            logging.info(f"Successfully processed {blob.name}")
        else:
            raise FileNotFoundError(f"Output file not found: {output_path}")
        
    except Exception as e:
        logging.error(f"Error processing {blob.name}: {str(e)}")
        raise