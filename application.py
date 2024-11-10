from flask import Flask, request, jsonify
from perfect import RentRollProcessor
import asyncio
import tempfile
import os
from src.utils.storage import AzureStorageClient
from src.utils.logging import setup_logging
import pandas as pd
from io import StringIO

app = Flask(__name__)
logger = setup_logging()
storage_client = AzureStorageClient()

with app.app_context():
    logger.info("Initializing application...")

@app.route("/")
def home():
    logger.info("Home route accessed")
    return jsonify({
        "status": "online",
        "message": "Rent Roll Processor API is running"
    })
@app.route("/health")
def health():
    return jsonify({
        "status": "healthy"
    })

@app.route("/api/process", methods=["POST"])
def process_rent_roll():
    if "file" not in request.files:
        return jsonify({"error": "No file part"}), 400
    
    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No selected file"}), 400
        
    try:
        # Create a temporary file for initial upload
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as temp_file:
            file.save(temp_file.name)
            
            # Process the file and get DataFrame
            df = asyncio.run(RentRollProcessor.process_file_to_df(temp_file.name))
            
            # Convert DataFrame to CSV string
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()
            
            # Generate output filename
            output_filename = os.path.splitext(file.filename)[0] + ".csv"
            
            # Upload CSV directly to processed container
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as csv_temp:
                csv_temp.write(csv_content)
                csv_temp.flush()
                
                # Upload to processed container and get download URL
                blob_name = f"processed/{output_filename}"
                storage_client.upload_file(csv_temp.name, output_filename, container="processed")
                download_url = storage_client.get_download_url(output_filename)
                
                return jsonify({
                    "status": "success",
                    "message": "File processed successfully",
                    "download_url": download_url
                })
                
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        return jsonify({
            "error": str(e)
        }), 500
    finally:
        # Cleanup temporary files
        if "temp_file" in locals():
            try:
                os.unlink(temp_file.name)
            except:
                pass
        if "csv_temp" in locals():
            try:
                os.unlink(csv_temp.name)
            except:
                pass

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)