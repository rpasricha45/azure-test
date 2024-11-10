from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta
import os
import logging

logger = logging.getLogger(__name__)

class AzureStorageClient:
    def __init__(self):
        conn_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        if not conn_str:
            raise ValueError("Azure Storage connection string not found in environment variables")
        
        self.service_client = BlobServiceClient.from_connection_string(conn_str)
        
    def upload_file(self, file_path: str, blob_name: str, container: str = "rentrolls") -> str:
        """Upload a file to specified Azure Blob Storage container"""
        try:
            # Get the container client
            container_client = self.service_client.get_container_client(container)
            
            # Create container if it doesn't exist
            if not container_client.exists():
                container_client.create_container()
            
            # Get blob client
            blob_client = container_client.get_blob_client(blob_name)
            
            # Upload the file
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
                
            return f"{container}/{blob_name}"
            
        except Exception as e:
            logger.error(f"Error uploading file to blob storage: {str(e)}")
            raise
            
    def get_download_url(self, blob_path: str) -> str:
        """Generate a SAS URL for downloading a blob from processed container"""
        try:
            # Always get from processed container
            blob_client = self.service_client.get_blob_client(
                container="processed",
                blob=blob_path
            )
            
            # Generate SAS token valid for 24 hours
            sas_token = generate_blob_sas(
                account_name=self.service_client.account_name,
                container_name="processed",
                blob_name=blob_path,
                account_key=self.service_client.credential.account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + timedelta(hours=24)
            )
            
            return f"{blob_client.url}?{sas_token}"
            
        except Exception as e:
            logger.error(f"Error generating download URL: {str(e)}")
            raise