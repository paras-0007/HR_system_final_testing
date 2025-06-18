import os
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from utils.auth import get_google_credentials
from utils.logger import logger

class DriveHandler:
    def __init__(self):
        self.service = build('drive', 'v3', credentials=get_google_credentials())

    def upload_to_drive(self, file_path):
        """Upload file to Google Drive and return shareable link"""
        try:
            # Use os.path.basename for better cross-platform compatibility
            file_name = os.path.basename(file_path)
            file_metadata = {'name': file_name}
            media = MediaFileUpload(file_path, mimetype='application/pdf')

            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, webViewLink'
            ).execute()

            # Set permissions to get shareable link
            self.service.permissions().create(
                fileId=file['id'],
                body={'type': 'anyone', 'role': 'reader'}
            ).execute()

            return file.get('webViewLink')
        except Exception as e:
            logger.error(f"Drive upload failed: {str(e)}", exc_info=True)
            return None