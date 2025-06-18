import time
from config import CHECK_INTERVAL
from utils.logger import logger
from utils.file_utils import setup_temp_dir
from modules.email_handler import EmailHandler
from modules.drive_handler import DriveHandler
from modules.pdf_processor import FileProcessor
from modules.ai_classifier import AIClassifier
from modules.database_handler import DatabaseHandler

class HRClassifier:
    def __init__(self):
        self.email_handler = EmailHandler()
        self.drive_handler = DriveHandler()
        self.file_processor = FileProcessor()
        self.ai_classifier = AIClassifier()
        self.db_handler = DatabaseHandler()
        self.processed_message_ids = set()

    def run(self):
        logger.info("Starting HR Email Classifier")
        self.db_handler.create_tables()
        try:
            while True:
                # 1. Process brand-new application emails
                self.process_new_applications()
                
                # 2. Process replies in ongoing conversations
                self.process_replies()
                
                logger.info(f"Waiting for {CHECK_INTERVAL} seconds before next check...")
                time.sleep(CHECK_INTERVAL)
        except KeyboardInterrupt:
            logger.info("Application stopped by user.")
        except Exception as e:
            logger.critical(f"A critical error occurred in the main loop: {str(e)}", exc_info=True)

    def process_new_applications(self):
        logger.info("Checking for new applications...")
        messages = self.email_handler.fetch_unread_emails()
        if not messages:
            logger.info("No new applications found.")
            return
        for msg in messages:
            if msg['id'] in self.processed_message_ids: continue
            self.process_single_email(msg['id'])
            self.processed_message_ids.add(msg['id'])

    def process_replies(self):
        logger.info("Checking for replies in active threads...")
        active_threads = self.db_handler.get_active_threads()
        
        for applicant_id, thread_id in active_threads:
            messages_in_thread = self.email_handler.fetch_new_messages_in_thread(thread_id)
            
            # Get IDs of messages already in our DB for this applicant
            convos = self.db_handler.get_conversations(applicant_id)
            known_ids = set(convos['gmail_message_id'].tolist()) if not convos.empty else set()

            for msg_summary in messages_in_thread:
                msg_id = msg_summary['id']
                if msg_id in known_ids or msg_id in self.processed_message_ids:
                    continue

                email_data = self.email_handler.get_email_content(msg_id)
                if not email_data:
                    self.processed_message_ids.add(msg_id)
                    continue
                
                comm_data = {
                    "applicant_id": applicant_id, "gmail_message_id": email_data['id'],
                    "sender": email_data['sender'], "subject": email_data['subject'],
                    "body": email_data['body'], "direction": "Incoming"
                }
                
                self.db_handler.insert_communication(comm_data)
                self.processed_message_ids.add(msg_id)
                logger.info(f"New reply from applicant {applicant_id} (message: {msg_id}) has been saved.")

    def process_single_email(self, msg_id):
        logger.info(f"Processing new application with email ID: {msg_id}")
        try:
            email_data = self.email_handler.get_email_content(msg_id)
            if not email_data: return

            file_path = self.email_handler.save_attachment(msg_id)
            if not file_path:
                logger.warning(f"No processable attachment in email {msg_id}. Skipping.")
                self.email_handler.mark_as_read(msg_id)
                return

            drive_url = self.drive_handler.upload_to_drive(file_path)
            resume_text = self.file_processor.extract_text(file_path)

            ai_data = self.ai_classifier.extract_info(email_data['subject'], email_data['body'], resume_text)
            
            applicant_data = {**ai_data, 'Email': email_data['sender'], 'CV_URL': drive_url}
            
            applicant_id = self.db_handler.insert_applicant_and_communication(applicant_data, email_data)
            
            if applicant_id:
                self.email_handler.mark_as_read(msg_id)
        except Exception as e:
            logger.error(f"Failed to process email {msg_id}: {str(e)}", exc_info=True)

if __name__ == "__main__":
    classifier = HRClassifier()
    classifier.run()