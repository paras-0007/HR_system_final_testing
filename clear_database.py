from modules.database_handler import DatabaseHandler
from utils.logger import logger
import sys

def clear_database():
    """
    A standalone script to completely wipe and reset the project's database tables.
    """
    print("This script will permanently delete all applicant and communication data.")
    if sys.version_info.major < 3:
        response = raw_input("Are you sure you want to continue? (yes/no): ")
    else:
        response = input("Are you sure you want to continue? (yes/no): ")
        
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    logger.info("Initializing database handler to clear tables...")
    db_handler = DatabaseHandler()
    
    if db_handler.clear_all_tables():
        logger.info("Database has been cleared successfully.")
        logger.info("You can now run main.py to create the new tables and process data.")
    else:
        logger.error("An error occurred while clearing the database.")

if __name__ == "__main__":
    clear_database()