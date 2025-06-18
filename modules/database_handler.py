import psycopg2
import pandas as pd
from utils.logger import logger
from config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT

class DatabaseHandler:
    # ... (init, _connect, create_tables remain the same) ...
    def __init__(self):
        self.conn_params = {
            "dbname": DB_NAME,
            "user": DB_USER,
            "password": DB_PASSWORD,
            "host": DB_HOST,
            "port": DB_PORT
        }
        self.conn = None
    def _connect(self):
        try:
            if self.conn is None or self.conn.closed: self.conn = psycopg2.connect(**self.conn_params)
        except psycopg2.OperationalError as e:
            logger.error(f"Could not connect to the database: {e}"); self.conn = None
    def create_tables(self):
        self._connect()
        if not self.conn: return
        queries = [
            """CREATE TABLE IF NOT EXISTS applicants (id SERIAL PRIMARY KEY, name VARCHAR(255), email VARCHAR(255) UNIQUE, phone VARCHAR(20), domain VARCHAR(255), education TEXT, job_history TEXT, cv_url TEXT, status VARCHAR(255) DEFAULT 'New', created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, gmail_thread_id VARCHAR(255));""",
            """CREATE TABLE IF NOT EXISTS communications (id SERIAL PRIMARY KEY, applicant_id INTEGER REFERENCES applicants(id) ON DELETE CASCADE, gmail_message_id VARCHAR(255) UNIQUE, sender TEXT, subject TEXT, body TEXT, direction VARCHAR(50), sent_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP);""",
            """CREATE TABLE IF NOT EXISTS export_logs (id SERIAL PRIMARY KEY, file_name VARCHAR(255), sheet_url TEXT, created_by VARCHAR(255), created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP);""",
            """CREATE TABLE IF NOT EXISTS applicant_statuses (id SERIAL PRIMARY KEY, status_name VARCHAR(255) UNIQUE NOT NULL);""",
            """CREATE TABLE IF NOT EXISTS interviewers (id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL, email VARCHAR(255) UNIQUE NOT NULL);""",
            """CREATE TABLE IF NOT EXISTS interviews (id SERIAL PRIMARY KEY, applicant_id INTEGER REFERENCES applicants(id) ON DELETE CASCADE, interviewer_id INTEGER REFERENCES interviewers(id) ON DELETE SET NULL, event_title VARCHAR(255), start_time TIMESTAMP WITH TIME ZONE, end_time TIMESTAMP WITH TIME ZONE, google_calendar_event_id VARCHAR(255), status VARCHAR(50) DEFAULT 'Pending');"""
        ]
        try:
            with self.conn.cursor() as cur:
                for query in queries: cur.execute(query)
                self.conn.commit()
                logger.info("All tables are ready.")
        except Exception as e:
            logger.error(f"Error during initial table creation: {e}"); self.conn.rollback(); return
        self._populate_initial_statuses()

    # --- NEW METHODS for logging and retrieving interviews ---
    def log_interview(self, applicant_id, interviewer_id, title, start_time, end_time, event_id):
        """Logs a scheduled interview in the database."""
        self._connect()
        if not self.conn: return False
        sql = """
        INSERT INTO interviews (applicant_id, interviewer_id, event_title, start_time, end_time, google_calendar_event_id, status)
        VALUES (%s, %s, %s, %s, %s, %s, 'Scheduled');
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, (applicant_id, interviewer_id, title, start_time, end_time, event_id))
                self.conn.commit()
                logger.info(f"Successfully logged interview for applicant {applicant_id}")
                return True
        except Exception as e:
            logger.error(f"Failed to log interview: {e}", exc_info=True)
            self.conn.rollback()
            return False

    def get_interviews_for_applicant(self, applicant_id):
        """Fetches all scheduled interviews for a specific applicant."""
        self._connect()
        if not self.conn: return pd.DataFrame()
        query = """
        SELECT i.event_title, i.start_time, i.status, iv.name as interviewer_name
        FROM interviews i
        LEFT JOIN interviewers iv ON i.interviewer_id = iv.id
        WHERE i.applicant_id = %s
        ORDER BY i.start_time DESC;
        """
        try:
            df = pd.read_sql_query(query, self.conn, params=(applicant_id,))
            return df
        except Exception as e:
            logger.error(f"Error fetching interviews for applicant {applicant_id}: {e}")
            return pd.DataFrame()

    # ... (all other existing methods remain the same)
    def get_interviewers(self):
        self._connect();
        if not self.conn: return pd.DataFrame()
        query = "SELECT id, name, email FROM interviewers ORDER BY name;"
        try: return pd.read_sql_query(query, self.conn)
        except Exception as e: logger.error(f"Error fetching interviewers: {e}"); return pd.DataFrame()
    def add_interviewer(self, name, email):
        self._connect();
        if not self.conn: return False
        sql = "INSERT INTO interviewers (name, email) VALUES (%s, %s) ON CONFLICT (email) DO NOTHING;"
        try:
            with self.conn.cursor() as cur: cur.execute(sql, (name, email)); self.conn.commit(); return cur.rowcount > 0
        except Exception as e: logger.error(f"Error adding interviewer '{name}': {e}"); self.conn.rollback(); return False
    def delete_interviewer(self, interviewer_id):
        self._connect();
        if not self.conn: return False
        sql = "DELETE FROM interviewers WHERE id = %s;"
        try:
            with self.conn.cursor() as cur: cur.execute(sql, (interviewer_id,)); self.conn.commit(); return True
        except Exception as e: logger.error(f"Error deleting interviewer {interviewer_id}: {e}"); self.conn.rollback(); return False
    def _populate_initial_statuses(self):
        self._connect();
        if not self.conn: return
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM applicant_statuses;")
                if cur.fetchone()[0] == 0:
                    default_statuses = ["New", "Screening", "Interview Round 1", "Task Sent", "Interview Round 2", "Offer", "Rejected", "Hired"]
                    insert_query = "INSERT INTO applicant_statuses (status_name) VALUES (%s);"
                    for status in default_statuses: cur.execute(insert_query, (status,))
                    self.conn.commit(); logger.info("Populated applicant_statuses with default values.")
        except Exception as e: logger.error(f"Error populating default statuses: {e}"); self.conn.rollback()
    def get_statuses(self):
        self._connect();
        if not self.conn: return []
        try:
            with self.conn.cursor() as cur: cur.execute("SELECT status_name FROM applicant_statuses ORDER BY id;"); return [row[0] for row in cur.fetchall()]
        except Exception as e: logger.error(f"Error fetching statuses: {e}"); return []
    def add_status(self, status_name):
        self._connect();
        if not self.conn: return False
        sql = "INSERT INTO applicant_statuses (status_name) VALUES (%s) ON CONFLICT (status_name) DO NOTHING;"
        try:
            with self.conn.cursor() as cur: cur.execute(sql, (status_name,)); self.conn.commit(); return cur.rowcount > 0
        except Exception as e: logger.error(f"Error adding status '{status_name}': {e}"); self.conn.rollback(); return False
    def delete_status(self, status_name):
        self._connect();
        if not self.conn: return "Database connection failed."
        check_sql = "SELECT 1 FROM applicants WHERE status = %s LIMIT 1;"; delete_sql = "DELETE FROM applicant_statuses WHERE status_name = %s;"
        try:
            with self.conn.cursor() as cur:
                cur.execute(check_sql, (status_name,))
                if cur.fetchone(): return f"Cannot delete '{status_name}' as it is currently assigned to one or more applicants."
                cur.execute(delete_sql, (status_name,)); self.conn.commit()
                if cur.rowcount > 0: return None
                else: return f"Status '{status_name}' not found."
        except Exception as e: logger.error(f"Error deleting status '{status_name}': {e}"); self.conn.rollback(); return f"An unexpected error occurred: {e}"
    def delete_applicants(self, applicant_ids):
        if not applicant_ids: return False
        self._connect();
        if not self.conn: return False
        ids_tuple = tuple(applicant_ids) if isinstance(applicant_ids, list) else applicant_ids; sql = "DELETE FROM applicants WHERE id IN %s;"
        try:
            with self.conn.cursor() as cur: cur.execute(sql, (ids_tuple,)); self.conn.commit(); logger.info(f"Successfully deleted {cur.rowcount} applicants."); return True
        except Exception as e: logger.error(f"Error deleting applicants: {e}"); self.conn.rollback(); return False
    def clear_all_tables(self):
        self._connect();
        if not self.conn: return False
        drop_command = "DROP TABLE IF EXISTS applicants, communications, applicant_statuses, export_logs, interviewers, interviews CASCADE;"
        try:
            with self.conn.cursor() as cur: cur.execute(drop_command); self.conn.commit(); logger.info("Successfully dropped all application tables."); return True
        except Exception as e: logger.error(f"Error dropping tables: {e}"); self.conn.rollback(); return False
    def insert_applicant_and_communication(self, applicant_data, email_data):
        self._connect();
        if not self.conn: return None
        check_sql = "SELECT id FROM applicants WHERE email = %s;"; insert_applicant_sql = "INSERT INTO applicants (name, email, phone, domain, education, job_history, cv_url, gmail_thread_id, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;"; insert_comm_sql = "INSERT INTO communications (applicant_id, gmail_message_id, sender, subject, body, direction) VALUES (%s, %s, %s, %s, %s, 'Incoming');"
        try:
            with self.conn.cursor() as cur:
                cur.execute(check_sql, (applicant_data.get("Email"),))
                if cur.fetchone(): logger.info(f"Skipping duplicate applicant: {applicant_data.get('Email')}"); return None                
                cur.execute(insert_applicant_sql, (applicant_data.get("Name"), applicant_data.get("Email"), applicant_data.get("Phone"), applicant_data.get("Domain", "Other"), applicant_data.get("Education"), applicant_data.get("JobHistory"), applicant_data.get("CV_URL"), email_data.get("thread_id"), "New")); applicant_id = cur.fetchone()[0]
                cur.execute(insert_comm_sql, (applicant_id, email_data.get("id"), email_data.get("sender"), email_data.get("subject"), email_data.get("body"))); self.conn.commit(); logger.info(f"New applicant '{applicant_data.get('Name')}' and initial email inserted."); return applicant_id
        except Exception as e: logger.error(f"Error in combined insert: {e}", exc_info=True); self.conn.rollback(); return None
    def update_applicant_status(self, applicant_id, new_status):
        self._connect();
        if not self.conn: return False
        sql = "UPDATE applicants SET status = %s WHERE id = %s;"
        try:
            with self.conn.cursor() as cur: cur.execute(sql, (new_status, applicant_id)); self.conn.commit(); logger.info(f"Updated status for applicant {applicant_id} to '{new_status}'."); return True
        except Exception as e: logger.error(f"Error updating status: {e}"); self.conn.rollback(); return False
    def insert_communication(self, comm_data):
        self._connect();
        if not self.conn: return False
        sql = "INSERT INTO communications (applicant_id, gmail_message_id, sender, subject, body, direction) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (gmail_message_id) DO NOTHING;"
        try:
            with self.conn.cursor() as cur: cur.execute(sql, (comm_data.get("applicant_id"), comm_data.get("gmail_message_id"), comm_data.get("sender"), comm_data.get("subject"), comm_data.get("body"), comm_data.get("direction"))); self.conn.commit(); return True
        except Exception as e: logger.error(f"Error inserting communication: {e}"); self.conn.rollback(); return False
    def get_conversations(self, applicant_id):
        self._connect();
        if not self.conn: return pd.DataFrame()
        query = "SELECT gmail_message_id, sender, subject, body, direction, sent_at FROM communications WHERE applicant_id = %s ORDER BY sent_at ASC;"
        try: return pd.read_sql_query(query, self.conn, params=(applicant_id,))
        except Exception as e: logger.error(f"Error fetching conversations: {e}"); return pd.DataFrame()
    def fetch_applicants_as_df(self):
        self._connect();
        if not self.conn: return pd.DataFrame()
        query = "SELECT id, name, email, phone, domain, job_history, education, cv_url, status, created_at, gmail_thread_id FROM applicants ORDER BY created_at DESC;"
        try: df = pd.read_sql_query(query, self.conn); df['job_history'] = df['job_history'].fillna(''); return df
        except Exception as e: logger.error(f"Error fetching applicants: {e}"); return pd.DataFrame()
    def get_active_threads(self):
        self._connect();
        if not self.conn: return []
        query = "SELECT id, gmail_thread_id FROM applicants WHERE status NOT IN ('Rejected', 'Hired');"
        try:
            with self.conn.cursor() as cur: cur.execute(query); return cur.fetchall() 
        except Exception as e: logger.error(f"Error fetching active threads: {e}"); return []
    def insert_export_log(self, file_name, sheet_url, user="HR"):
        self._connect();
        if not self.conn: return False
        sql = "INSERT INTO export_logs (file_name, sheet_url, created_by) VALUES (%s, %s, %s);"
        try:
            with self.conn.cursor() as cur: cur.execute(sql, (file_name, sheet_url, user)); self.conn.commit(); logger.info(f"New export log created for: {file_name}"); return True
        except Exception as e: logger.error(f"Error inserting export log: {e}"); self.conn.rollback(); return False
    def delete_export_log(self, log_id):
        self._connect();
        if not self.conn: return False
        sql = "DELETE FROM export_logs WHERE id = %s;"
        try:
            with self.conn.cursor() as cur: cur.execute(sql, (log_id,)); self.conn.commit(); logger.info(f"Deleted export log with ID: {log_id}"); return True
        except Exception as e: logger.error(f"Error deleting export log {log_id}: {e}"); self.conn.rollback(); return False
    def fetch_export_logs(self):
        self._connect();
        if not self.conn: return pd.DataFrame()
        query = "SELECT id, file_name, sheet_url, created_at FROM export_logs ORDER BY created_at DESC LIMIT 5;"
        try: return pd.read_sql_query(query, self.conn)
        except Exception as e: logger.error(f"Error fetching export logs: {e}"); return pd.DataFrame()
    def insert_bulk_applicants(self, applicants_df):
        self._connect();
        if not self.conn: return 0, 0
        inserted_count, skipped_count = 0, 0
        applicants_df.columns = [col.replace('_', ' ').title().replace(' ', '') for col in applicants_df.columns]
        required_cols = ['Name', 'Email']
        if not all(col in applicants_df.columns for col in required_cols):
            logger.error(f"Import failed: DataFrame is missing required columns 'Name' or 'Email'. Found: {list(applicants_df.columns)}"); return "Import failed: The sheet must contain 'Name' and 'Email' columns.", 0
        check_sql = "SELECT id FROM applicants WHERE email = %s;"; insert_sql = "INSERT INTO applicants (name, email, phone, domain, education, job_history, cv_url, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
        try:
            with self.conn.cursor() as cur:
                for _, row in applicants_df.iterrows():
                    email = row.get('Email')
                    if not email: skipped_count += 1; continue
                    cur.execute(check_sql, (email,))
                    if cur.fetchone(): skipped_count += 1; continue
                    cur.execute(insert_sql, (row.get('Name'), email, row.get('Phone'), row.get('Domain', 'Other'), row.get('Education'), row.get('JobHistory'), row.get('CvUrl'), row.get('Status', 'New'))); inserted_count += 1
                self.conn.commit(); logger.info(f"Bulk insert complete. Inserted: {inserted_count}, Skipped: {skipped_count}")
        except Exception as e: logger.error(f"Error during bulk insert: {e}", exc_info=True); self.conn.rollback(); return str(e), 0
        return inserted_count, skipped_count  