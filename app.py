import streamlit as st
import pandas as pd
import datetime
from modules.database_handler import DatabaseHandler
from modules.email_handler import EmailHandler
from modules.calendar_handler import CalendarHandler
from streamlit_quill import st_quill
from modules.sheet_updater import SheetsUpdater
import re

# --- Page Configuration & Resource Caching ---
st.set_page_config(page_title="HR Applicant Dashboard", page_icon="📑", layout="wide")

@st.cache_resource
def get_db_handler(): return DatabaseHandler()
@st.cache_resource
def get_email_handler(): return EmailHandler()
@st.cache_resource
def get_sheets_updater(): return SheetsUpdater()
@st.cache_resource
def get_calendar_handler(): return CalendarHandler()

db_handler = get_db_handler()
email_handler = get_email_handler()
sheets_updater = get_sheets_updater()
calendar_handler = get_calendar_handler()

# --- Cached Data Fetching Functions ---
@st.cache_data(ttl=600) 
def load_all_applicants():
    df = db_handler.fetch_applicants_as_df()
    rename_map = {
        'id': 'Id', 'name': 'Name', 'email': 'Email', 'phone': 'Phone', 'domain': 'Domain',
        'education': 'Education', 'job_history': 'JobHistory', 'cv_url': 'CvUrl', 'status': 'Status',
        'created_at': 'CreatedAt', 'gmail_thread_id': 'GmailThreadId'
    }
    return df.rename(columns=rename_map)

@st.cache_data(ttl=3600)
def load_statuses(): return db_handler.get_statuses()
@st.cache_data(ttl=3600)
def load_interviewers(): return db_handler.get_interviewers()
@st.cache_data(ttl=600)
def load_interviews(applicant_id): return db_handler.get_interviews_for_applicant(applicant_id)

# --- Initialize Session State ---
if 'selected_applicants' not in st.session_state: st.session_state.selected_applicants = set()
if 'confirm_delete' not in st.session_state: st.session_state.confirm_delete = False

# Load initial data
df = load_all_applicants()
status_list = load_statuses()
interviewer_list = load_interviewers()

# --- App Header & Sidebar ---
st.title("HR Applicant Dashboard")
st.markdown("Manage applicant lifecycles, from screening to hiring.")

st.sidebar.header("Filter & Search")
search_query = st.sidebar.text_input("Search by Name or Email", placeholder="e.g. Paras Kaushik")
df_filtered = df.copy()
if not df.empty:
    status_options = ['All'] + sorted(df['Status'].unique().tolist())
    status_filter = st.sidebar.selectbox("Filter by Status:", options=status_options)
    if status_filter != 'All':
        df_filtered = df_filtered[df_filtered['Status'] == status_filter]
    domain_options = ['All'] + sorted(df['Domain'].unique().tolist())
    domain_filter = st.sidebar.selectbox("Filter by Domain:", options=domain_options)
    if domain_filter != 'All':
        df_filtered = df_filtered[df_filtered['Domain'] == domain_filter]
    if search_query:
        df_filtered = df_filtered[
            df_filtered['Name'].str.contains(search_query, case=False, na=False) |
            df_filtered['Email'].str.contains(search_query, case=False, na=False)
        ]
st.sidebar.divider()
if st.sidebar.button("Refresh Data", use_container_width=True): st.cache_data.clear(); st.rerun()
st.sidebar.divider()
st.sidebar.header("🔥 Actions on Selected")
num_selected = len(st.session_state.selected_applicants)
if num_selected > 0:
    st.sidebar.markdown(f"**{num_selected} applicant(s) selected**")
    if st.sidebar.button(f"Export {num_selected} Selected to Sheet", use_container_width=True):
        with st.spinner("Generating your Google Sheet..."):
            selected_ids = list(st.session_state.selected_applicants)
            export_df = df[df['Id'].isin(selected_ids)]
            columns_to_export = ['Name', 'Email', 'Phone', 'Education', 'JobHistory', 'CvUrl', 'Domain', 'Status']
            data_to_export = export_df[columns_to_export].to_dict('records')
            export_result = sheets_updater.create_export_sheet(data_to_export, [c.replace('JobHistory', 'Job History').replace('CvUrl', 'CV URL') for c in columns_to_export])
            if export_result and export_result.get('url'): db_handler.insert_export_log(export_result['title'], export_result['url']); st.sidebar.success("Export successful!"); st.session_state.selected_applicants.clear(); st.rerun()
            else: st.sidebar.error("Export failed. Check logs.")
    if st.sidebar.button(f"Delete {num_selected} Selected Applicant(s)", type="primary", use_container_width=True): st.session_state.confirm_delete = True
    if st.session_state.confirm_delete:
        st.sidebar.warning("This is permanent. Are you sure?")
        c1, c2 = st.sidebar.columns(2)
        if c1.button("✅ Yes, I'm sure", use_container_width=True, type="primary"):
            ids_to_delete = list(st.session_state.selected_applicants)
            if db_handler.delete_applicants(ids_to_delete): st.success(f"Successfully deleted {len(ids_to_delete)} applicants."); st.session_state.selected_applicants.clear(); st.session_state.confirm_delete = False; st.cache_data.clear(); st.rerun()
            else: st.error("An error occurred during deletion.")
        if c2.button("❌ Cancel", use_container_width=True): st.session_state.confirm_delete = False; st.rerun()
else:
    st.sidebar.info("Select applicants using the checkboxes to perform actions.")
st.sidebar.divider()
st.sidebar.header("History & Imports")
with st.sidebar.expander("View Recent Exports"):
    export_logs = db_handler.fetch_export_logs()
    def delete_log_and_rerun(log_id):
        if db_handler.delete_export_log(log_id): st.rerun()
        else: st.error("Failed to delete the export log.")
    if not export_logs.empty:
        for _, log in export_logs.iterrows():
            col1, col2 = st.columns([4, 1])
            with col1: st.markdown(f"[{log['file_name']}]({log['sheet_url']})", help=f"Created on {pd.to_datetime(log['created_at']).strftime('%b %d, %H:%M')}")
            with col2: st.button("🗑️", key=f"del_log_{log['id']}", on_click=delete_log_and_rerun, args=(log['id'],), help="Delete this export log")
    else: st.info("No recent exports found.")
with st.sidebar.expander("Import Applicants from Sheet"):
    sheet_url = st.text_input("Paste Google Sheet URL here", placeholder="https://docs.google.com/spreadsheets/d/...")
    def extract_spreadsheet_id(url):
        match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', url)
        return match.group(1) if match else None
    if st.button("Import from Sheet"):
        if sheet_url:
            spreadsheet_id = extract_spreadsheet_id(sheet_url)
            if spreadsheet_id:
                with st.spinner("Reading data..."): data = sheets_updater.read_sheet_data(spreadsheet_id)
                if isinstance(data, str): st.error(data)
                elif data.empty: st.warning("No data found.")
                else:
                    with st.spinner(f"Importing {len(data)} records..."):
                        inserted, skipped = db_handler.insert_bulk_applicants(data)
                    if isinstance(inserted, str): st.error(inserted)
                    else:
                        st.success(f"Imported {inserted} new applicants.")
                        if skipped > 0: st.info(f"Skipped {skipped} (already exist).")
                        st.cache_data.clear(); st.rerun()
            else: st.error("Invalid Google Sheet URL.")
        else: st.warning("Please paste a URL.")

# --- Main Dashboard Display ---
if df_filtered.empty:
    st.warning("No applicants found or none match the current filters.")
else:
    tab1, tab2 = st.tabs(["Applicant Dashboard", "⚙️ System Settings"])

    with tab1: # Main dashboard for managing applicants
        st.metric("Displaying Applicants", f"{len(df_filtered)} / {len(df)}")
        
        filtered_ids = set(df_filtered['Id'])
        def toggle_applicant_selection(applicant_id):
            if applicant_id in st.session_state.selected_applicants: st.session_state.selected_applicants.remove(applicant_id)
            else: st.session_state.selected_applicants.add(applicant_id)
            
        def handle_select_all():
            if st.session_state.get('select_all_visible_checkbox', False): st.session_state.selected_applicants.update(filtered_ids)
            else: st.session_state.selected_applicants.difference_update(filtered_ids)
            
        is_all_selected = filtered_ids.issubset(st.session_state.selected_applicants) and bool(filtered_ids)
        
        st.checkbox("Select/Deselect All Visible Applicants", value=is_all_selected, key='select_all_visible_checkbox', on_change=handle_select_all)
        st.divider()

        for _, row in df_filtered.iterrows():
            applicant_id = row['Id']
            expander_title = f"**{row['Name']}** ({row['Domain']}) - Status: **{row['Status']}**"
            with st.expander(expander_title):
                st.checkbox(f"Select {row['Name']}", value=(applicant_id in st.session_state.selected_applicants), key=f"select_{applicant_id}", on_change=toggle_applicant_selection, args=(applicant_id,))
                st.divider()
                
                col1, col2 = st.columns([2, 1])
                with col1: # APPLICANT DETAILS
                    st.subheader("Applicant Details")
                    st.markdown(f"**Email:** `{row['Email']}`\n\n**Phone:** `{row['Phone'] or 'N/A'}`")
                    st.link_button("📄 View Resume on Drive", url=row['CvUrl'] or "#", use_container_width=True, disabled=not row['CvUrl'])
                    st.markdown("**Education**"); st.write(row['Education'] or "No details extracted.")
                    st.markdown("**Job History**"); st.markdown(row['JobHistory'] or "No details extracted.")
                
                with col2: # ACTIONS
                    st.subheader("Actions")
                    current_status_index = status_list.index(row['Status']) if row['Status'] in status_list else 0
                    new_status = st.selectbox("Change Status", options=status_list, index=current_status_index, key=f"status_{applicant_id}")
                    if st.button("Save Status", key=f"save_{applicant_id}"):
                        if db_handler.update_applicant_status(applicant_id, new_status):
                            st.success(f"Status updated to '{new_status}'!"); st.cache_data.clear(); st.rerun()
                        else: st.error("Failed to update status.")
                    st.divider()
                    st.subheader("Interview")
                    
                    interviews = load_interviews(applicant_id)
                    if not interviews.empty:
                        st.write("**Scheduled Interviews:**")
                        for _, interview in interviews.iterrows():
                            st.success(f"✅ {interview['event_title']} with {interview['interviewer_name']} on {interview['start_time'].strftime('%b %d, %Y at %I:%M %p')}")

                    schedule_key = f"show_schedule_form_{applicant_id}"
                    if st.button("🗓️ Schedule New Interview", key=f"schedule_btn_{applicant_id}"):
                        st.session_state[schedule_key] = not st.session_state.get(schedule_key, False)

                    if st.session_state.get(schedule_key, False):
                        with st.form(f"schedule_form_{applicant_id}"):
                            interviewer_options = {f"{name} ({email})": email for name, email in zip(interviewer_list['name'], interviewer_list['email'])}
                            interviewer_display = st.selectbox("Select Interviewer", options=list(interviewer_options.keys()), key=f"sel_int_{applicant_id}")
                            duration = st.selectbox("Interview Duration (minutes)", options=[30, 45, 60], key=f"sel_dur_{applicant_id}")
                            if st.form_submit_button("1. Find Available Times"):
                                st.session_state[f'schedule_interviewer_{applicant_id}'] = interviewer_options[interviewer_display]
                                st.session_state[f'schedule_duration_{applicant_id}'] = duration
                                with st.spinner(f"Finding open slots..."):
                                    st.session_state[f'available_slots_{applicant_id}'] = calendar_handler.find_available_slots(interviewer_email=interviewer_options[interviewer_display], duration_minutes=duration)
                                    if not st.session_state.get(f'available_slots_{applicant_id}', []):
                                        st.warning("No available slots found.")
                        
                        slots_key = f"available_slots_{applicant_id}"
                        if st.session_state.get(slots_key):
                            available_slots = st.session_state[slots_key]
                            formatted_slots = {s.strftime('%A, %b %d at %I:%M %p'): s for s in available_slots}
                            selected_slots_display = st.multiselect("2. Select times to propose:", options=list(formatted_slots.keys()), key=f"multi_{applicant_id}")
                            if selected_slots_display and st.button("Prepare Email", key=f"prep_email_{applicant_id}"):
                                email_body = [f"Dear {row['Name']},<br><br>Following up, please let us know which time works for your interview:<ul>"] + [f"<li>{s}</li>" for s in selected_slots_display] + ["</ul>We look forward to hearing from you.<br><br>Best regards,<br>HR Department"]
                                st.session_state[f'email_body_{applicant_id}'] = "".join(email_body)
                                st.session_state[f"show_hub_{applicant_id}"] = True
                                st.rerun()

                        if st.session_state.get(slots_key):
                            with st.form(f"booking_form_{applicant_id}"):
                                st.write("**3. Confirm Final Time & Book**")
                                final_slot_options = {s.strftime('%A, %b %d at %I:%M %p'): s for s in st.session_state.get(slots_key, [])}
                                final_slot_display = st.selectbox("Confirmed time by applicant:", options=list(final_slot_options.keys()), key=f"final_slot_{applicant_id}")
                                description = st.text_area("Event Description / Notes:", key=f"desc_{applicant_id}", placeholder="e.g., First round technical interview for the Software Developer role.")
                                
                                if st.form_submit_button("✅ Confirm & Book in Google Calendar"):
                                    if not final_slot_display: st.error("Please select the final confirmed time slot.")
                                    else:
                                        with st.spinner("Booking interview in Google Calendar..."):
                                            interviewer_email = st.session_state[f'schedule_interviewer_{applicant_id}']
                                            duration = st.session_state[f'schedule_duration_{applicant_id}']
                                            start_time = final_slot_options[final_slot_display]
                                            end_time = start_time + datetime.timedelta(minutes=duration)
                                            interviewer_details = interviewer_list[interviewer_list['email'] == interviewer_email].iloc[0]

                                            created_event = calendar_handler.create_calendar_event(
                                                applicant_name=row['Name'], applicant_email=row['Email'], interviewer_email=interviewer_email,
                                                start_time=start_time, end_time=end_time, description=description
                                            )
                                            if created_event:
                                                db_handler.log_interview(
                                                    applicant_id=applicant_id, interviewer_id=interviewer_details['id'],
                                                    title=created_event['summary'], start_time=start_time, end_time=end_time,
                                                    event_id=created_event['id']
                                                )
                                                st.success(f"Interview booked! Event created in Google Calendar.")
                                                
                                                keys_to_delete = [
                                                    f'schedule_interviewer_{applicant_id}',
                                                    f'schedule_duration_{applicant_id}',
                                                    f'available_slots_{applicant_id}',
                                                    f'show_schedule_form_{applicant_id}',
                                                    f'email_body_{applicant_id}'
                                                ]
                                                for k in keys_to_delete:
                                                    if k in st.session_state:
                                                        del st.session_state[k]
                                                st.rerun()
                                            else: st.error("Failed to create Google Calendar event.")
                
                st.divider()
                hub_key = f"show_hub_{applicant_id}"
                if st.button("View/Hide Communication Hub", key=f"hub_toggle_{applicant_id}"):
                    st.session_state[hub_key] = not st.session_state.get(hub_key, False)
                if st.session_state.get(hub_key, False):
                    st.subheader("Communication Hub")
                    with st.container(height=400):
                        conversations = db_handler.get_conversations(applicant_id)
                        if not conversations.empty:
                            for _, comm in conversations.iterrows():
                                role = "user" if comm['direction'] == 'Incoming' else "assistant"
                                with st.chat_message(role): st.markdown(f"**From:** {comm['sender']}<br>**Subject:** {comm.get('subject', 'N/A')}<hr>{comm['body']}", unsafe_allow_html=True)
                        else: st.info("No communication history.")
                    with st.form(key=f"email_form_{applicant_id}"):
                        subject = st.text_input("Subject", value=f"Re: Your application for {row['Domain']}")

                        email_content = st.session_state.get(f'email_body_{applicant_id}', f"Dear {row['Name']},\n\n")
                        content = st_quill(value=email_content, html=True, key=f"quill_{applicant_id}")
                        attachment = st.file_uploader("Attach a file")
                        if st.form_submit_button("Send Email"):
                            if not content or len(content) < 15: st.error("Email body is too short.")
                            else:
                                with st.spinner("Sending email..."):
                                    sent_message = email_handler.send_email(to=row['Email'], subject=subject, body=content, thread_id=row['GmailThreadId'], attachment=attachment)
                                    if sent_message:
                                        comm_data = {"applicant_id": applicant_id, "gmail_message_id": sent_message['id'], "sender": "HR Department", "subject": subject, "body": content, "direction": "Outgoing"}
                                        db_handler.insert_communication(comm_data)
                                        if f'email_body_{applicant_id}' in st.session_state:
                                            del st.session_state[f'email_body_{applicant_id}']
                                        st.success("Email sent and logged!"); st.rerun()
                                    else: st.error("Failed to send email.")

    with tab2: # Settings tab for managing statuses and interviewers
        st.header("Manage System Settings")
        st.markdown("Here you can add or remove applicant statuses and interviewers available throughout the application.")
        st.divider()

        col_status, col_interviewer = st.columns(2, gap="large")

        with col_status:
            st.subheader("Applicant Statuses")
            for status in status_list:
                c1, c2 = st.columns([3, 1])
                c1.write(status)
                if status not in ["New", "Hired", "Rejected"]:
                    if c2.button("🗑️", key=f"del_status_{status}", help=f"Delete '{status}' status", use_container_width=True):
                        error_msg = db_handler.delete_status(status)
                        if error_msg: st.error(error_msg)
                        else: st.success(f"Status '{status}' deleted."); st.cache_data.clear(); st.rerun()
            
            with st.form("new_status_form", clear_on_submit=True):
                new_status_name = st.text_input("Add a new status")
                if st.form_submit_button("Add Status", use_container_width=True):
                    if new_status_name:
                        if db_handler.add_status(new_status_name): st.success(f"Status '{new_status_name}' added."); st.cache_data.clear(); st.rerun()
                        else: st.warning(f"Status '{new_status_name}' already exists.")

        with col_interviewer:
            st.subheader("Interviewers")
            if not interviewer_list.empty:
                for _, interviewer in interviewer_list.iterrows():
                    c1, c2 = st.columns([4, 1])
                    c1.text(f"{interviewer['name']} ({interviewer['email']})")
                    if c2.button("🗑️", key=f"del_interviewer_{interviewer['id']}", help=f"Delete {interviewer['name']}", use_container_width=True):
                        if db_handler.delete_interviewer(interviewer['id']): st.success(f"Interviewer '{interviewer['name']}' deleted."); st.cache_data.clear(); st.rerun()
                        else: st.error("Could not delete interviewer.")
            
            with st.form("new_interviewer_form", clear_on_submit=True):
                st.write("Add New Interviewer:")
                new_interviewer_name = st.text_input("Name", key="new_interviewer_name_input")
                new_interviewer_email = st.text_input("Google Account Email", key="new_interviewer_email_input")
                if st.form_submit_button("Add Interviewer", use_container_width=True):
                    if new_interviewer_name and new_interviewer_email:
                        if db_handler.add_interviewer(new_interviewer_name, new_interviewer_email): st.success(f"Interviewer '{new_interviewer_name}' added."); st.cache_data.clear(); st.rerun()
                        else: st.warning("Interviewer with that email already exists.")
                    else: st.warning("Please provide both name and email.")