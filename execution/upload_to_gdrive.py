import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from dotenv import load_dotenv

load_dotenv()

SCOPES = ['https://www.googleapis.com/auth/drive.file']

def get_gdrive_service():
    creds = None
    # token.json stores the user's access and refresh tokens
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    elif os.getenv("GOOGLE_DRIVE_TOKEN_JSON"):
        # For Railway: load from env var to avoid re-auth in headless
        token_data = json.loads(os.getenv("GOOGLE_DRIVE_TOKEN_JSON"))
        creds = Credentials.from_authorized_user_info(token_data, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
            
    return build('drive', 'v3', credentials=creds)

def upload_pdf_to_drive(pdf_path, folder_id):
    service = get_gdrive_service()
    
    file_metadata = {
        'name': os.path.basename(pdf_path),
        'parents': [folder_id]
    }
    media = MediaFileUpload(pdf_path, mimetype='application/pdf', resumable=True)
    
    file = service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink').execute()
    link = file.get('webViewLink')
    print(link)
    return link

if __name__ == "__main__":
    folder_id = os.getenv("GDRIVE_FOLDER_ID")
    if not folder_id:
        print("Error: GDRIVE_FOLDER_ID not found in .env")
        exit(1)
        
    pdf_path = ".tmp/report.pdf"
    if not os.path.exists(pdf_path):
        print(f"Error: {pdf_path} does not exist.")
        exit(1)
        
    upload_pdf_to_drive(pdf_path, folder_id)
