import os
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ['https://www.googleapis.com/auth/drive.file']

def authenticate():
    if not os.path.exists('token.json'):
         print("Opening browser for Google Drive authentication...")
         flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
         creds = flow.run_local_server(port=0)
         with open('token.json', 'w') as token:
             token.write(creds.to_json())
         print("Authentication successful! token.json saved.")
    else:
         print("Already authenticated.")

if __name__ == "__main__":
    authenticate()
