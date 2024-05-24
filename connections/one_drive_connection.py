from dotenv import load_dotenv
import os

class ConnectionOneDrive:
    def __init__(self):
        load_dotenv()
        self.tenant_id = os.getenv("ONE_DRIVE_TENAN_ID")
        self.user_principal_name = os.getenv("ONE_DRIVE_USER_PRINCIPAL_NAME")
        self.client_id = os.getenv("ONE_DRIVE_CLIENTE_ID")
        self.client_secret = os.getenv("ONE_DRIVE_CLIENT_SECRET")
        self.authority_url = os.getenv("ONE_DRIVE_AUTHORITY_URL")
        self.authority_url=self.authority_url+self.tenant_id
        self.scope = os.getenv("ONE_DRIVE_SCOPE")
