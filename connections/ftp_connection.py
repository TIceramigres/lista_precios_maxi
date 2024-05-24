import ftplib
from dotenv import load_dotenv
import os

class ConnectionFTP:
    def __init__(self, db):
        load_dotenv()  # Cargar las variables de entorno desde el archivo .env
        self.hostname = os.getenv(f"FTP_{db}_HOST")
        self.username = os.getenv(f"FTP_{db}_USER")
        self.password = os.getenv(f"FTP_{db}_PASSWORD")
        self.ftp = None
        
    def connect(self):
        self.ftp = ftplib.FTP(self.hostname)
        self.ftp.login(self.username, self.password)

    def list_files(self):
        return self.ftp.dir()

    def download_file(self, remote_path, local_path):
        with open(local_path, 'wb') as f:
            self.ftp.retrbinary('RETR ' + remote_path, f.write)

    def upload_file(self, local_path, remote_path):
        with open(local_path, 'rb') as f:
            self.ftp.storbinary('STOR ' + remote_path, f)

    def delete_local_file(self, local_path):
        self.ftp.delete(local_path)

    def close(self):
        self.ftp.quit()