from dotenv import load_dotenv
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders


class mail:
    def __init__(self):
        load_dotenv()
        self.host = os.getenv("MAIL_HOST")
        self.port = os.getenv("MAIL_PORT")
        self.username = os.getenv("MAIL_USERNAME")
        self.password = os.getenv("MAIL_PASSWORD")

    def enviar_correo(self, destinatario, asunto, contenido, archivo_adjunto):
        mensaje = MIMEMultipart()
        mensaje['From'] = self.username
        mensaje['To'] = destinatario
        mensaje['Subject'] = asunto
        mensaje.attach(MIMEText(contenido, 'html'))

        if archivo_adjunto:
            adjunto = open(archivo_adjunto, 'rb')
            parte_adjunta = MIMEBase('application', 'octet-stream')
            parte_adjunta.set_payload(adjunto.read())
            encoders.encode_base64(parte_adjunta)
            parte_adjunta.add_header('Content-Disposition', f'attachment; filename={os.path.basename(archivo_adjunto)}')
            mensaje.attach(parte_adjunta)
            adjunto.close()

        with smtplib.SMTP(self.host, self.port) as server:
            server.starttls()
            server.login(self.username, self.password)
            server.send_message(mensaje)  
