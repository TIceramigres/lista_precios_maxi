import os
import requests
import msal
from connections.one_drive_connection import ConnectionOneDrive

class EnviarListaPrecioOneDrive:
    def __init__(self, nombreArchivo):
        self.onedrive_folder = 'Lista de precios'
        self.archivo = nombreArchivo
        self.conexionOneDrive = ConnectionOneDrive()
        self.app = msal.ConfidentialClientApplication(self.conexionOneDrive.client_id, authority=self.conexionOneDrive.authority_url, client_credential=self.conexionOneDrive.client_secret)
        self.enviado()

    def enviado(self):
        token_response = self.app.acquire_token_for_client(scopes=[self.conexionOneDrive.scope])

        if 'access_token' in token_response:
            access_token = token_response['access_token']
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }

            # Obtener el drive_id del usuario usando userPrincipalName
            drive_url = f'https://graph.microsoft.com/v1.0/users/{self.conexionOneDrive.user_principal_name}/drive'
            drive_response = requests.get(drive_url, headers=headers)
            
            if drive_response.status_code == 200:
                drive_id = drive_response.json()['id']
                
                # Verificar la existencia de la carpeta /Documents/
                check_folder_url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{self.onedrive_folder}'
                check_folder_response = requests.get(check_folder_url, headers=headers)
                
                if check_folder_response.status_code == 200:
                    # Listar todos los archivos en la carpeta /Documents/
                    list_files_url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{self.onedrive_folder}:/children'
                    files_response = requests.get(list_files_url, headers=headers)
                    
                    if files_response.status_code == 200:
                        files = files_response.json()['value']
                        
                        # Eliminar cada archivo listado
                        for file in files:
                            file_id = file['id']
                            delete_url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{file_id}'
                            delete_response = requests.delete(delete_url, headers=headers)
                            
                            if delete_response.status_code == 204:
                                print(f'Archivo {file["name"]} eliminado exitosamente')
                            else:
                                print(f'Error al eliminar el archivo {file["name"]}: {delete_response.status_code}, {delete_response.text}')
                        
                        # Leer el archivo a subir
                        with open(self.archivo, 'rb') as file:
                            file_data = file.read()

                        # Subir el archivo a OneDrive
                        upload_url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{self.onedrive_folder}/{os.path.basename(self.archivo)}:/content'
                        response = requests.put(upload_url, headers=headers, data=file_data)

                        if response.status_code in [200, 201]:
                            print('Archivo subido exitosamente a OneDrive')
                        else:
                            print(f'Error al subir el archivo: {response.status_code}, {response.text}')
                    else:
                        print(f'Error al listar los archivos: {files_response.status_code}, {files_response.text}')
                else:
                    print(f'Error al verificar la carpeta {self.onedrive_folder}: {check_folder_response.status_code}, {check_folder_response.text}')
            else:
                print(f'Error al obtener el drive_id: {drive_response.status_code}, {drive_response.text}')
        else:
            print('Error al obtener el token de acceso')
        self.removeArchivo()

    def removeArchivo(self):
        if os.path.exists(self.archivo):
            os.remove(self.archivo)


