import pyodbc
from dotenv import load_dotenv
import os
import pandas as pd

class ConnectionSqlServer:

    def __init__(self, db):
        load_dotenv()
        self.driver = os.getenv(f"MSSQL_{db}_DRIVER")
        self.server = os.getenv(f"MSSQL_{db}_SERVER")
        self.database = os.getenv(f"MSSQL_{db}_DATABASE")
        self.username = os.getenv(f"MSSQL_{db}_USER")
        self.password = os.getenv(f"MSSQL_{db}_PASSWORD")
        self.encrypt = os.getenv(f"MSSQL_{db}_ENCRYPT")
    
    def connect(self):
        try:
            connection_string = f"DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password};Encrypt={self.encrypt}"
            self.connection = pyodbc.connect(connection_string)
        except Exception as e:
            print("Se presento un error conectando a la base de datos MSSQL Error:", e)
    
    def disconnect(self):
        try:
            if self.connection:
                self.connection.close()
        except Exception as e:
            print("Se presento un error cerrando la conexion MSSQL Error:", e)
    
    def execute_select(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        # print(columns)
        # print(results)
        # Crear una lista de objetos a partir de las filas del cursor
        objects = []
        for row in results:
        # Crear un diccionario a partir de la tupla y las columnas
            object_dict = dict(zip(columns, row))
        # Crear un objeto a partir del diccionario
            item = type('Object', (object,), object_dict)()
        # Añadir el objeto a la lista de objetos
            objects.append(item)
        return objects
    
    
    def execute_query(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        lista_de_tuplas = list(map(tuple, results))
        
        objects = []
        for row in results:
        # Crear un diccionario a partir de la tupla y las columnas
            object_dict = dict(zip(columns, row))
        # Crear un objeto a partir del diccionario
            item = type('Object', (object,), object_dict)()
        # Añadir el objeto a la lista de objetos
            objects.append(item)
        
        
        # print(lista_de_tuplas)
        # print(columns)
        # print(results)
        # tupla_limpia = tuple(map(lambda x: x.strip(), results))
        # print(tupla_limpia)
        # transposed_list = [list(x) for x in zip(*results)]
        # datos_transpuestos = list(map(list, zip(*results)))
        df = pd.DataFrame(objects, columns=columns) 
        return df