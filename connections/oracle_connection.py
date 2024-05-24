import cx_Oracle
from dotenv import load_dotenv
import os

class ConnectionOracle:

    def __init__(self, db):
        load_dotenv()
        self.host = os.getenv(f"ORACLE_{db}_HOST")
        self.port = os.getenv(f"ORACLE_{db}_PORT")
        self.service = os.getenv(f"ORACLE_{db}_SID")
        self.user = os.getenv(f"ORACLE_{db}_USER")
        self.password = os.getenv(f"ORACLE_{db}_PASSWORD")
        self.connection = None
        self.cursor = None
    
    def connect(self):
        try:
            dsn_tns = cx_Oracle.makedsn(self.host, self.port, service_name=self.service)
            self.connection = cx_Oracle.connect(user=self.user, password=self.password, dsn=dsn_tns)
            print("Conexión a Oracle establecida correctamente")
        except Exception as e:
            print("Se presento un error conectando a la base de datos ORACLE Error:", e)
    
    def disconnect(self):
        try:
            if self.connection:
                self.connection.close()
        except Exception as e:
            print("Se presento un error cerrando la conexion ORACLE Error:", e)
    
    def execute_select(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        # print(columns)
        # Crear una lista de objetos a partir de las filas del cursor
        objects = []
        for row in results:
        # Crear un diccionario a partir de la tupla y las columnas
            object_dict = dict(zip(columns, row))
        # Crear un objeto a partir del diccionario
            item = type('Object', (object,), object_dict)()
        # Añadir el objeto a la lista de objetos
            objects.append(item)
        return results, columns