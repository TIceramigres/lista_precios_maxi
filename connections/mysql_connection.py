import mysql.connector
from dotenv import load_dotenv
import os

class ConnectionMySql:

    def __init__(self, db):
        load_dotenv()  # Cargar las variables de entorno desde el archivo .env
        self.host = os.getenv(f"MYSQL_{db}_HOST")
        self.port = os.getenv(f"MYSQL_{db}_PORT")
        self.username = os.getenv(f"MYSQL_{db}_USER")
        self.password = os.getenv(f"MYSQL_{db}_PASSWORD")
        self.database = os.getenv(f"MYSQL_{db}_DATABASE")
        self.connection = None
        self.cursor = None

    def connect(self):
        self.connection = mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            database=self.database
        )
        self.cursor = self.connection.cursor()

    def disconnect(self):
        if self.connection:
            self.connection.close()
        if self.cursor:
            self.cursor.close()

    def execute_select(self, query):
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        columns = [column[0] for column in self.cursor.description]
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

    def execute_insert(self, query, data):
        try:
            self.cursor.execute(query, data)
            self.connection.commit()
            # print(self.cursor.statement)
            return self.cursor.rowcount
        except mysql.connector.Error as error:
            print("Error al insertar dato: {}".format(error))

    def execute_delete(self, query):
        try:
            self.cursor.execute(query)
            self.connection.commit()
            # print(self.cursor.statement)
            return self.cursor.rowcount
        except mysql.connector.Error as error:
            print("Error al destruir el dato: {}".format(error))