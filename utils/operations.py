import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from connections.mail import mail
from connections.alchemy_connection import AlchemyConnection
import re

class Operations:

    def __init__(self):
        self.destinatarios = '''analistasw1@maxicassa.com, analistasw2@Maxicassa.com, analistabi@maxicassa.com, especialistasw@maxicassa.com, 
                                auxiliartic3@maxicassa.com, analistasw4@Maxicassa.com, soporteit@maxicassa.com, jtecnologia@maxicassa.com, coordtecnologia@maxicassa.com'''
        
    def get_data(self, engine, query):
        try:
            with engine.connect() as connection:
                results = connection.execute(text(query))
                df = pd.DataFrame(results)
                df_cleaned = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                print(f'Se han retornado {df_cleaned.shape[0]} registros')
                self.load_log(engine, None, query, 'Obtener_data', df_cleaned.shape[0], False)
                return df_cleaned
        except Exception as e:
            print("Se ha presentado un error obteniendo los datos en MSSQL Error: ", e)
            self.load_log(engine, None, query, 'Obtener_data', 0, True, f'Se ha presentado un error obteniendo los datos: ')
            adjunto = None
            correo = mail()
            correo.enviar_correo(self.destinatarios, 'Error en la ETL', f'Hola, Se ha presentado un error obteniendo los datos del siguiente proceso almacenado: {query} en MSSQL Error: {e}', adjunto)

    def delete_data(self, engine, query):
        tabla = re.search(r"FROM\s+(\w+)", query, re.IGNORECASE)
        tabla = tabla.group(1) 
        try:
            Session = sessionmaker(bind=engine)
            session = Session()
            result = session.execute(text(query))
            session.commit()
            print(f'Se ha eliminado {result.rowcount} registros')
            self.load_log(engine, tabla, None, 'Eliminar_data', result.rowcount, False)
        except Exception as e:
            print("Se ha presentado un error eliminando los datos en MYSQL Error: ", e)
            self.load_log(engine, tabla, None, 'eliminar_data', 0, True,  f'Hola, Se ha presentado un error eliminando los datos en la siguiente query:{query}')
            adjunto = None
            correo = mail()
            correo.enviar_correo(self.destinatarios, 'Error en la ETL', f'Hola, Se ha presentado un error eliminando los datos en la siguiente query:{query} en MYSQL Error: {e}', adjunto)
        finally:
            session.close()

    def load_data(self, df, engine, table, operation = 'append'):
        try:
            df.to_sql(table, engine, if_exists=operation, index=False)
            print(f'Se han insertado {df.shape[0]} registros')
            self.load_log(engine, table, None, 'Cargar_data', df.shape[0], False)
        except Exception as e:
            print(f"Se ha presentado un error insertado los datos la tabla {table} en MYSQL Error: ", e)
            self.load_log(engine, table, None, 'Cargar_data', 0, True, f'fallo en la inserción de los datos en la tabla {table}')
            adjunto = None
            correo = mail()
            correo.enviar_correo(self.destinatarios, 'Error en la ETL', f'Hola, Se ha presentado un error insertado los datos en la tabla {table} MYSQL Error: {e}', adjunto)
    
    def load_indices(self, engine, table, index_columns):
        try:
            connection = engine.connect()
            for column, es_texto in index_columns.items():
                index_name = f"idx_{column}"
                if self.indice_existe(connection, table, index_name):
                    print(f"Indice {index_name} ya esta creado en la tabla {table}")
                else:    
                    if es_texto:
                        sql = f"CREATE INDEX {index_name} ON {table} ({column}(255))"
                    else:
                        sql = f"CREATE INDEX {index_name} ON {table} ({column})"
                    connection.execute(text(sql))
                    print(f"Indice {index_name} creado correctamente en la tabla {table}")
            connection.close()          
        except Exception as e:
            print(f"Se ha presentado un error creado los indices en la table {table} en MYSQL Error: ", e)
    
    def indice_existe(self, engine, table, index_name):
        consulta_existencia = f"""
            SELECT COUNT(*)
            FROM information_schema.statistics
            WHERE table_schema = '{engine.dialect.default_schema_name}'
                AND table_name = '{table}'
                AND index_name = '{index_name}';
        """
        resultado = engine.execute(text(consulta_existencia)).scalar()
        return resultado > 0

    @staticmethod
    def load_log(engine, table, procesoalmacendo, operacion, cantDatos, falla, descripcionFalla=None):
        operation = 'append'
        connection_mysql = AlchemyConnection("STAGE")
        engine_mysql = connection_mysql.create_connection()
        df_log = pd.DataFrame(columns=['base_datos', 'operacion', 'tabla', 'proceso_almacenado', 'cantidad', 'fallo', 'descripcion_falla', 'fecha'])
        if engine == None:
            df_log.loc[0] = [engine, operacion, table, procesoalmacendo, cantDatos, falla, descripcionFalla, pd.to_datetime('today').strftime('%Y-%m-%d %H:%M:%S')]
        else:    
            df_log.loc[0] = [engine.url.database, operacion, table, procesoalmacendo, cantDatos, falla, descripcionFalla, pd.to_datetime('today').strftime('%Y-%m-%d %H:%M:%S')]
        df_log.to_sql('log_eventos_etls', engine_mysql, if_exists=operation, index=False)
        

