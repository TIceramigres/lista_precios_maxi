import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pandas as pd
import numpy as np
from utils.dates import *
from connections.alchemy_connection import AlchemyConnection
from utils.operations import Operations
import xlsxwriter
from envio_one_drive.enviar_lista_precio import EnviarListaPrecioOneDrive

class ListaPrecioInventario:
    def __init__(self):
        self.operation = Operations()
        connection_mssql = AlchemyConnection("UNOEE")
        self.engine_mssql = connection_mssql.create_connection()
        self.df_lista_precios = None
        self.df_lista_precios_CO = None
        self.df_inventario_CO = None

    def main(self):
        print('<--------------- Ejecutando Script Contigencia de lista de precio y inventario--------------->')
        print('<--------------- Ejecutando consulta de lista de precio --------------->')
        sql = "exec sp_items_lista_prc_cons 4,NULL,'2024-05-16 00:00:00',10114,'copia_contigencia',NULL,0,'2015-11-01 00:00:00','2015-11-27 00:00:00',NULL,NULL,NULL,NULL,NULL,0,1,NULL,NULL,NULL,NULL"
        self.df_lista_precios = self.operation.get_data(self.engine_mssql, sql)
        self.df_lista_precios['f_lista'] = self.df_lista_precios['f_lista'].astype(np.float64)
        self.df_lista_precios['f_item'] = self.df_lista_precios['f_item'].astype(np.float64)
        self.listaPreciosXCO()

    def listaPreciosXCO(self):
        print('<--------------- Ejecutando consulta de lista de precio por centro operaciones --------------->')
        sql="""SELECT f111_id_lista_precios,f111_id_co FROM t111_mc_promo_dsctos_linea where f111_id_co is not null and f111_id_lista_precios is not null and f111_id_cia = 4"""
        self.df_lista_precios_CO = self.operation.get_data(self.engine_mssql, sql)
        self.df_lista_precios_CO['f111_id_lista_precios'] = self.df_lista_precios_CO['f111_id_lista_precios'].astype(np.float64)
        self.df_lista_precios_CO['f111_id_co'] = self.df_lista_precios_CO['f111_id_co'].astype(np.float64)
        self.inventarioXCO()

    def inventarioXCO(self):
        print('<--------------- Ejecutando consulta inventario por Centro de operaciones --------------->')
        sql="""exec sp_cons_inv_exitencias 4,'          ','   ','   ',0,0,0,1,NULL,1,0,NULL,10017,'existencia_inventario_maxicassa'
                    ,3224,'COP',NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL,0,NULL,NULL"""
        self.df_inventario_CO = self.operation.get_data(self.engine_mssql, sql)
        self.df_inventario_CO['f_co_bodega'] = self.df_inventario_CO['f_co_bodega'].astype(np.float64)
        self.df_inventario_CO['f_item'] = self.df_inventario_CO['f_item'].astype(np.float64)
        self.procesarInformacion()
        
    def procesarInformacion(self):
        print('<--------------- Ejecutando procesamiento de la informacion --------------->')
        df_resultado = self.df_inventario_CO.merge(self.df_lista_precios_CO, left_on="f_co_bodega", right_on='f111_id_co', how="inner")
        # print(f'Se han retornado {df_resultado.shape[0]} registros 1')
        df_resultado = df_resultado.merge(self.df_lista_precios, left_on=['f111_id_lista_precios', 'f_item'], right_on=['f_lista', 'f_item'], how="inner")
        # print(f'Se han retornado {df_resultado.shape[0]} registros 2')
        columnas_a_eliminar = ['f_fecha_ultima_compra', 'f_fecha_ultima_venta', 'f_fecha_ultima_entrada', 'f_costo_prom_uni', 'f_peso_um', 'f_peso', 'f_tipo_inv', 'f_valor_total',
                               'f_costo_prom_tot_ins', 'f_instalacion', 'f_origen', 'f_rowid_item_cri', 'f_rowid_bodega', 'f_rowid_item_ext_x', 'f_ind_tipo_item', 'f_utilidad',
                               'f_divisor_margen', 'f111_id_co', 'f_fecha_inactivacion', 'f_rowid_item', 'f_rowid_item_ext_y', 'f_rowid_item_precio', 'f_01_011', 
                               'f111_id_lista_precios', 'f_cant_existencia_1', 'f_cant_disponible_1']
        df_resultado = df_resultado.drop(columns=columnas_a_eliminar)
        df_resultado = df_resultado.rename(columns={'f_co_bodega': 'CO',
                                                    'f_bodega':'BODEGA', 
                                                    'f_item':'ITEM', 
                                                    'f_um':'UNIDAD MEDIDAD',
                                                    'f_desc_item':'DESCRIPCION ITEM', 
                                                    'f_cant_disponible':'CANTIDAD DISPONIBLE', 
                                                    'f_cant_existencia':'CANTIDAD EXISTENCIA', 
                                                    # 'f_cant_existencia_1':'CANTIDAD EXISTENCIA 1', 
                                                    # 'f_cant_disponible_1':'CANTIDAD DISPONIBLE 1', 
                                                    'f_cant_existencia_actual_1':'CANTIDAD EXISTENCIA ACTUAL',
                                                    'f_lista':'LISTA', 
                                                    'f_lista_desc':'DESCRIPCION LISTA', 
                                                    'f_precio_min':'PRECIO MINIMO', 
                                                    'f_estado_item':'ESTADO' })
        df_resultado['PRECIO + IVA'] = None
        df_resultado = df_resultado[['CO', 'BODEGA', 'ITEM', 'DESCRIPCION ITEM', 'UNIDAD MEDIDAD', 'CANTIDAD DISPONIBLE', 'CANTIDAD EXISTENCIA',
                                 'CANTIDAD EXISTENCIA ACTUAL', 'LISTA', 'DESCRIPCION LISTA', 'PRECIO MINIMO', 'PRECIO + IVA', 'ESTADO']]
        df_resultado = df_resultado.drop_duplicates()
        self.generarExcel(df_resultado)

    def generarExcel(self, df_resultado):
        ahora = datetime.now()
        nombre_archivo = f'lista_precios_{ahora.strftime("%Y-%m-%d_%H-%M-%S")}.xlsx'
        workbook = xlsxwriter.Workbook(nombre_archivo, {'nan_inf_to_errors': True})
        grupos = df_resultado.groupby('CO')
        
        # Definir los formatos
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'fg_color': '#FFFF00',
            'border': 1
        })
        cell_format = workbook.add_format({
            'border': 1
        })
        currency_format = workbook.add_format({'border': 1, 'num_format': '_($* #,##0_);_($* (#,##0);_($* "-"_);_(@_)'})
        
        # Escribir cada grupo en una hoja separada
        for nombre, grupo in grupos:
            hoja_nombre = f'Co-{int(nombre)}'
            worksheet = workbook.add_worksheet(hoja_nombre)
            
            # Escribir las cabeceras con formato y filtros
            for i, col in enumerate(grupo.columns):
                worksheet.write(0, i, col, header_format)
                
            # Escribir los datos con formato y ajustar el tamaño de las columnas
            for i, (index, row) in enumerate(grupo.iterrows()):
                for j, value in enumerate(row):
                    if self.get_column_letter(j) == 'K':
                        worksheet.write(i + 1, j, value, currency_format)
                    else:
                        worksheet.write(i + 1, j, value, cell_format)
                worksheet.write_formula(f'L{i + 2}', f'=ROUNDUP((K{i + 2} * 1.19)/500, 0)*500 - 10', currency_format)
            
            # Ajustar el tamaño de las columnas
            for j, col in enumerate(grupo.columns):
                max_len = max(
                    grupo[col].astype(str).map(len).max(),  # Longitud máxima del contenido de la columna
                    len(col)  # Longitud del nombre de la columna
                ) + 2  # Añadir un poco de margen
                worksheet.set_column(j, j, max_len)
            worksheet.autofilter(0, 0, 0, len(grupo.columns) - 1)  # Agregar filtro a la cabecera
        
        workbook.close()
        print("iniciado envio")
        EnviarListaPrecioOneDrive(nombre_archivo)

    def get_column_letter(self, col_num):
        col_letter = ''
        while col_num > 0:
            col_num, remainder = divmod(col_num, 26)
            col_letter = chr(65 + remainder) + col_letter
        return col_letter


    # def removeArchivo(nombre_archivo):
    #     if os.path.exists(nombre_archivo):
    #         os.remove(nombre_archivo)
    
  
lista_precio_inventario = ListaPrecioInventario()
lista_precio_inventario.main()