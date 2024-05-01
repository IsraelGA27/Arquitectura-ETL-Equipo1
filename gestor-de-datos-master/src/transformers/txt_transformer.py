#!/usr/bin/env python
# -*- coding: utf-8 -*-
#-------------------------------------------------------------------------
# Archivo: txt_transformer.py
# Autor: Equipo #1 basandonos en el trabajo de: Perla Velasco & Yonathan Mtz. & Jorge Solís
# Versión: 1.0.0 Abril 2024
# Descripción:
#
# Este archivo define un procesador de datos que se encarga de transformar
# y formatear el contenido de un archivo de texto (.txt)
#-------------------------------------------------------------------------

from src.extractors.txt_extractor import TXTExtractor
from os.path import join
import luigi, os, json

class TXTTransformer(luigi.Task):
    def requires(self):
        return TXTExtractor()

    def run(self):
        result = []    
        for input_file in self.input():
            with input_file.open('r') as txt_file:
                next(txt_file)
                content = txt_file.read()
                records = content.split(';') #Separa el contenido del archivo utilizando ";" para obtener cada registro por separado.
                for record in records:
                    fields = [field.strip() for field in record.strip().split(',')] # Separa los campos de cada registro utilizando la coma como delimitador.
                    if len(fields) >= 8: # Comprueba si hay al menos 8 campos en el registro
                        result.append({ # Agrega un diccionario con los campos procesados a la lista de resultados
                            "description": fields[2],
                            "quantity": int(fields[3]),
                            "price": float(fields[5]),
                            "total": int(fields[3]) * float(fields[5]), # Calcula el total multiplicando la cantidad por el precio como sucede en los demás transformer
                            "invoice": fields[0],
                            "provider": fields[6],
                            "country": fields[7]
                        })

        with self.output().open('w') as out:
            out.write(json.dumps(result, indent=4))

    def output(self):
        project_dir = os.path.dirname(os.path.abspath("loader.py"))
        result_dir = os.path.join(project_dir, "result")
        return luigi.LocalTarget(os.path.join(result_dir, "txt.json"))