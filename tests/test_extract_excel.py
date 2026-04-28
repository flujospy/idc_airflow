# tests/test_extract_excel.py

import os
import sys

# 1) Agregar la carpeta raíz del proyecto al sys.path
#    Esto hace que "tasks", "config", etc. sean importables.
CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
sys.path.append(PROJECT_ROOT)



from tasks.extract_excel_maestro import extract_excel_from_onedrive

if __name__== "__main__":
    df = extract_excel_from_onedrive.fn()
    print("Columnas: ", df.columns.tolist())
    print(df.head())