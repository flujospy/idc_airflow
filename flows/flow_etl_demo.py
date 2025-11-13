import json, os
from glob import glob
from pathlib import Path
import pandas as pd
from prefect import flow, get_run_logger

from tasks.limpiar_datos import leer_excel, normalizar_columna, limpiar_basico
from tasks.transformar_csv import transformar

def _load_config():
    cfg_path = Path(__file__).resolve().parents[1] / "config.json"
    with open(cfg_path, "r", encoding="utf-8") as f:
        return json.load(f)

@flow
def etl_demo():
    cfg = _load_config()
    logger = get_run_logger()

    input_dir = cfg["paths"]["input_dir"]
    output_dir = cfg["paths"]["output_dir"]  # <- corregido
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    pattern    = cfg["etl_demo"].get("input_glob", "*.xlsx")
    sheet_name = cfg["etl_demo"].get("sheet_name", 0)
    out_name   = cfg["etl_demo"].get("output_name", "clean_consolidado.xlsx")  # <- corregido

    archivos = glob(os.path.join(input_dir, pattern))
    if not archivos:
        logger.warning(f"No hay archivos Excel ({pattern}) en {input_dir}")
        return

    logger.info(f"Procesando {len(archivos)} archivo(s) de Excel...")

    futures = []
    for f in archivos:
        df = leer_excel.submit(f, sheet_name)
        df = normalizar_columna.submit(df)
        df = limpiar_basico.submit(df)
        df = transformar.submit(df)
        futures.append(df)

    final = pd.concat([d.result() for d in futures], ignore_index=True)

    out_path = os.path.join(output_dir, out_name)
    final.to_excel(out_path, index=False, engine="openpyxl")

    logger.info(f"Archivo Excel generado: {out_path}")
    logger.info(f"Filas totales: {len(final):,}")

if __name__ == "__main__":
    etl_demo()