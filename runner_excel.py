import argparse
from flows.etl_onedrive_excel_to_mysql import etl_ondedrive_excel_to_mysql


def main():
    parser = argparse.ArgumentParser(

        description="Runner para extraccion y carga de archivo maestro"
    )
    #parser.add_argument(

    #    "--no-show-head",
    #    action="store_true",
    #    help="No mostrar las primeras filas del Dataframe en los logs."
    #)

    _=parser.parse_args()

    etl_ondedrive_excel_to_mysql()


    #args = parser.parse_args()
    rows = etl_ondedrive_excel_to_mysql()

    print(f"[runner_excel] Flow completado. filas leidas {rows}")

if __name__ == "__main__":
    main()