import argparse
from prefect import flow
from prefect.settings import PREFECT_API_URL
from flows.elt_AP_api import etl_aportes_retiros_flow



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Runner ETL de Movimientos (Aportes/Retiros) en bloques de 15 días"
    )
    parser.add_argument(
        "--inicio",
        required=True,
        help="Fecha de inicio (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--fin",
        required=True,
        help="Fecha de fin (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--tabla",
        default="movimiento",
        help="Nombre de la tabla destino en MySQL (default: movimiento)",
    )
    parser.add_argument(
        "--endpoint",
        default="/General/ConsultarMovimientos",
        help="Endpoint de la API de movimientos (default: /General/ConsultarMovimientos)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    print("=" * 70)
    print(" ETL MOVIMIENTOS (Aportes/Retiros)")
    print("=" * 70)
    print(f" Rango total: {args.inicio} a {args.fin}")
    print(f" Tabla destino: {args.tabla}")
    print(f" Endpoint: {args.endpoint}")
    print("=" * 70)
    print()

    # Ejecuta el flow de Prefect
    result = etl_aportes_retiros_flow(
        inicio=args.inicio,
        fin=args.fin,
        endpoint=args.endpoint,
        tabla_destino=args.tabla,
    )

    print()
    print("=" * 70)
    print(" RESUMEN DEVUELTO POR EL FLOW")
    print("=" * 70)
    print(result)


if __name__ == "__main__":
    main()