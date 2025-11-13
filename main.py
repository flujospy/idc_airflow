# main.py
from pathlib import Path
from dotenv import load_dotenv

# 1) Cargar .env ANTES de importar el flow
load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env", override=True)

from flows.etl_api_lotes import etl_api_to_mysql_flow  # <-- ahora sí
from datetime import datetime, timedelta
import argparse
import httpx
import os

def _flag(name: str) -> str:
    v = os.getenv(name)
    return "OK" if (v is not None and v.strip() != "") else "NO"

def main():
    """Punto de entrada principal"""
    parser = argparse.ArgumentParser(description="Ejecutar ETL de API a MySQL")
    parser.add_argument(
        "--fecha-inicio",
        type=str,
        help="Fecha inicial (YYYY-MM-DD)",
        default=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    )
    parser.add_argument(
        "--fecha-fin",
        type=str,
        help="Fecha final (YYYY-MM-DD)",
        default=datetime.now().strftime("%Y-%m-%d")
    )
    parser.add_argument(
        "--numero-identificacion",
        type=str,
        help="Número de identificación (opcional)",
        default=None
    )
    parser.add_argument(
        "--codigo-producto",
        type=int,
        help="Código de producto (opcional)",
        default=None
    )
    parser.add_argument(
        "--tipo-producto",
        type=str,
        help='Valor para tipoConsulta.producto (por defecto "M")',
        default="M"
    )
    parser.add_argument(
        "--tipo-cliente",
        type=str,
        help='Valor para tipoConsulta.cliente (por defecto "M")',
        default="M"
    )
    parser.add_argument(
        "--entity-name",
        type=str,
        help="Nombre de la entidad",
        default="datos_api"
    )
    parser.add_argument(
        "--tabla-destino",
        type=str,
        help="Tabla MySQL destino",
        default="datos_api"
    )
    parser.add_argument(
        "--endpoint",
        type=str,
        help="Endpoint de la API",
        default="/api/v1/datos"
    )

    args = parser.parse_args()

    print("=" * 70)
    print(" INICIANDO ETL")
    print("=" * 70)
    print(f" Rango: {args.fecha_inicio} a {args.fecha_fin}")
    print(f" Entidad: {args.entity_name}")
    print(f" Tabla: {args.tabla_destino}")
    print(f" Endpoint: {args.endpoint}")
    print("=" * 70)

    # (Opcional) Verificación rápida de variables esperadas
    print("\n[ENV CHECK] Variables esperadas:")
    for k in ["API_TOKEN", "API_AUTH_URL", "API_USERNAME", "API_PASSWORD", "API_BASE_URL"]:
        print(f"  {k}: {_flag(k)}")

    try:
        result = etl_api_to_mysql_flow(
            fecha_inicio=args.fecha_inicio,
            fecha_fin=args.fecha_fin,
            entity_name=args.entity_name,
            tabla_destino=args.tabla_destino,
            endpoint=args.endpoint,
            numero_identificacion=args.numero_identificacion,
            codigo_producto=args.codigo_producto,
            tipo_producto=args.tipo_producto,
            tipo_cliente=args.tipo_cliente,
        )
    except httpx.ConnectError:
        print("--- Aviso: no se pudo reportar estado a la Prefect API local (ws/http). "
              "Revisa firewall/antivirus o usa 'prefect server start' y "
              "exporta PREFECT_API_URL ---")
        raise

    print("\n" + "=" * 70)
    print(" ETL COMPLETADO")
    print("=" * 70)
    print(f" Run ID: {result['run_id']}")
    print(f" Status: {result['status']}")
    print(f" Registros: {result['total_registros']}")
    print(f" Lotes exitosos: {result['lotes_exitosos']}")
    print(f" Lotes fallidos: {result['lotes_fallidos']}")
    print(f" Duración: {result['duracion_segundos']:.2f}s")
    print("=" * 70)


if __name__ == "__main__":
    main()
