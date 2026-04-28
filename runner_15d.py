# runner_15d.py
import argparse
from datetime import datetime, timedelta
from typing import Iterator, Tuple, Optional

from flows.etl_api_lotes import etl_api_to_mysql_flow  # 

def chunked_dates(start: str, end: str, step_days: int = 5) -> Iterator[Tuple[str, str]]:
    d0 = datetime.strptime(start, "%Y-%m-%d").date()
    d1 = datetime.strptime(end, "%Y-%m-%d").date()
    cur = d0
    while cur <= d1:
        sub_end = min(cur + timedelta(days=step_days - 1), d1)
        yield (cur.isoformat(), sub_end.isoformat())
        cur = sub_end + timedelta(days=1)

def main():
    p = argparse.ArgumentParser(description="Ejecuta el ETL por bloques de 15 días.")
    p.add_argument("--inicio", required=True, help="YYYY-MM-DD")
    p.add_argument("--fin", required=True, help="YYYY-MM-DD")
    p.add_argument("--tabla", default="tabla_saldos")
    p.add_argument("--endpoint", default="/General/ConsultarSaldos")
    p.add_argument("--codigo-producto", type=int, default=None)
    p.add_argument("--numero-identificacion", default=None)
    p.add_argument("--tipo-producto", default="M")
    p.add_argument("--tipo-cliente", default="M")
    p.add_argument("--entity-name", default="datos_api")
    args = p.parse_args()

    print("="*70)
    print(f"Rango total: {args.inicio} a {args.fin} (bloques de 15 días)")
    print(f"Tabla destino: {args.tabla}")
    print("="*70)

    total_runs = 0
    ok = 0
    fail = 0

    for i, (ini, fin) in enumerate(chunked_dates(args.inicio, args.fin, 5), 1):
        print(f"\n>>> Bloque {i}: {ini} .. {fin}")
        total_runs += 1
        try:
            # Llama al flow (que internamente hace DELETE+LOAD del sub-rango)
            result = etl_api_to_mysql_flow(
                fecha_inicio=ini,
                fecha_fin=fin,
                entity_name=args.entity_name,
                tabla_destino=args.tabla,
                endpoint=args.endpoint,
                numero_identificacion=args.numero_identificacion,
                codigo_producto=args.codigo_producto,
                tipo_producto=args.tipo_producto,
                tipo_cliente=args.tipo_cliente,
            )
            status = (result or {}).get("status", "UNKNOWN")
            if status == "SUCCESS":
                ok += 1
                print(f"✔ Bloque {i} OK")
            else:
                fail += 1
                print(f"✖ Bloque {i} terminó con estado: {status}")
        except Exception as e:
            fail += 1
            print(f"✖ Bloque {i} error: {e}")

    print("\n" + "="*70)
    print(f"Bloques ejecutados: {total_runs}  |  OK: {ok}  |  FAIL: {fail}")
    print("="*70)

if __name__ == "__main__":
    main()
