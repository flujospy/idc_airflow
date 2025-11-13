from datetime import datetime, timedelta
from typing import List, Tuple

def split_date_range(
    fecha_inicio: str,
    fecha_fin: str,
    batch_days: int = 15
) -> List[Tuple[str, str]]:
    """Divide un rango de fechas en lotes más pequeños"""
    start = datetime.strptime(fecha_inicio, '%Y-%m-%d')
    end = datetime.strptime(fecha_fin, '%Y-%m-%d')
    
    batches = []
    current = start
    
    while current <= end:
        batch_end = min(current + timedelta(days=batch_days - 1), end)
        batches.append((
            current.strftime('%Y-%m-%d'),
            batch_end.strftime('%Y-%m-%d')
        ))
        current = batch_end + timedelta(days=1)
    
    return batches