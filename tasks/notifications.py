from prefect import task, get_run_logger
import requests
from config.settings import settings
from typing import Dict, Any

@task(name="Enviar notificación")
def send_notification(mensaje: str, metadata: Dict[str, Any] = None):
    """Envía notificación a Slack"""
    logger = get_run_logger()
    
    if not settings.SLACK_WEBHOOK:
        logger.info("Slack no configurado, omitiendo notificación")
        return
    
    payload = {
        "text": mensaje,
        "blocks": [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": mensaje}
            }
        ]
    }
    
    if metadata:
        fields = [
            {"type": "mrkdwn", "text": f"*{k}:* {v}"}
            for k, v in metadata.items()
        ]
        payload["blocks"].append({
            "type": "section",
            "fields": fields
        })
    
    try:
        response = requests.post(settings.SLACK_WEBHOOK, json=payload)
        response.raise_for_status()
        logger.info(" Notificación enviada")
    except Exception as e:
        logger.error(f"Error enviando notificación: {str(e)}")