from flows.opportunity_lifecycle import flujo_opportunity_lifecycle

if __name__ == "__main__":
    flujo_opportunity_lifecycle.serve(
        name="crm-lifecycle-auto-3x-dia",
        parameters={"ventana_dias": 1, "usuario": "scheduler-crm"},
        cron="0 15,19,23 * * *",
        tags=["crm", "lifecycle", "automatico"],
        description="Lifecycle CRM — 9:00 / 13:00 / 17:00 GT"
    )