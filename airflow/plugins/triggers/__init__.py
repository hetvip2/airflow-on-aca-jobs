try:
    from triggers.azure_container_apps_job_trigger import (
        AzureContainerAppsJobTrigger,
    )
except ModuleNotFoundError:  # pragma: no cover - import fallback
    from plugins.triggers.azure_container_apps_job_trigger import (
        AzureContainerAppsJobTrigger,
    )

__all__ = ["AzureContainerAppsJobTrigger"]
