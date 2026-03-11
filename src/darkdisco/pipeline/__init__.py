"""Pipeline: poll sources → match watch terms → create findings → alert."""

from darkdisco.pipeline.worker import app as celery_app

__all__ = ["celery_app"]
