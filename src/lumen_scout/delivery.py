from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

from .models import Lead


class Delivery(ABC):
    @abstractmethod
    def deliver(self, lead: Lead, content: str, output_path: Path) -> None:
        raise NotImplementedError


class ManualDelivery(Delivery):
    def deliver(self, lead: Lead, content: str, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(content.strip() + "\n", encoding="utf-8")


class GmailDraftDelivery(Delivery):
    """Stub for future Gmail draft support (v2)."""

    def deliver(self, lead: Lead, content: str, output_path: Path) -> None:
        raise NotImplementedError("Gmail draft delivery is not implemented in v1")
