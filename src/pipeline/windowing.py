from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta


@dataclass(frozen=True)
class ProcessingWindow:
    start_date: date
    end_date: date

    @property
    def start_iso(self) -> str:
        return self.start_date.isoformat()

    @property
    def end_iso(self) -> str:
        return self.end_date.isoformat()


def parse_process_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def build_processing_window(process_date: str, lookback_days: int) -> ProcessingWindow:
    if lookback_days < 0:
        raise ValueError("lookback_days must be zero or positive")

    end_date = parse_process_date(process_date)
    start_date = end_date - timedelta(days=lookback_days)
    return ProcessingWindow(start_date=start_date, end_date=end_date)
