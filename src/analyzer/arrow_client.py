"""Arrow Flight клиент — получает агрегированные данные вакансий от Go-сервера."""

import pyarrow.flight as flight
import polars as pl
import logging
from typing import Iterator

logger = logging.getLogger(__name__)


class VacancyFlightClient:
    """Клиент Arrow Flight для получения агрегированных окон вакансий."""

    def __init__(self, host: str = "localhost", port: int = 50051):
        self.location = flight.Location.for_grpc_tcp(host, port)
        self.client: flight.FlightClient | None = None

    def connect(self) -> None:
        self.client = flight.connect(self.location)
        logger.info(f"Connected to Arrow Flight server at {self.location}")

    def close(self) -> None:
        if self.client:
            self.client.close()
            self.client = None

    def stream_windows(self) -> Iterator[pl.DataFrame]:
        """Стримит агрегированные окна как Polars DataFrame."""
        if not self.client:
            self.connect()

        ticket = flight.Ticket(b"vacancies")
        reader = self.client.do_get(ticket)

        for chunk, _ in reader:
            df = pl.from_arrow(chunk)
            logger.info(f"Received window: {len(df)} rows")
            yield df

    def fetch_all(self) -> pl.DataFrame:
        """Получает все доступные данные в один DataFrame."""
        frames = list(self.stream_windows())
        if not frames:
            return pl.DataFrame()
        return pl.concat(frames)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()
