"""
Задание 7 (повышенное): потоковый NATS-консьюмер со скользящим окном.

Отличие от tumbling window (Go):
  - Tumbling window: фиксированные неперекрывающиеся отрезки времени (каждые N сек — сброс)
  - Sliding window (здесь): в каждый момент содержит события за последние window_sec секунд;
    «скользит» вперёд непрерывно — старые события вытесняются по мере поступления новых.

Поток данных:
  Go-сборщик → NATS topic "vacancies" → SlidingWindowConsumer → агрегации в реальном времени

Запуск:
    python nats_consumer.py --nats-url nats://localhost:4222 --window-sec 60
"""

import argparse
import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field

import nats

logger = logging.getLogger(__name__)


@dataclass
class WindowStats:
    """Агрегированная статистика текущего скользящего окна."""
    window_sec: int
    total: int
    by_area: dict = field(default_factory=dict)
    avg_salary_from: float = 0.0
    top_area: str = ""
    top_area_count: int = 0


class SlidingWindowConsumer:
    """
    Подписывается на NATS-топик и поддерживает скользящее окно заданной ширины.

    Буфер — deque пар (monotonic_timestamp, vacancy_dict).
    При каждом новом сообщении события старше window_sec удаляются из начала deque.
    """

    def __init__(self, nats_url: str, subject: str = "vacancies", window_sec: int = 60):
        self.nats_url = nats_url
        self.subject = subject
        self.window_sec = window_sec
        self._buffer: deque[tuple[float, dict]] = deque()

    def _evict_old(self) -> None:
        """Вытесняет события старше window_sec из начала буфера."""
        cutoff = time.monotonic() - self.window_sec
        while self._buffer and self._buffer[0][0] < cutoff:
            self._buffer.popleft()

    def _aggregate(self) -> WindowStats:
        """Считает агрегации по текущему содержимому скользящего окна."""
        stats = WindowStats(window_sec=self.window_sec, total=len(self._buffer))
        salary_sum, salary_count = 0.0, 0

        for _, v in self._buffer:
            # area_name может быть как плоским полем (после clean_data), так и вложенным
            area = v.get("area_name") or (v.get("area") or {}).get("name", "Unknown")
            stats.by_area[area] = stats.by_area.get(area, 0) + 1

            salary = v.get("salary_from") or 0
            if not salary and v.get("salary"):
                salary = (v["salary"] or {}).get("from") or 0
            if salary and salary > 0:
                salary_sum += salary
                salary_count += 1

        if salary_count:
            stats.avg_salary_from = salary_sum / salary_count

        if stats.by_area:
            stats.top_area = max(stats.by_area, key=lambda k: stats.by_area[k])
            stats.top_area_count = stats.by_area[stats.top_area]

        return stats

    def _print_stats(self, stats: WindowStats) -> None:
        print(
            f"\n[sliding window {stats.window_sec}s]  "
            f"total={stats.total}  "
            f"top={stats.top_area}({stats.top_area_count})  "
            f"avg_salary_from={stats.avg_salary_from:,.0f} ₽"
        )
        top5 = sorted(stats.by_area.items(), key=lambda x: x[1], reverse=True)[:5]
        for area, cnt in top5:
            print(f"  {area:<25} {cnt:>5} вакансий")

    async def run(self, report_interval: int = 10) -> None:
        """
        Запускает асинхронный event loop:
          - подписка на NATS топик
          - обработка каждого входящего сообщения (добавление в буфер, вытеснение старых)
          - вывод агрегаций каждые report_interval секунд
        """
        nc = await nats.connect(self.nats_url)
        logger.info(f"[nats] connected → {self.nats_url}, subject={self.subject}, window={self.window_sec}s")
        print(f"[nats] подключён к {self.nats_url} | топик={self.subject} | окно={self.window_sec}с")

        received_total = 0

        async def _handler(msg):
            nonlocal received_total
            try:
                vacancy = json.loads(msg.data.decode())
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.warning(f"[nats] bad message: {e}")
                return

            self._buffer.append((time.monotonic(), vacancy))
            self._evict_old()
            received_total += 1

            if received_total % 50 == 0:
                logger.info(f"[nats] received={received_total}, window_size={len(self._buffer)}")

        await nc.subscribe(self.subject, cb=_handler)
        print(f"[nats] подписка оформлена, ожидаем вакансии...\n")

        try:
            last_report = time.monotonic()
            while True:
                await asyncio.sleep(1)
                if time.monotonic() - last_report >= report_interval:
                    self._evict_old()
                    stats = self._aggregate()
                    self._print_stats(stats)
                    last_report = time.monotonic()
        except asyncio.CancelledError:
            pass
        finally:
            await nc.drain()
            logger.info("[nats] connection drained, consumer stopped")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="NATS sliding window consumer")
    parser.add_argument("--nats-url", default="nats://localhost:4222", help="NATS server URL")
    parser.add_argument("--subject", default="vacancies", help="NATS subject (topic)")
    parser.add_argument("--window-sec", type=int, default=60, help="скользящее окно (секунды)")
    parser.add_argument("--report-interval", type=int, default=10, help="интервал вывода статистики (сек)")
    args = parser.parse_args()

    consumer = SlidingWindowConsumer(args.nats_url, args.subject, args.window_sec)
    asyncio.run(consumer.run(report_interval=args.report_interval))


if __name__ == "__main__":
    main()
