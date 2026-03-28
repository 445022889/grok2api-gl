"""Token 刷新调度器"""

import asyncio
from typing import Optional

from app.core.config import get_config
from app.core.logger import logger
from app.core.storage import get_storage, StorageError, RedisStorage
from app.services.token.manager import get_token_manager


class TokenRefreshScheduler:
    """Token 自动刷新调度器"""

    def __init__(self, interval_hours: int = 8):
        self.interval_hours = interval_hours
        self.interval_seconds = interval_hours * 3600
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._last_super_quota_refresh_at = 0.0

    @staticmethod
    def _resolve_loop_interval_seconds(default_hours: float = 8) -> float:
        intervals = []
        try:
            intervals.append(float(get_config("token.refresh_interval_hours", default_hours)) * 3600)
        except Exception:
            intervals.append(float(default_hours) * 3600)
        try:
            intervals.append(float(get_config("token.super_refresh_interval_hours", default_hours)) * 3600)
        except Exception:
            pass
        try:
            super_quota_minutes = float(
                get_config("token.super_quota_refresh_interval_minutes", 0)
            )
            if super_quota_minutes > 0:
                intervals.append(super_quota_minutes * 60)
        except Exception:
            pass
        intervals = [value for value in intervals if value and value > 0]
        if not intervals:
            return max(60.0, float(default_hours) * 3600)
        return max(60.0, min(intervals))

    async def _refresh_loop(self):
        """刷新循环"""
        logger.info(f"Scheduler: started (base interval: {self.interval_hours}h)")

        while self._running:
            try:
                self.interval_seconds = self._resolve_loop_interval_seconds(
                    self.interval_hours
                )
                storage = get_storage()
                lock_acquired = False
                lock = None

                if isinstance(storage, RedisStorage):
                    # Redis: non-blocking lock to avoid multi-worker duplication
                    lock_key = "grok2api:lock:token_refresh"
                    lock = storage.redis.lock(
                        lock_key, timeout=self.interval_seconds + 60, blocking_timeout=0
                    )
                    lock_acquired = await lock.acquire(blocking=False)
                else:
                    try:
                        async with storage.acquire_lock("token_refresh", timeout=1):
                            lock_acquired = True
                    except StorageError:
                        lock_acquired = False

                if not lock_acquired:
                    logger.info("Scheduler: skipped (lock not acquired)")
                    await asyncio.sleep(self.interval_seconds)
                    continue

                try:
                    logger.info("Scheduler: starting token refresh...")
                    manager = await get_token_manager()
                    result = await manager.refresh_cooling_tokens()

                    logger.info(
                        f"Scheduler: refresh completed - "
                        f"checked={result['checked']}, "
                        f"refreshed={result['refreshed']}, "
                        f"recovered={result['recovered']}, "
                        f"expired={result['expired']}"
                    )

                    try:
                        interval_minutes = float(
                            get_config("token.super_quota_refresh_interval_minutes", 0)
                        )
                    except Exception:
                        interval_minutes = 0.0
                    try:
                        threshold = int(
                            get_config("token.super_quota_refresh_threshold", 500)
                        )
                    except Exception:
                        threshold = 500

                    now = asyncio.get_running_loop().time()
                    should_refresh_super_quota = (
                        interval_minutes > 0
                        and threshold > 0
                        and (
                            self._last_super_quota_refresh_at <= 0
                            or now - self._last_super_quota_refresh_at
                            >= interval_minutes * 60
                        )
                    )
                    if should_refresh_super_quota:
                        logger.info(
                            "Scheduler: starting super quota refresh..."
                        )
                        super_result = await manager.refresh_super_tokens_below_threshold(
                            trigger="scheduler_super_quota",
                            quota_threshold=threshold,
                        )
                        self._last_super_quota_refresh_at = now
                        logger.info(
                            f"Scheduler: super quota refresh completed - "
                            f"checked={super_result['checked']}, "
                            f"refreshed={super_result['refreshed']}, "
                            f"recovered={super_result['recovered']}, "
                            f"expired={super_result['expired']}, "
                            f"threshold={threshold}"
                        )
                finally:
                    if lock is not None and lock_acquired:
                        try:
                            await lock.release()
                        except Exception:
                            pass

                await asyncio.sleep(self.interval_seconds)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Scheduler: refresh error - {e}")
                await asyncio.sleep(self.interval_seconds)

    def start(self):
        """启动调度器"""
        if self._running:
            logger.warning("Scheduler: already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._refresh_loop())
        logger.info("Scheduler: enabled")

    def stop(self):
        """停止调度器"""
        if not self._running:
            return

        self._running = False
        if self._task:
            self._task.cancel()
        logger.info("Scheduler: stopped")


# 全局单例
_scheduler: Optional[TokenRefreshScheduler] = None


def get_scheduler(interval_hours: int = 8) -> TokenRefreshScheduler:
    """获取调度器单例"""
    global _scheduler
    if _scheduler is None:
        _scheduler = TokenRefreshScheduler(interval_hours)
    return _scheduler


__all__ = ["TokenRefreshScheduler", "get_scheduler"]
