import logging
import httpx
import json
import asyncio
from typing import Optional
from collections import deque
from time import time
from datetime import datetime
import os

class BetterStackHandler(logging.Handler):
    """
    Asynchronous logging handler for sending logs to Better Stack.

    Args:
        token (str): Better Stack authentication token.
        url (str): Better Stack ingest URL.
        max_buffer_size (int, optional): Maximum number of logs to buffer before sending. Defaults to 1.
        flush_interval (float, optional): Maximum time (in seconds) to wait before flushing buffer. Defaults to 1.0.
    """
    def __init__(self, token: str, url: str, max_buffer_size: int = 1, flush_interval: float = 1.0):
        super().__init__()
        if not token:
            raise ValueError("Better Stack token cannot be empty")
        if not url:
            raise ValueError("Better Stack URL cannot be empty")
        self.token = token
        self.url = url
        self.buffer = deque(maxlen=max_buffer_size)
        self.flush_interval = flush_interval
        self.last_flush = time()
        self._error_logger = logging.getLogger('BetterStackError')
        self._error_logger.setLevel(logging.ERROR)
        self._error_handler = logging.StreamHandler()
        self._error_logger.addHandler(self._error_handler)

    async def _send_logs(self, logs: list):
        """Send buffered logs to Better Stack asynchronously."""
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}"
        }
        async with httpx.AsyncClient() as client:
            try:
                for log in logs:
                    self._error_logger.info(f"Sending log to Better Stack: {log}")
                    response = await client.post(self.url, headers=headers, json=log)
                    self._error_logger.info(f"Log sent successfully, status: {response.status_code}")
            except Exception as e:
                self._error_logger.error(f"Failed to send logs to Better Stack: {str(e)}")

    def emit(self, record: logging.LogRecord):
        """
        Process a log record and add it to the buffer.

        Args:
            record: The log record to process.
        """
        try:
            log_entry = {
                "dt": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
                "message": self.format(record),
                "level": record.levelname
            }
            self.buffer.append(log_entry)
            if len(self.buffer) >= self.buffer.maxlen or (time() - self.last_flush) >= self.flush_interval:
                asyncio.create_task(self._flush_async())
        except Exception as e:
            self._error_logger.error(f"Error processing log record: {str(e)}")

    async def _flush_async(self):
        """Asynchronous flush of buffered logs to Better Stack."""
        if self.buffer:
            logs = list(self.buffer)
            self.buffer.clear()
            self.last_flush = time()
            await self._send_logs(logs)

    def flush(self):
        """
        Synchronous flush method to satisfy logging.Handler interface.
        Schedules async flush if event loop is running, otherwise logs warning.
        """
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._flush_async())
        except RuntimeError as e:
            self._error_logger.warning(f"Cannot flush logs: {str(e)}")

    def close(self):
        """
        Close the handler and flush remaining logs synchronously if possible.
        """
        if self.buffer:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._flush_async())
            except RuntimeError as e:
                self._error_logger.warning(f"Cannot flush logs: {str(e)}")
        super().close()

def setup_logging(
    token: str,
    url: str,
    level: int = logging.INFO,
    format_string: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
) -> logging.Logger:
    """
    Configure logging with Better Stack handler and console output.

    Args:
        token: Better Stack authentication token.
        url: Better Stack ingest URL.
        level: Logging level (default: logging.INFO).
        format_string: Log message format (default: '%(asctime)s - %(name)s - %(levelname)s - %(message)s').

    Returns:
        Configured logger instance.

    Raises:
        ValueError: If token or URL is empty or None.
    """
    if not token:
        raise ValueError("Better Stack token cannot be empty")
    if not url:
        raise ValueError("Better Stack URL cannot be empty")

    logger = logging.getLogger('ArticleSearch')
    logger.setLevel(level)
    formatter = logging.Formatter(format_string)

    # Better Stack handler
    better_stack_handler = BetterStackHandler(token=token, url=url)
    better_stack_handler.setFormatter(formatter)
    logger.addHandler(better_stack_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger