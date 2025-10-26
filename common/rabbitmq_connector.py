import asyncio
import logging
from typing import Optional

from aiormq.exceptions import AMQPConnectionError

from .rpc.rabbitmq_client import RabbitMQClient

_DEFAULT_MAX_RETRIES = 5
_DEFAULT_RETRY_DELAY = 2.0

logger = logging.getLogger(__name__)


class RabbitmqConnector:
    @staticmethod
    def create(host: str,
               port: str,
               login: str,
               password: str,
               connection_name: Optional[str] = None) -> RabbitMQClient:
        return RabbitMQClient(host=host,
                              port=int(port),
                              login=login,
                              password=password,
                              connection_name=connection_name)

    @staticmethod
    async def connect(host: str,
                      port: str,
                      login: str,
                      password: str,
                      connection_name: Optional[str] = None,
                      max_retries: int = _DEFAULT_MAX_RETRIES,
                      retry_delay: float = _DEFAULT_RETRY_DELAY):
        attempt = 0
        while True:
            client = RabbitMQClient(host=host,
                                    port=int(port),
                                    login=login,
                                    password=password,
                                    connection_name=connection_name)
            try:
                return await client.connect()
            except (AMQPConnectionError, OSError) as exc:
                attempt += 1
                if attempt > max_retries:
                    logger.error(
                        "Failed to connect to RabbitMQ after %s attempts: %s",
                        attempt,
                        exc,
                    )
                    raise exc
                logger.warning(
                    "Failed to connect to RabbitMQ (attempt %s/%s). Retrying in %.1f seconds: %s",
                    attempt,
                    max_retries,
                    retry_delay,
                    exc,
                )
                await asyncio.sleep(retry_delay)
