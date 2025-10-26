import json
import logging
import re
from datetime import UTC, datetime

from aiohttp import ClientSession

from proxy_http.async_proxies_concurrent_limiter import AsyncSessionConcurrentLimiter
from proxy_http.decorators import catch_aiohttp
from .abstract_parser import MaxAttemptsReachedError, AbstractCsmoneyParser
from ._name_patcher import patch_market_name
from ....models.csmoney import CsmoneyItem, CsmoneyItemPack, CsmoneyItemCategory
from ....queues import AbstractCsmoneyWriter

_RESPONSE_TIMEOUT = 10
_POSTPONE_DURATION = 25
_MAX_ATTEMPTS_DEFAULT = 300
_NEXT_DATA_PATTERN = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json"[^>]*>(?P<data>.*?)</script>',
    re.DOTALL,
)
_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://cs.money/csgo/trade",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

_CLOUDFLARE_KEYWORDS = (
    "just a moment",
    "cf-mitigated",
    "cf-browser-verification",
    "cf-chl",
)

logger = logging.getLogger(__name__)


def _csmoney_unix_to_datetime(unix: int | None) -> datetime | None:
    if unix:
        return datetime.fromtimestamp(unix / 1000, UTC)
    return None


def _extract_next_data(html: str) -> dict:
    match = _NEXT_DATA_PATTERN.search(html)
    if not match:
        raise ValueError("__NEXT_DATA__ script not found in cs.money response")
    return json.loads(match.group("data"))


def _extract_skins(data: dict) -> list[dict]:
    try:
        props = data["props"]
        page_props = props["pageProps"]
        bot_init = page_props["botInitData"]
        skins_info = bot_init["skinsInfo"]
    except KeyError as exc:  # pragma: no cover - defensive branch
        raise ValueError("Unexpected structure of cs.money page") from exc

    skins = skins_info.get("skins")
    if not isinstance(skins, list):
        return []
    return skins


def _is_cloudflare_challenge(html: str) -> bool:
    lowered = html.lower()
    return any(keyword in lowered for keyword in _CLOUDFLARE_KEYWORDS)


def _create_items(json_item) -> list[CsmoneyItem]:
    if "fullName" not in json_item:
        return []
    name = patch_market_name(json_item["fullName"])
    overpay = json_item.get("overpay", None)
    overpay_float = overpay.get("float", None) if overpay else None
    items = [
        CsmoneyItem(
            name=name,
            price=json_item["price"],
            asset_id=str(json_item["assetId"]),
            name_id=json_item["nameId"],
            type_=CsmoneyItemCategory(json_item["type"]),
            float_=json_item.get("float", None),
            unlock_timestamp=_csmoney_unix_to_datetime(json_item.get("tradeLock", None)),
            overpay_float=overpay_float,
        )
    ]
    is_stack = "stackSize" in json_item and "stackId" in json_item and "stackItems" in json_item
    if is_stack:
        for stack_item in json_item["stackItems"]:
            items.append(
                CsmoneyItem(
                    name=name,
                    price=json_item["price"],
                    asset_id=str(stack_item["id"]),
                    name_id=json_item["nameId"],
                    type_=CsmoneyItemCategory(json_item["type"]),
                    float_=stack_item.get("float", None),
                    unlock_timestamp=_csmoney_unix_to_datetime(stack_item["tradeLock"]),
                    overpay_float=None,
                )
            )
    return items


@catch_aiohttp(logger)
async def _request(session: ClientSession, url: str) -> str | None:
    async with session.get(url, timeout=_RESPONSE_TIMEOUT, headers=_REQUEST_HEADERS) as response:
        response.raise_for_status()
        text = await response.text()
        if _is_cloudflare_challenge(text):
            logger.warning("Cloudflare challenge detected for %s", url)
            return None
        return text


async def _process_items(items_data: list[dict], result_queue: AbstractCsmoneyWriter) -> None:
    pack = CsmoneyItemPack()
    for json_item in items_data:
        items = _create_items(json_item)
        for item in items:
            pack.items.append(item)
    await result_queue.put(pack)


class CsmoneyParserImpl(AbstractCsmoneyParser):
    def __init__(self, limiter: AsyncSessionConcurrentLimiter):
        self._limiter = limiter

    async def parse(
        self, url: str, result_queue: AbstractCsmoneyWriter, max_attempts: int = _MAX_ATTEMPTS_DEFAULT
    ) -> None:
        failed_attempts = 0
        while failed_attempts <= max_attempts:
            session = await self._limiter.get_available(_POSTPONE_DURATION)
            html = await _request(session, url)
            if not html:
                logger.info(
                    "Failed to load cs.money page",
                    extra={"attempt": failed_attempts, "url": url},
                )
                failed_attempts += 1
                continue

            logger.info("Successfully got a response for %s", url)

            try:
                next_data = _extract_next_data(html)
                skins_data = _extract_skins(next_data)
            except ValueError as exc:
                logger.exception("Failed to parse cs.money page", exc_info=exc)
                failed_attempts += 1
                continue

            await _process_items(skins_data, result_queue)
            break

        if failed_attempts > max_attempts:
            raise MaxAttemptsReachedError()
