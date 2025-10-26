import datetime
import json
import sys
import types
from unittest.mock import AsyncMock

import aiohttp
import pytest
import pytest_asyncio
from aiohttp import ClientSession
from aioresponses import aioresponses

dummy_aioredis = types.ModuleType("aioredis")
dummy_aioredis.Redis = object
sys.modules.setdefault("aioredis", dummy_aioredis)

from price_monitoring.models.csmoney import (
    CsmoneyItem,
    CsmoneyItemCategory,
    CsmoneyItemPack,
)
from price_monitoring.parsers.csmoney.parser.abstract_parser import MaxAttemptsReachedError
from price_monitoring.parsers.csmoney.parser.parser import (
    CsmoneyParserImpl,
    _create_items,
    _csmoney_unix_to_datetime,
    _is_cloudflare_challenge,
)


def _build_html(items: list[dict]) -> str:
    data = {"props": {"pageProps": {"botInitData": {"skinsInfo": {"skins": items}}}}}
    return (
        "<!DOCTYPE html><html><head></head><body>"
        "<script id=\"__NEXT_DATA__\" type=\"application/json\">"
        f"{json.dumps(data)}"
        "</script></body></html>"
    )


@pytest_asyncio.fixture()
async def limiter_fixture():
    session = ClientSession()
    limiter = AsyncMock()
    limiter.get_available.return_value = session
    yield limiter
    await session.close()


@pytest.fixture()
def result_queue_fixture():
    return AsyncMock()


@pytest.fixture()
def parser_fixture(limiter_fixture):
    return CsmoneyParserImpl(limiter_fixture)


@pytest.fixture()
def response_fixture():
    with open("tests/parsers/csmoney/parser/data/item_1.json", encoding="utf8") as f:
        return json.load(f)


@pytest.fixture()
def csmoney_item_fixture():
    return CsmoneyItem(
        name="★ Butterfly Knife | Doppler (Factory New)",
        price=24768.93,
        asset_id="24898849555",
        name_id=3985,
        type_=CsmoneyItemCategory.KNIFE,
        float_="0.008115612901747",
        unlock_timestamp=datetime.datetime.fromtimestamp(1645430400, datetime.UTC),
        overpay_float=140.69,
    )


@pytest.fixture()
def csmoney_item_pack_fixture(csmoney_item_fixture):
    return CsmoneyItemPack(items=[csmoney_item_fixture, csmoney_item_fixture])


def test_csmoney_unix_to_datetime():
    assert _csmoney_unix_to_datetime(1645009200000) == datetime.datetime.fromtimestamp(
        1645009200, datetime.UTC
    )


def test_create_items_without_stack():
    with open("tests/parsers/csmoney/parser/data/item_1.json", encoding="utf8") as f:
        data = json.load(f)
        items = [
            CsmoneyItem(
                name="★ Butterfly Knife | Doppler (Factory New)",
                price=24768.93,
                asset_id="24898849555",
                name_id=3985,
                type_=CsmoneyItemCategory.KNIFE,
                float_="0.008115612901747",
                unlock_timestamp=datetime.datetime.fromtimestamp(1645430400, datetime.UTC),
                overpay_float=140.69,
            )
        ]
        assert _create_items(data) == items


def test_create_items_without_full_name():
    with open(
        "tests/parsers/csmoney/parser/data/item_without_full_name_1.json",
        encoding="utf8",
    ) as f:
        data = json.load(f)
        assert _create_items(data) == []


def test_create_items_with_stack():
    with open("tests/parsers/csmoney/parser/data/item_with_stack_1.json", encoding="utf8") as f:
        data = json.load(f)
        items = [
            CsmoneyItem(
                name="★ Sport Gloves | Vice (Factory New)",
                price=35718.7,
                asset_id="24491496626",
                name_id=29570,
                type_=CsmoneyItemCategory.GLOVE,
                float_="0.065496817231178",
                unlock_timestamp=None,
                overpay_float=None,
            ),
            CsmoneyItem(
                name="★ Sport Gloves | Vice (Factory New)",
                price=35718.7,
                asset_id="24571330159",
                name_id=29570,
                type_=CsmoneyItemCategory.GLOVE,
                float_="0.067453943192958",
                unlock_timestamp=None,
                overpay_float=None,
            ),
        ]
        assert _create_items(data) == items


def test_create_items_with_stack_and_tradelock():
    with open("tests/parsers/csmoney/parser/data/item_with_stack_2.json", encoding="utf8") as f:
        data = json.load(f)
        items = [
            CsmoneyItem(
                name="★ M9 Bayonet | Doppler (Factory New)",
                price=11592.8,
                asset_id="24899230485",
                name_id=15840,
                type_=CsmoneyItemCategory.KNIFE,
                float_="0.056123819202184",
                unlock_timestamp=datetime.datetime.fromtimestamp(1645430400, datetime.UTC),
                overpay_float=None,
            ),
            CsmoneyItem(
                name="★ M9 Bayonet | Doppler (Factory New)",
                price=11592.8,
                asset_id="24902572721",
                name_id=15840,
                type_=CsmoneyItemCategory.KNIFE,
                float_="0.06806051731109601",
                unlock_timestamp=datetime.datetime.fromtimestamp(1645430400, datetime.UTC),
                overpay_float=None,
            ),
        ]
        assert _create_items(data) == items


def test_is_cloudflare_challenge():
    assert _is_cloudflare_challenge("<title>Just a moment...</title>")
    assert _is_cloudflare_challenge("CF-Mitigated"), "check is case insensitive"
    assert not _is_cloudflare_challenge("<html><body>ok</body></html>")


@pytest.mark.asyncio
async def test_parse__puts_items(
    parser_fixture,
    result_queue_fixture,
    response_fixture,
    csmoney_item_pack_fixture,
):
    html = _build_html([response_fixture, response_fixture])

    with aioresponses() as mocked:
        mocked.get(
            "https://cs.money/csgo/trade?minPrice=0.2&maxPrice=0.3",
            body=html,
        )

        await parser_fixture.parse(
            "https://cs.money/csgo/trade?minPrice=0.2&maxPrice=0.3",
            result_queue_fixture,
        )

    result_queue_fixture.put.assert_called_once_with(csmoney_item_pack_fixture)


@pytest.mark.asyncio
async def test_parse__retries_on_errors(
    parser_fixture, result_queue_fixture, response_fixture
):
    html = _build_html([response_fixture])

    with aioresponses() as mocked:
        mocked.get(
            "https://cs.money/csgo/trade?minPrice=0.2&maxPrice=0.3",
            exception=aiohttp.ClientConnectionError(),
        )
        mocked.get(
            "https://cs.money/csgo/trade?minPrice=0.2&maxPrice=0.3",
            body=html,
        )

        await parser_fixture.parse(
            "https://cs.money/csgo/trade?minPrice=0.2&maxPrice=0.3",
            result_queue_fixture,
        )

    assert result_queue_fixture.put.call_count == 1


@pytest.mark.asyncio
async def test_parse__raises_when_max_attempts_reached(
    parser_fixture, result_queue_fixture
):
    with aioresponses() as mocked:
        mocked.get(
            "https://cs.money/csgo/trade?minPrice=0.2&maxPrice=0.3",
            exception=aiohttp.ClientConnectionError(),
        )

        with pytest.raises(MaxAttemptsReachedError):
            await parser_fixture.parse(
                "https://cs.money/csgo/trade?minPrice=0.2&maxPrice=0.3",
                result_queue_fixture,
                max_attempts=0,
            )

    assert result_queue_fixture.put.call_count == 0


@pytest.mark.asyncio
async def test_parse__retries_when_cloudflare_blocks_request(
    parser_fixture, result_queue_fixture, response_fixture
):
    challenge_html = "<!DOCTYPE html><html><head><title>Just a moment...</title></head></html>"
    valid_html = _build_html([response_fixture])

    with aioresponses() as mocked:
        mocked.get(
            "https://cs.money/csgo/trade?minPrice=0.2&maxPrice=0.3",
            body=challenge_html,
        )
        mocked.get(
            "https://cs.money/csgo/trade?minPrice=0.2&maxPrice=0.3",
            body=valid_html,
        )

        await parser_fixture.parse(
            "https://cs.money/csgo/trade?minPrice=0.2&maxPrice=0.3",
            result_queue_fixture,
        )

    assert result_queue_fixture.put.call_count == 1


if __name__ == "__main__":
    pytest.main()
