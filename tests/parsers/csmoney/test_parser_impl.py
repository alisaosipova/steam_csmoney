import json
from unittest.mock import AsyncMock

import pytest

from price_monitoring.parsers.csmoney.parser import parser as parser_module
from price_monitoring.parsers.csmoney.parser.parser import CsmoneyParserImpl


@pytest.fixture()
def limiter():
    limiter = AsyncMock()
    limiter.get_available = AsyncMock()
    limiter.get_available.return_value = AsyncMock()
    return limiter


def _build_payload():
    return {
        "props": {
            "pageProps": {
                "botInitData": {
                    "skinsInfo": {
                        "skins": [
                            {
                                "fullName": "AK-47 | Redline (Field-Tested)",
                                "price": 12.34,
                                "assetId": 123,
                                "nameId": 456,
                                "type": 3,
                            }
                        ]
                    }
                }
            }
        }
    }


def test_extract_next_data_from_dict():
    payload = _build_payload()

    assert parser_module._extract_next_data(payload) is payload


def test_extract_next_data_from_html():
    payload = _build_payload()
    html = (
        '<html><script id="__NEXT_DATA__" type="application/json">'
        f"{json.dumps(payload)}"
        "</script></html>"
    )

    assert parser_module._extract_next_data(html) == payload


@pytest.mark.asyncio()
async def test_parse_handles_json_payload(monkeypatch, limiter):
    payload = _build_payload()
    request_mock = AsyncMock(return_value=payload)
    process_mock = AsyncMock()
    result_queue = AsyncMock()

    monkeypatch.setattr(parser_module, "_request", request_mock)
    monkeypatch.setattr(parser_module, "_process_items", process_mock)

    parser = CsmoneyParserImpl(limiter)

    await parser.parse("https://example.com", result_queue, max_attempts=1)

    process_mock.assert_awaited_once()
    await_call = process_mock.await_args
    assert (
        await_call.args[0]
        == payload["props"]["pageProps"]["botInitData"]["skinsInfo"]["skins"]
    )
    assert await_call.args[1] is result_queue
