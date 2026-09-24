from unittest.mock import MagicMock, patch
import json
from pathlib import Path
from urllib.request import urlopen
from urllib.error import HTTPError
import pytest
from observa_probe.__main__ import main, sink
from observa_probe.health import start_server
from observa_probe.contract import validate_payload, sanitize_headers
from observa_probe.telemetry import configure
from test_runtime import message


def test_shared_fixture():
    fixture = json.loads(Path('/fixtures/contracts.json').read_text())
    assert validate_payload(fixture['valid_payload']) == fixture['valid_payload']
    assert sanitize_headers([('traceparent', fixture['traceparent']), ('tracestate', fixture['tracestate'])])[1] is None


@pytest.mark.parametrize('mode', ['entry', 'relay', 'sink'])
def test_main_dispatch(mode):
    with patch('observa_probe.__main__.configure') as config, patch('observa_probe.__main__.entry') as entry, patch('observa_probe.__main__.relay') as relay, patch('observa_probe.__main__.sink') as consume, patch('observa_probe.__main__.Producer'):
        main([mode, '--order-id', '3e7723b4-cdb5-4ee0-aea5-c6fd4ca63bc0'])
        {'entry': entry, 'relay': relay, 'sink': consume}[mode].assert_called_once()
        config.return_value.shutdown.assert_called_once()


def test_failure_flushes():
    with patch('observa_probe.__main__.configure') as config, patch('observa_probe.__main__.entry', side_effect=ValueError('invalid')):
        with pytest.raises(ValueError):
            main(['entry'])
        config.return_value.force_flush.assert_called_once()


def test_sink_loop_closes():
    client, tracer, state = MagicMock(), MagicMock(), {'value': False}
    msg = message()
    msg.error.return_value = None
    def poll(_):
        state['value'] = True
        return msg
    client.poll.side_effect = poll
    with patch('observa_probe.__main__.start_server'), patch('observa_probe.__main__.consume_one') as consume:
        sink(tracer, client, state)
        consume.assert_called_once_with(client, msg, tracer)
        client.close.assert_called_once()
        client.subscribe.call_args.kwargs['on_assign'](client, [])
        client.subscribe.call_args.kwargs['on_revoke'](client, [])


def test_sink_error_closes():
    client = MagicMock()
    client.poll.return_value.error.return_value = 'error'
    with patch('observa_probe.__main__.start_server'), pytest.raises(RuntimeError):
        sink(MagicMock(), client, {'value': False})
    client.close.assert_called_once()


def test_configure():
    with patch('observa_probe.telemetry.OTLPSpanExporter'), patch('observa_probe.telemetry.BatchSpanProcessor'):
        provider = configure('test')
        assert provider.resource.attributes['service.name'] == 'test'
        provider.shutdown()


def test_health_is_not_metrics():
    state = {'ready': False}
    server = start_server(state, 0)
    base = f'http://127.0.0.1:{server.server_port}'
    try:
        with pytest.raises(HTTPError) as error:
            urlopen(base + '/health')
        assert error.value.code == 503
        state['ready'] = True
        assert urlopen(base + '/health').read() == b'ready'
        assert b'observa_probe_processed_total' in urlopen(base + '/metrics').read()
        with pytest.raises(HTTPError):
            urlopen(base + '/bad')
    finally:
        server.shutdown()
        server.server_close()
