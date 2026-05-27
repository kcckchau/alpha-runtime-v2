from __future__ import annotations

import errno
from unittest.mock import patch

import click
import pytest

from alpha.main import _reserve_api_socket


def test_reserve_api_socket_binds_and_closes() -> None:
    sock = _reserve_api_socket("127.0.0.1", 0)
    try:
        assert sock.fileno() != -1
        assert sock.getsockname()[1] > 0
    finally:
        sock.close()


def test_reserve_api_socket_raises_click_exception_when_port_in_use() -> None:
    err = OSError(errno.EADDRINUSE, "Address already in use")
    with patch("alpha.main.socket.create_server", side_effect=err):
        with pytest.raises(click.ClickException, match="API port 8000 on 0.0.0.0 is unavailable"):
            _reserve_api_socket("0.0.0.0", 8000)
