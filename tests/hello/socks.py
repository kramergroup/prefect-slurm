"""
This is a patch for the websockets package to support proxy connections.

See: - https://github.com/racinette/websockets_proxy?tab=readme-ov-file
     - https://github.com/python-websockets/websockets/issues/364

"""

import os

import websockets
from websockets_proxy import Proxy, proxy_connect


class EnvProxyConnect(proxy_connect):
    def __init__(self, *args, **kwargs):
        proxy = Proxy.from_url(os.environ["https_proxy"])
        super().__init__(*args, proxy=proxy, **kwargs)


def monkey_patch_websockets():
    """
    Patches the connect method from websockets to enable proxy support
    """

    # if os.getenv("http_proxy") or os.getenv("https_proxy"):
    print("Patching websockets to support proxy connections")
    websockets.connect = EnvProxyConnect
