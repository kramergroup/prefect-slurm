import asyncio

import asyncssh


async def test():
    async with asyncssh.connect(
        host="hsuper-login.hsu-hh.de",
        options=asyncssh.SSHClientConnectionOptions(
            username="kramerd",
            password=None,
            known_hosts=None,
        ),
    ) as c:
        await c.run("ls")


asyncio.run(test())
