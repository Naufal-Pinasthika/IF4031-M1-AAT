#!/usr/bin/env python3
"""Loopback-only bridge for a Windows load generator into the WSL VM network.

This is lab tooling, not the application load balancer. It uses an asynchronous
TCP relay instead of one process per connection. Stop with Ctrl+C after tests.
"""
import asyncio
import resource


async def relay(reader, writer):
    while chunk := await reader.read(65536):
        writer.write(chunk)
        await writer.drain()


async def accept(reader, writer, port):
    remote = None
    tasks = []
    try:
        upstream, remote = await asyncio.wait_for(asyncio.open_connection('192.168.93.10', port), 5)
        tasks = [asyncio.create_task(relay(reader, remote)), asyncio.create_task(relay(upstream, writer))]
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    except (OSError, asyncio.TimeoutError):
        pass
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for stream in (writer, remote):
            if stream:
                stream.close()


async def main():
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(100000, hard), hard))
    servers = []
    for port in [30080, 30083, 30084, 30086, 30090]:
        servers.append(await asyncio.start_server(lambda r,w,p=port:accept(r,w,p), '127.0.0.1', port, backlog=20000))
    print('Lab TCP relays ready on loopback ports 30080/30083/30084/30086/30090', flush=True)
    await asyncio.gather(*(server.serve_forever() for server in servers))


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
