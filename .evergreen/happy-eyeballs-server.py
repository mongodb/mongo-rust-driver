import argparse
import asyncio
import socket
import sys

parser = argparse.ArgumentParser(
    prog='happy-eyeballs-server',
    description='Fake server for testing happy eyeballs',
)
parser.add_argument('-c', '--control', default=10036, type=int, metavar='PORT', help='control port')
parser.add_argument('-v4', '--ipv4', default=10037, type=int, metavar='PORT', help='IPv4 port')
parser.add_argument('-v6', '--ipv6', default=10038, type=int, metavar='PORT', help='IPv6 port')
args = parser.parse_args()

PREFIX='happy eyeballs server'

async def main():
    shutdown = asyncio.Event()
    srv = await asyncio.start_server(lambda reader, writer: on_control_connected(reader, writer, shutdown), 'localhost', args.control)
    print(f'{PREFIX}: listening for control connections on {args.control}', file=sys.stderr)
    async with srv:
        await shutdown.wait()
    print(f'{PREFIX}: all done', file=sys.stderr)

async def on_control_connected(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, shutdown: asyncio.Event):
    data = await reader.readexactly(1)
    if data == b'\x04':
        print(f'{PREFIX}: ========================', file=sys.stderr)
        print(f'{PREFIX}: request for delayed IPv4', file=sys.stderr)
    elif data == b'\x06':
        print(f'{PREFIX}: ========================', file=sys.stderr)
        print(f'{PREFIX}: request for delayed IPv6', file=sys.stderr)
    elif data == b'\xFF':
        print(f'{PREFIX}: shutting down', file=sys.stderr)
        writer.close()
        await writer.wait_closed()
        shutdown.set()
        return
    else:
        raise Exception(f'Unexpected control byte: {data}')
    
    connected = asyncio.Event()
    on_ipv4_connected = lambda reader, writer: on_connected('IPv4', writer, b'\x04', connected)
    on_ipv6_connected = lambda reader, writer: on_connected('IPv6', writer, b'\x06', connected)
    srv4 = await asyncio.start_server(on_ipv4_connected, 'localhost', args.ipv4, family=socket.AF_INET, start_serving=False)
    srv6 = await asyncio.start_server(on_ipv6_connected, 'localhost', args.ipv6, family=socket.AF_INET6, start_serving=False)
    print(f'{PREFIX}: open for IPv4 on {args.ipv4}', file=sys.stderr)
    print(f'{PREFIX}: open for IPv6 on {args.ipv6}', file=sys.stderr)

    writer.write(b'\x01')
    await writer.drain()
    writer.close()
    await writer.wait_closed()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(listen('IPv4', srv4, data == b'\x04', connected))
        tg.create_task(listen('IPv6', srv6, data == b'\x06', connected))

    srv4.close()
    srv6.close()
    async with asyncio.TaskGroup() as tg:
        tg.create_task(srv4.wait_closed())
        tg.create_task(srv6.wait_closed())
    
    print(f'{PREFIX}: connection complete, test ports closed', file=sys.stderr)
    print(f'{PREFIX}: ========================', file=sys.stderr)

async def listen(name: str, srv: asyncio.Server, delay: bool, connected: asyncio.Event):
    if delay:
        print(f'{PREFIX}: delaying {name} connections', file=sys.stderr)
        await asyncio.sleep(1.0)
    print(f'{PREFIX}: accepting {name} connections', file=sys.stderr)
    async with srv:
        await srv.start_serving()
        await connected.wait()

async def on_connected(name: str, writer: asyncio.StreamWriter, payload: bytes, connected: asyncio.Event):
    print(f'{PREFIX}: connected on {name}')
    writer.write(payload)
    await writer.drain()
    writer.close()
    await writer.wait_closed()
    connected.set()

asyncio.run(main())
