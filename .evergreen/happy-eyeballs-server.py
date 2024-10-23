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
    srv = await asyncio.start_server(on_control_connected, 'localhost', args.control)
    print(f'{PREFIX}: listening for control connections on {args.control}', file=sys.stderr)
    async with srv:
        await srv.serve_forever()

async def on_control_connected(reader, writer):
    print(f'{PREFIX}: accepted control connection', file=sys.stderr)
    data = await reader.readexactly(1)
    if data != b'\x04' and data != b'\x06':
        raise Exception(f'Expecting 4 or 6 for control byte, got {data}')
    srv4 = await asyncio.start_server(on_ipv4_connected, 'localhost', args.ipv4, family=socket.AF_INET, start_serving=False)
    srv6 = await asyncio.start_server(on_ipv6_connected, 'localhost', args.ipv6, family=socket.AF_INET6, start_serving=False)
    writer.write(b'\x01')
    await writer.drain()
    async with asyncio.TaskGroup() as tg:
        tg.create_task(listen_4(srv4, data == b'\x04'))
        tg.create_task(listen_6(srv6, data == b'\x06'))

async def listen_4(srv, delay):
    if delay:
        print(f'{PREFIX}: delaying IPv4 binding', file=sys.stderr)
        await asyncio.sleep(1.0)
    print(f'{PREFIX}: listening for IPv4 connections on {args.ipv4}', file=sys.stderr)
    async with srv:
        await srv.serve_forever()

async def listen_6(srv, delay):
    if delay:
        print(f'{PREFIX}: delaying IPv6 binding', file=sys.stderr)
        await asyncio.sleep(1.0)
    print(f'{PREFIX}: listening for IPv6 connections on {args.ipv6}', file=sys.stderr)
    async with srv:
        await srv.serve_forever()

async def on_ipv4_connected(reader, writer):
    writer.write(b'\x04')
    await writer.drain()
    writer.close()
    await writer.wait_closed()

async def on_ipv6_connected(reader, writer):
    writer.write(b'\x06')
    await writer.drain()
    writer.close()
    await writer.wait_closed()

asyncio.run(main())
