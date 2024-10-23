import argparse
import asyncio
import socket

parser = argparse.ArgumentParser(
    prog='happy-eyeballs-client',
    description='client for testing the happy eyeballs test server',
)
parser.add_argument('-c', '--control', default=10036, type=int, metavar='PORT', help='control port')
parser.add_argument('-v4', '--ipv4', default=10037, type=int, metavar='PORT', help='IPv4 port')
parser.add_argument('-v6', '--ipv6', default=10038, type=int, metavar='PORT', help='IPv6 port')
parser.add_argument('-d', '--delay', default=4, type=int)
args = parser.parse_args()

async def main():
    print('connecting to control')
    control_r, control_w = await asyncio.open_connection('localhost', args.control)
    control_w.write(args.delay.to_bytes())
    await control_w.drain()
    data = await control_r.read(1)
    if data != b'\x01':
        raise Exception(f'Expected byte 1, got {data}')
    async with asyncio.TaskGroup() as tg:
        tg.create_task(connect('IPv4', args.ipv4, socket.AF_INET, b'\x04'))
        tg.create_task(connect('IPv6', args.ipv6, socket.AF_INET6, b'\x06'))

async def connect(name: str, port: int, family: socket.AddressFamily, payload: bytes):
    print(f'{name}: connecting')
    try:
        reader, writer = await asyncio.open_connection('localhost', port, family=family)
    except Exception as e:
        print(f'{name}: failed ({e})')
        return
    print(f'{name}: connected')
    data = await reader.readexactly(1)
    if data != payload:
        raise Exception(f'Expected {payload}, got {data}')
    writer.close()
    await writer.wait_closed()
    print(f'{name}: done')

asyncio.run(main())