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
        tg.create_task(connect_4())
        tg.create_task(connect_6())

async def connect_4():
    print('ipv4: connecting')
    reader, writer = await asyncio.open_connection('localhost', args.ipv4, family=socket.AF_INET)
    print('ipv4: connected')
    data = await reader.readexactly(1)
    if data != b'\x04':
        raise Exception(f'Expected byte 4, got {data}')
    writer.close()
    await writer.wait_closed()
    print('ipv4: done')

async def connect_6():
    print('ipv6: connecting')
    reader, writer = await asyncio.open_connection('localhost', args.ipv6, family=socket.AF_INET6)
    print('ipv6: connected')
    data = await reader.readexactly(1)
    if data != b'\x06':
        raise Exception(f'Expected byte 6, got {data}')
    writer.close()
    await writer.wait_closed()
    print('ipv6: done')

asyncio.run(main())