import argparse
import asyncio
import socket
import sys

parser = argparse.ArgumentParser(
    prog='happy-eyeballs-server',
    description='Fake server for testing happy eyeballs',
)
parser.add_argument('-c', '--control', default=10036, type=int, metavar='PORT', help='control port')
parser.add_argument('--stop', action='store_true', help='stop a currently-running server')
args = parser.parse_args()

PREFIX='happy eyeballs server'

async def main():
    # Stop a running server
    if args.stop:
        control_r, control_w = await asyncio.open_connection('localhost', args.control)
        control_w.write(b'\xFF')
        await control_w.drain()
        control_w.close()
        await control_w.wait_closed()
        return
    
    # Start the control server
    shutdown = asyncio.Event()
    srv = await asyncio.start_server(lambda reader, writer: on_control_connected(reader, writer, shutdown), 'localhost', args.control)
    print(f'{PREFIX}: listening for control connections on {args.control}', file=sys.stderr)
    async with srv:
        await shutdown.wait()
    print(f'{PREFIX}: all done', file=sys.stderr)

async def on_control_connected(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, shutdown: asyncio.Event):
    # Read the control request byte
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
    
    # Create the test servers but do not yet start accepting connections
    connected = asyncio.Event()
    on_ipv4_connected = lambda reader, writer: on_connected('IPv4', writer, b'\x04', connected)
    on_ipv6_connected = lambda reader, writer: on_connected('IPv6', writer, b'\x06', connected)
    srv4 = await asyncio.start_server(on_ipv4_connected, 'localhost', family=socket.AF_INET, start_serving=False)
    srv6 = await asyncio.start_server(on_ipv6_connected, 'localhost', family=socket.AF_INET6, start_serving=False)
    ipv4_port = srv4.sockets[0].getsockname()[1]
    ipv6_port = srv6.sockets[0].getsockname()[1]
    print(f'{PREFIX}: open for IPv4 on {ipv4_port}', file=sys.stderr)
    print(f'{PREFIX}: open for IPv6 on {ipv6_port}', file=sys.stderr)

    # Reply to control request with success byte and test server ports
    writer.write(b'\x01')
    writer.write(ipv4_port.to_bytes(2))
    writer.write(ipv6_port.to_bytes(2))
    await writer.drain()
    writer.close()
    await writer.wait_closed()

    # Start test servers listening in parallel, one with a delay
    async with asyncio.TaskGroup() as tg:
        tg.create_task(listen('IPv4', srv4, data == b'\x04', connected))
        tg.create_task(listen('IPv6', srv6, data == b'\x06', connected))

    # Wait for the test servers to shut down
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
        # Terminate this test server when either test server has handled a request
        await connected.wait()

async def on_connected(name: str, writer: asyncio.StreamWriter, payload: bytes, connected: asyncio.Event):
    print(f'{PREFIX}: connected on {name}')
    writer.write(payload)
    await writer.drain()
    writer.close()
    await writer.wait_closed()
    connected.set()

asyncio.run(main())
