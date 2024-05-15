import impl
import asyncio
import aioudp
import logging


async def main():
    local = await aioudp.open_remote_endpoint("192.168.5.214", port=8989)

    async with impl.KcpConnector(local) as connector:

        async def do_handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            
            stream = await connector.open()

            async def copy_read():
                while True:
                    try:
                        data = await stream.read()
                        writer.write(data)
                        await writer.drain()
                    except Exception as e:
                        break

            async def copy_write():
                while True:
                    data = await reader.read(8192 * 20)
                    if not data:
                        break
                    await stream.write(data)

            futures = [asyncio.shield(copy_read()), asyncio.shield(copy_write())]

            await asyncio.wait(futures, return_when=asyncio.FIRST_COMPLETED)

            for fut in futures:
                if not fut.done():
                    fut.cancel()
    
        server = await asyncio.start_server(do_handle, "0.0.0.0", 8989)

        async with server:
            await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main(), debug=False)