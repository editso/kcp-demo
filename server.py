import impl
import asyncio
import aioudp
import logging


async def main():
    local = await aioudp.open_local_endpoint("0.0.0.0", port=8989)

    async with impl.KcpListener(local) as listener:
        while True:
            stream = await listener.accept()

            reader, writer = await asyncio.open_connection("127.0.0.1", 9999)

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


if __name__ == "__main__":
    asyncio.run(main())
