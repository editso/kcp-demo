import impl
import asyncio
import aioudp


async def main():
    local = await aioudp.open_remote_endpoint("127.0.0.1", port=8888)
    async with impl.KcpConnector(local) as connector:

        stream = await connector.open()

        data = await stream.write(b"hello 1111111111111111111")

        while True:
            data = await stream.read()
            print(data)
            await stream.write(b"hello 1111111111111111111")


if __name__ == "__main__":
    asyncio.run(main())
