import impl
import asyncio
import aioudp


async def main():
    local = await aioudp.open_local_endpoint("127.0.0.1", port=8888)

    async with impl.KcpListener(local) as listener:
        stream = await listener.accept()

        while True:
            data = await stream.read()
            print(data)
            await stream.write(data)


if __name__ == "__main__":
    asyncio.run(main())
