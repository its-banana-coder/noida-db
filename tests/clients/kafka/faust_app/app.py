import faust
import asyncio
import os
import sys

# Get broker URL from environment or default to localhost
broker_url = os.environ.get('FAUST_BROKER_URL', 'kafka://localhost:9092')
app = faust.App('hello-app', broker=broker_url)

class Greeting(faust.Record):
    message: str

topic = app.topic('hello-topic', value_type=Greeting, partitions=4)

@app.agent(topic)
async def hello(greetings):
    async for greeting in greetings:
        print(f'[PID {os.getpid()}] Received greeting: {greeting.message}')
        await asyncio.sleep(0.1)


if __name__ == '__main__':
    app.main()
