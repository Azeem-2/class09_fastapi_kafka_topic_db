from fastapi import FastAPI
from sqlmodel import SQLModel, Field, create_engine, Session
from db_service import setting
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaConsumer
import json
import asyncio


class Todo (SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    content: str = Field(index=True, min_length=3, max_length=54)
    is_completed: bool = Field(default=False)


connection_string: str = str(setting.DATABASE_URL).replace(
    "postgresql", "postgresql+psycopg")
engine = create_engine(connection_string, connect_args={},
                       pool_recycle=300, pool_size=10)


@asynccontextmanager
async def lifespan(app: FastAPI):
    print('Creating Tables')
    create_tables()
    print("Tables Created")
    loop = asyncio.get_event_loop()
    task = loop.create_task(consume_orders())
    yield
    task.cancel()
    await task


app: FastAPI = FastAPI(
    lifespan=lifespan, title="PostgreSQL DB service", version='1.0.0')


def create_tables() -> None:
    SQLModel.metadata.create_all(engine)


def get_session():
    with Session(engine) as session:
        yield session


async def consume_orders():
    consumer = AIOKafkaConsumer(
        setting.KAFKA_ORDER_TOPIC,
        bootstrap_servers=setting.BOOTSTRAP_SERVER,
        group_id=setting.KAFKA_CONSUMER_GROUP_ID
    )

    await consumer.start()
    print("consumer started....")
    try:
        async for msg in consumer:
            if msg.value is not None:
                try:
                    todo_data = json.loads(msg.value)
                    todo = Todo(
                        content=todo_data['content'],
                        # is_completed=todo_data['is_completed']
                    )

                    with Session(engine) as session:
                        session.add(todo)
                        session.commit()
                        session.refresh(todo)

                except json.JSONDecodeError as e:
                    print(f"Failed to decode JSON message: {e}")
                except KeyError as e:
                    print(f"Missing expected key in message: {e}")
            else:
                print("Received message with no value")
    finally:
        await consumer.stop()

