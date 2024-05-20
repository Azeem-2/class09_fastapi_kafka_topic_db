from fastapi import FastAPI, BackgroundTasks
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from sqlmodel import SQLModel
import json
import aiosmtplib
from email.message import EmailMessage
from todo import setting  # Ensure you're importing the setting module


class Todo (SQLModel):
    content: str


async def create_topic():
    admin_client = AIOKafkaAdminClient(bootstrap_servers=setting.BOOTSTRAP_SERVER)
    await admin_client.start()
    topic_list = [NewTopic(name=setting.KAFKA_ORDER_TOPIC, num_partitions=2, replication_factor=1)]
    try:
        await admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{setting.KAFKA_ORDER_TOPIC}' created successfully")
    except Exception as e:
        print(f"Failed to create topic '{setting.KAFKA_ORDER_TOPIC}': {e}")
    finally:
        await admin_client.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Fastapi app started...")
    await create_topic()
    yield


app = FastAPI(
    lifespan=lifespan,
    title="FastAPI Producer Service...",
    version='1.0.0'
)


@app.get('/')
async def root():
    return {"message": "Welcome to the todo microservice"}


async def send_email(subject: str, content: str, to: str):
    message = EmailMessage()
    message["From"] = setting.EMAIL_SENDER
    message["To"] = to
    message["Subject"] = subject
    message.set_content(content)

    await aiosmtplib.send(
        message,
        hostname=setting.SMTP_HOST,
        port=setting.SMTP_PORT,
        username=setting.SMTP_USER,
        password=setting.SMTP_PASSWORD,
        use_tls=setting.SMTP_USE_TLS,
    )


@app.post('/create_todo')
async def create_todo(todo: Todo, background_tasks: BackgroundTasks):

    producer = AIOKafkaProducer(bootstrap_servers=setting.BOOTSTRAP_SERVER)
    serialized_todo = json.dumps(todo.__dict__).encode('utf-8')
  
    await producer.start()
    try:
        await producer.send_and_wait(setting.KAFKA_ORDER_TOPIC, serialized_todo)
        # Schedule the background task to send an email
        background_tasks.add_task(send_email, "New Todo Created", f"Todo content: {todo.content}", setting.EMAIL_RECIPIENT)
    finally:
        await producer.stop()
    
    return {"message": "Todo generated successfully"}
