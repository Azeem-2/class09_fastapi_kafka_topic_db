from starlette.config import Config
from starlette.datastructures import Secret


try:
    config = Config(".env")

except FileNotFoundError:
    config = Config()

BOOTSTRAP_SERVER = config("KAFKA_SERVER", cast=str)
KAFKA_ORDER_TOPIC = config("KAFKA_ORDER_TOPIC", cast=str)
KAFKA_CONSUMER_GROUP_ID = config("KAFKA_CONSUMER_GROUP_ID", cast=str)

SMTP_HOST = config("SMTP_HOST", cast=str)
SMTP_PORT = config("SMTP_PORT", cast=int)
SMTP_USER = config("SMTP_USER", cast=str)
SMTP_PASSWORD = config("SMTP_PASSWORD", cast=Secret)
SMTP_USE_TLS = config("SMTP_USE_TLS", cast=bool)
EMAIL_SENDER = config("EMAIL_SENDER", cast=str)
EMAIL_RECIPIENT = config("EMAIL_RECIPIENT", cast=str)
