import os
from dataclasses import dataclass
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class DBConfig:
    host: str = os.getenv("DB_HOST", "localhost")
    port: str = os.getenv("DB_PORT", "5432")
    name: str = os.getenv("DB_NAME", "market")
    user: str = os.getenv("DB_USER", "etl")
    password: str = os.getenv("DB_PASSWORD", "etl")

def get_engine():
    cfg = DBConfig()
    url = f"postgresql+psycopg2://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.name}"
    return create_engine(url, future=True)

def exec_sql(sql: str, params: dict | None = None):
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(text(sql), params or {})
