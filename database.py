"""数据库连接管理"""
import logging
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME

logger = logging.getLogger(__name__)

DATABASE_URL = f"mysql+aiomysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"

engine = create_async_engine(DATABASE_URL, echo=False, pool_size=20, max_overflow=10, pool_recycle=3600)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def init_db():
    """创建所有表，并为存量库补齐归档依赖的索引"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # 存量 rate_limit_events 表可能没有 created_at 索引（归档窗口扫描的关键索引）
        if engine.dialect.name == "mysql":
            try:
                exists = await conn.scalar(text(
                    "SELECT COUNT(1) FROM information_schema.STATISTICS "
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'rate_limit_events' "
                    "AND COLUMN_NAME = 'created_at'"
                ))
                if not exists:
                    await conn.execute(text("CREATE INDEX idx_events_created ON rate_limit_events (created_at)"))
                    logger.info("已创建 rate_limit_events.created_at 索引")
            except Exception as e:
                logger.warning(f"检查/创建 idx_events_created 失败: {e}")
    logger.info("数据库表初始化完成")


async def get_session() -> AsyncSession:
    async with async_session() as session:
        yield session
