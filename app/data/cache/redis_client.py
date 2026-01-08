import redis.asyncio as redis
from app.core.config import settings

class RedisClient:
    def __init__(self):
        self.client = None
        self._connect()
    
    def _connect(self):
        try:
            self.client = redis.from_url(
                settings.REDIS_URL,
                encoding="utf-8",
                decode_responses=True
            )
        except Exception as e:
            print(f"Redis connection error: {e}")
            self.client = None
    
    async def get(self, key: str):
        if not self.client:
            self._connect()
            if not self.client:
                return None
        
        try:
            return await self.client.get(key)
        except Exception as e:
            print(f"Redis get error: {e}")
            return None
    
    async def setex(self, key: str, time: int, value: str):
        if not self.client:
            self._connect()
            if not self.client:
                return
        
        try:
            await self.client.setex(key, time, value)
        except Exception as e:
            print(f"Redis setex error: {e}")
    
    async def exists(self, key: str) -> bool:
        if not self.client:
            self._connect()
            if not self.client:
                return False
        
        try:
            return await self.client.exists(key) > 0
        except Exception as e:
            print(f"Redis exists error: {e}")
            return False
    
    async def delete(self, key: str):
        if not self.client:
            self._connect()
            if not self.client:
                return
        
        try:
            await self.client.delete(key)
        except Exception as e:
            print(f"Redis delete error: {e}")
    
    async def close(self):
        if self.client:
            await self.client.close()

# Global redis client instance
redis_client = RedisClient()