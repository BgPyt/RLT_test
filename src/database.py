from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from config import URL_MONGO


cluster: AsyncIOMotorClient = AsyncIOMotorClient(URL_MONGO)
sample_collection: AsyncIOMotorCollection = cluster.junior_test.sample_collection


