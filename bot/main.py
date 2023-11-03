import json
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from pydantic import ValidationError
from bot.message_error import MESSAGE_REQUEST_EXAMPLE, MESSAGE_VALID_REQUESTS
from config import TOKEN
import asyncio
from src.database import sample_collection
from src.main import AggregationPipeline


bot = Bot(token=TOKEN)
dp = Dispatcher()


@dp.message(Command('start'))
async def start(msg: types.Message):
    await msg.reply(f'Hi <a href="{msg.from_user.url}">{msg.from_user.first_name}</a>!',
                    parse_mode="html",
                    reply_markup=types.force_reply.ForceReply())


@dp.message()
async def get_response_aggregation(msg: types.Message):
    try:
        dict_message = json.loads(msg.text)
    except json.decoder.JSONDecodeError:
        return await msg.answer(MESSAGE_REQUEST_EXAMPLE)
    try:
        response_message = await AggregationPipeline(**dict_message, collection=sample_collection).result_aggregation()
    except ValidationError:
        return await msg.answer(MESSAGE_VALID_REQUESTS)
    await msg.answer(json.dumps(response_message))


async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())

