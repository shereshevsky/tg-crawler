import os

from telethon import TelegramClient, functions
from telethon.errors import SessionPasswordNeededError


class Crawler:
    def __init__(self, api_id, api_hash, phone, username, media_folder):
        self.client = TelegramClient(username, api_id, api_hash)
        self.phone = phone
        self.media_folder = media_folder

    async def start(self):
        await self.client.start()
        if not await self.client.is_user_authorized():
            await self.client.send_code_request(self.phone)
            try:
                await self.client.sign_in(self.phone, input('Enter the code: '))
            except SessionPasswordNeededError:
                await self.client.sign_in(password=input('Password: '))

    async def search_channels(self, search_phrase):
        result = await self.client(functions.contacts.SearchRequest(q=search_phrase, limit=1_000))
        return result.chats

    async def fetch_messages(self, channel_id, max_id=None):
        messages_iter = self.client.iter_messages(
            channel_id, min_id=max_id, limit=1_000
        ) if max_id else self.client.iter_messages(channel_id, limit=1_000)
        return [message async for message in messages_iter if message and message.id > max_id]

    async def download_media(self, channel_id, message):
        full_path = f"{self.media_folder}/{channel_id}_{message.id}/"
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        if message.media:
            await self.client.download_media(message=message, file=full_path)

    async def close(self):
        await self.client.disconnect()

    async def send_message_to_group(self, group_id, message):
        """Send a message to a specific group."""
        await self.client.send_message(group_id, message)
