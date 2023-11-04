import asyncio
import configparser

from crawler import Crawler
from data_layer import DataLayer


async def main():
    config = configparser.ConfigParser()
    config.read("config.ini")

    api_id = int(config["Telegram"]["API_ID"])
    api_hash = config["Telegram"]["API_HASH"]
    phone = config["Telegram"]["PHONE"]
    username = config["Telegram"]["USERNAME"]
    conn_string = config["DB"]["CONN_STRING"]
    media_folder = config["FS"]["MEDIA_PATH"]

    crawler = Crawler(api_id, api_hash, phone, username, media_folder)
    data_layer = DataLayer(conn_string)

    await crawler.start()

    disabled_channels = data_layer.get_disabled_channels()

    for search_phrase in data_layer.get_search_phrases():
        channels = await crawler.search_channels(search_phrase)

        for channel in channels:
            if channel.id in disabled_channels:
                continue
            max_id = data_layer.get_max_id(channel.id)
            data_layer.save_channel(channel.id, search_phrase, channel)

            for message in await crawler.fetch_messages(channel.id, max_id):
                await crawler.download_media(channel.id, message)
                data_layer.save_message(channel.id, message)

    await crawler.close()
    data_layer.close()


if __name__ == '__main__':
    asyncio.run(main())
