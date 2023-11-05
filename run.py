#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import loguru
import asyncio
import configparser

from crawler import Crawler
from data_layer import DataLayer


async def main():

    logger = loguru.logger
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
    logger.debug(f"Disabled channels: {disabled_channels}")

    for search_phrase in data_layer.get_search_phrases():
        channels = await crawler.search_channels(search_phrase)

        for channel in channels:
            if channel.id in disabled_channels:
                continue

            data_layer.save_channel(channel.id, search_phrase, channel)
            max_id = current_max_id = data_layer.get_last_id(channel.id)
            logger.debug(f"Max id for channel {channel.id} is {max_id}")

            cnt = 0
            for message in await crawler.fetch_messages(channel.id, max_id):
                cnt += 1
                # await crawler.download_media(channel.id, message)
                data_layer.save_message(channel.id, message)
                current_max_id = max(current_max_id, message.id)

            logger.debug(f"Saved {cnt} messages for channel {channel.id}")
            if cnt > 0 and current_max_id > max_id:
                data_layer.set_last_id(channel.id, current_max_id)

    await crawler.close()
    data_layer.close()


if __name__ == '__main__':
    asyncio.run(main())
