import json

import psycopg2


class DataLayer:
    def __init__(self, connection_string):
        self.db = psycopg2.connect(connection_string, cursor_factory=psycopg2.extras.RealDictCursor)

    def get_max_id(self, channel_id):
        with self.db.cursor() as cursor:
            cursor.execute("select max(id) from public.messages where chanel_id = %s", (channel_id,))
            return cursor.fetchone()["max"]

    def save_channel(self, channel_id, search_phrase, channel_info):
        with self.db.cursor() as cursor:
            cursor.execute(
                "insert into public.channels (id, search_phrase, name, channel_body) "
                "values (%s, %s, %s, %s) on conflict do nothing",
                (channel_id, search_phrase, channel_info.username, json.dumps(channel_info.to_dict()))
            )
        self.db.commit()

    def save_message(self, channel_id, message):
        with self.db.cursor() as cursor:
            cursor.execute(
                "insert into public.messages (id, chanel_id, date, message_text, message_object) "
                "values (%s, %s, %s, %s, %s)",
                (message.id, channel_id, message.date.strftime('%Y-%m-%d %H:%M:%S'),
                 message.text, json.dumps(message.to_dict()))
            )
        self.db.commit()

    def close(self):
        self.db.close()

    def get_search_phrases(self):
        with self.db.cursor() as cursor:
            cursor.execute("select distinct search_phrase from search_phrases where not is_disabled", )
            results = cursor.fetchall()
        return [result["search_phrase"] for result in results]

    def get_disabled_channels(self):
        with self.db.cursor() as cursor:
            cursor.execute("select distinct id from channels where is_disabled", )
            results = cursor.fetchall()
        return [result["id"] for result in results]