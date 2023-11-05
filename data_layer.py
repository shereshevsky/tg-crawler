import json
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extras import Json


class DataLayer:
    def __init__(self, connection_string):
        self.db = psycopg2.connect(connection_string, cursor_factory=RealDictCursor)

    def get_max_id(self, channel_id):
        with self.db.cursor() as cursor:
            cursor.execute("select max(id) from messages where chanel_id = %s", (channel_id,))
            return cursor.fetchone()["max"]

    def save_channel(self, channel_id, search_phrase, channel_info):
        with self.db.cursor() as cursor:
            cursor.execute(
                "insert into channels (id, search_phrase, name, channel_body) "
                "values (%s, %s, %s, %s) on conflict do nothing ",
                (channel_id, search_phrase, channel_info.username, Json(json.loads(channel_info.to_json())))
            )
        self.db.commit()

    def save_message(self, channel_id, message):
        with self.db.cursor() as cursor:
            cursor.execute(
                "insert into public.messages (id, chanel_id, date, message_text, message_object) "
                "values (%s, %s, %s, %s, %s)",
                (message.id, channel_id, message.date.strftime('%Y-%m-%d %H:%M:%S'),
                 message.text, Json(json.loads(message.to_json())))
            )
        self.db.commit()

    def close(self):
        self.db.close()

    def get_search_phrases(self):
        with self.db.cursor() as cursor:
            cursor.execute("select distinct search_phrase from search_phrases "
                           "where not is_disabled and search_group in ('auto-discovered', 'seed')", )
            results = cursor.fetchall()
        return [result["search_phrase"] for result in results]

    def get_disabled_channels(self):
        with self.db.cursor() as cursor:
            cursor.execute("select distinct id from channels where is_disabled", )
            results = cursor.fetchall()
        return [result["id"] for result in results]

    def get_last_id(self, channel_id):
        with self.db.cursor() as cursor:
            cursor.execute("select last_id from channels where id = %s", (channel_id, ))
            res = cursor.fetchone()
        return res["last_id"] if res else 0

    def set_last_id(self, channel_id, message_id):
        with self.db.cursor() as cursor:
            cursor.execute("update channels set last_id = %s where id = %s", (message_id, channel_id, ))
        self.db.commit()
