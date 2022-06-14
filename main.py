import json
import vk_api
from datetime import datetime
import threading
import psycopg2
from psycopg2 import Error
import re
from flask import Flask, request

# create the Flask app
app = Flask(__name__)

date_bounds = []
keywords = []
params = {}
params_list = []
REQUEST_DATE = datetime.now()
sum_text_len = 0
valid_groups = 0
posts_quantity = 0
current_group = 0

with open("data.json", "r", encoding="utf-8") as data_read:
    data = json.load(data_read)
    tokens = data["access_keys"]
    members_q = data["members_quantity"]
    groups_q = data["groups_quantity"]
    groups_req = data["groups_per_request"]  # Количество получаемых групп одним запросом (max - 500)


@app.route("/vk", methods=["POST"])
def post_data():
    print(request)
    print(request.get_json())
    global date_bounds, params_list, params
    request_data = request.get_json()
    date_bounds = [datetime.strptime(request_data["time_bounds"][0], "%d/%m/%Y %H:%M:%S"),
                   datetime.strptime(request_data["time_bounds"][1], "%d/%m/%Y %H:%M:%S")]
    for par in request_data["keywords"]:
        if type(par) is str:
            params[par] = {
                "first_in": datetime.now(),
                "last_in": datetime.strptime("02/11/1000 22:07:55", "%d/%m/%Y %H:%M:%S"),
                "all": 0
            }
        else:
            params[tuple(par)] = {
                "first_in": datetime.now(),
                "last_in": datetime.strptime("02/11/1000 22:07:55", "%d/%m/%Y %H:%M:%S"),
                "all": 0
            }
    params_list = request_data["keywords"]
    main()
    return {"lines saved": len(params_list)}


def main():
    global tokens
    padding = 0
    print("main")
    for key in tokens:
        session = vk_api.VkApi(token=key)
        vk = session.get_api()
        try:
            threading.Thread(target=get_groups, args=(groups_q // len(tokens), session, padding,)).run()
        except:
            pass

        padding += 1
    while True:
        if current_group == groups_q:
            break
    create_db()



def get_groups(quantity, session, offset):  # get groups list and format it to dict {id:screen_name)
    global groups_req
    global members_q
    global valid_groups
    global current_group

    res = []

    for i in range(quantity // groups_req):
        ids = [str(j) for j in range((i + offset) * groups_req, ((i + offset) + 1) * groups_req)]
        res = session.method("groups.getById", {"group_ids": ",".join(ids), "fields": ["id", "members_count"]})
        for group in res:
            if "members_count" in group.keys() and group["members_count"] > members_q and group["is_closed"] == 0 and \
                    session.method("wall.get", {"owner_id": "-" + str(group["id"])})["count"] > 0:
                valid_groups += 1
                get_necessary_posts(group["id"], session)
            current_group = group["id"]+1
    return


# use it to get post by your params
def get_necessary_posts(group_id, session):
    global date_bounds
    global params
    global posts_quantity


    messages_quantity = session.method("wall.get", {"owner_id": "-" + str(group_id)})["count"]
    for offset in range(0, messages_quantity, 20):
        posts = session.method("wall.get", {"owner_id": "-" + str(group_id), "offset": offset})["items"]
        for post in posts:
            if date_bounds[0] < datetime.fromtimestamp(post["date"]):
                if date_bounds[1] > datetime.fromtimestamp(post["date"]):
                    posts_quantity += 1
                    analyze(post)
            else:

                return
    return


def analyze(post):
    global params
    global sum_text_len

    text = post["text"].lower()
    sum_text_len += len(text)
    temp_text = re.split("; |, | |: |. ", text)

    for param in params.keys():
        if type(param) is str:
            if r"" + param.lower() in text:
                if params[param]["first_in"] > datetime.fromtimestamp(post["date"]):
                    params[param]["first_in"] = datetime.fromtimestamp(post["date"])
                if params[param]["last_in"] < datetime.fromtimestamp(post["date"]):
                    params[param]["last_in"] = datetime.fromtimestamp(post["date"])
                params[param]["all"] += 1

        else:
            if param[1] < 0:
                eq = -1
            else:
                eq = 1
            for i in range(len(temp_text)):
                try:
                    if r"" + param[0] == temp_text[i] and r"" + param[2] in temp_text[i:i + param[1] + eq]:
                        if params[param]["first_in"] > datetime.fromtimestamp(post["date"]):
                            params[param]["first_in"] = datetime.fromtimestamp(post["date"])
                        if params[param]["last_in"] < datetime.fromtimestamp(post["date"]):
                            params[param]["last_in"] = datetime.fromtimestamp(post["date"])
                        params[param]["all"] += 1

                except:
                    pass
    return


def create_db():
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="root",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="greendata_db")
        cursor = connection.cursor()
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    try:
        cursor = connection.cursor()

        create_keys_table_query = """CREATE TABLE keys_info
                              (KEY TEXT     NOT NULL,
                              FIRST_IN           timestamp ,
                              LAST_IN            timestamp ,
                              ALL_IN     INTEGER
                              ); """
        cursor.execute(create_keys_table_query)
    except:
        connection.rollback()

    try:
        cursor = connection.cursor()
        create_request_table_query = """CREATE TABLE request_info
                                      (PARAMS       TEXT     NOT NULL,
                                      REQUEST_DATE           timestamp ,
                                      VALID_GROUPS     INTEGER,
                                      SUM_TEXT_LEN     INTEGER,
                                      POSTS_QUANTITY     INTEGER
                                      ); """

        cursor.execute(create_request_table_query)
    except:
        connection.rollback()
    try:
        cursor = connection.cursor()
        insert_keys_query = """ INSERT INTO keys_info (KEY, FIRST_IN, LAST_IN, ALL_IN)
                                      VALUES (%s, %s, %s, %s)"""
        for k, v in params.items():
            items = (str(k), v["first_in"], v["last_in"], v["all"])
            cursor.execute(insert_keys_query, items)
        connection.commit()
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
        connection.rollback()

    try:
        insert_request_query = """ INSERT INTO request_info 
        (PARAMS, REQUEST_DATE, VALID_GROUPS, SUM_TEXT_LEN, POSTS_QUANTITY)
                                      VALUES (%s, %s, %s, %s, %s)"""

        cursor.execute(insert_request_query,
                       (str(params_list), REQUEST_DATE, valid_groups, sum_text_len, posts_quantity))
        connection.commit()
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
        connection.rollback()
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")


if __name__ == "__main__":
    from waitress import serve
    serve(app, host="0.0.0.0", port=8080)