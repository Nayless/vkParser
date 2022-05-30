import json
import vk_api
from datetime import datetime
import threading
import psycopg2
from psycopg2 import Error


def main():
    global tokens
    padding = 0
    for key in tokens:
        session = vk_api.VkApi(token=key)
        vk = session.get_api()
        try:
            threading.Thread(target=get_groups, args=(groups_q // len(tokens), session, padding,)).start()
        except vk_api.exceptions.ApiError[29]:
            print("VK_API: Rate limit reached")
        padding += 1


def get_groups(quantity, session, offset):  # get groups list and format it to dict {id:screen_name)
    res = []
    global members_q
    for i in range(quantity // 500):
        ids = [str(j) for j in range((i + offset) * 500, ((i + offset) + 1) * 500)]
        res = session.method('groups.getById', {'group_ids': ','.join(ids), 'fields': ['id', 'members_count']})
        for group in res:
            if 'members_count' in group.keys() and group['members_count'] > members_q and group['is_closed'] == 0 and \
                    session.method('wall.get', {'owner_id': '-' + str(group['id'])})['count'] > 0:
                get_necessary_posts(group['id'], session)
    return


# use it to get post by your params
def get_necessary_posts(group_id, session):
    global date_bounds
    global params

    messages_quantity = session.method('wall.get', {'owner_id': '-' + str(group_id)})['count']
    for offset in range(0, messages_quantity, 20):
        posts = session.method('wall.get', {'owner_id': '-' + str(group_id), 'offset': offset})['items']
        for post in posts:
            if not analyze(post):
                return
    return


def analyze(post):
    global params
    text = post['text'].lower()
    for param in params.keys():
        if date_bounds[0] < datetime.fromtimestamp(post['date']):
            if date_bounds[1] > datetime.fromtimestamp(post['date']):
                if type(param) is str:
                    if r'' + param.lower() in text:
                        if params[param]['first_in'] > datetime.fromtimestamp(post['date']):
                            params[param]['first_in'] = datetime.fromtimestamp(post['date'])
                        if params[param]['last_in'] < datetime.fromtimestamp(post['date']):
                            params[param]['last_in'] = datetime.fromtimestamp(post['date'])
                        params[param]['all'] += 1
                else:
                    for i in range(len(text)):
                        if r'' + param[0] == text[i] and r'' + param[2] in text[i:i + param[1] + 1]:
                            if params[param]['first_in'] > datetime.fromtimestamp(post['date']):
                                params[param]['first_in'] = datetime.fromtimestamp(post['date'])
                            if params[param]['last_in'] < datetime.fromtimestamp(post['date']):
                                params[param]['last_in'] = datetime.fromtimestamp(post['date'])
                            params[param]['all'] += 1

        else:
            return False

    return True


def create_db():
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="root",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="greendata_db")
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    try:
        cursor = connection.cursor()

        create_table_query = '''CREATE TABLE vk_parser
                              (KEY TEXT     NOT NULL,
                              FIRST_IN           timestamp ,
                              LAST_IN            timestamp ,
                              ALL_IN     INTEGER
                              ); '''
        cursor.execute(create_table_query)
    except:
        pass

    try:
        insert_query = """ INSERT INTO vk_parser (KEY, FIRST_IN, LAST_IN, ALL_IN)
                                      VALUES (%s, %s, %s, %s)"""
        for k, v in params.items():
            items = (str(k), v['first_in'], v['last_in'], v['all'])
            cursor.execute(insert_query, items)
        connection.commit()
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")


with open('data.json', 'r') as data_read:
    data = json.load(data_read)
    date_bounds = [datetime.strptime(data['date_bounds'][0], '%d/%m/%Y %H:%M:%S'),
                   datetime.strptime(data['date_bounds'][1], '%d/%m/%Y %H:%M:%S')]

    params_list = data['params']
    tokens = data['access_keys']
    members_q = data['members_quantity']
    groups_q = data['groups_quantity']

    params = {}
    for par in data['params']:
        if type(par) is str:
            params[par] = {
                "first_in": datetime.strptime('27/03/2000 22:07:55', '%d/%m/%Y %H:%M:%S'),
                "last_in": datetime.now(),
                "all": 0
            }
        else:
            params[tuple(par)] = {
                "first_in": datetime.strptime('27/03/2000 22:07:55', '%d/%m/%Y %H:%M:%S'),
                "last_in": datetime.now(),
                "all": 0
            }

if __name__ == '__main__':
    main()
    create_db()
