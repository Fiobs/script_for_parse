import copy
import sqlite3
import requests
import time
from pprint import pprint
from sqlite3 import Error
from bs4 import BeautifulSoup
from fake_useragent import UserAgent


class Sql:
    def __init__(self, db_name):
        try:
            self.connection = sqlite3.connect(db_name)
            print("Connection to SQLite DB successful")
            self.cursor = self.connection.cursor()
            self.__create_tables()
        except Error as e:
            print(f"The error '{e}' occurred")
            exit()

    def __create_tables(self):
        self.execute_query(query=f'''CREATE TABLE IF NOT EXISTS posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        author TEXT NOT NULL,
        title TEXT NOT NULL,
        description TEXT NOT NULL DEFAULT '',
        datetime DATE);
        ''', message='posts table create successfully')

        self.execute_query(query=f'''CREATE TABLE IF NOT EXISTS releases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        link TEXT NOT NULL,
        title TEXT NOT NULL,
        description TEXT NOT NULL DEFAULT '',
        datetime DATE,
        post_id INTEGER NOT NULL,
        FOREIGN KEY (post_id) REFERENCES posts (id));
        ''', message='releases table create successfully')

        self.execute_query(query=f'''CREATE TABLE IF NOT EXISTS pep (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        link TEXT NOT NULL,
        release_id INTEGER NOT NULL,
        FOREIGN KEY (release_id) REFERENCES releases (id));
        ''', message='pep table create successfully')

        self.execute_query(query=f'''CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        version TEXT NOT NULL,
        version_link TEXT NOT NULL,
        operation_system TEXT NOT NULL,
        description TEXT NOT NULL DEFAULT '',
        md5_sum TEXT NOT NULL,
        file_size INTEGER NOT NULL,
        gpg INTEGER NOT NULL,
        release_id INTEGER NOT NULL,
        FOREIGN KEY (release_id) REFERENCES releases (id));
        ''', message='files table create successfully')

        print('*' * 30, '\nTABLES CREATE SUCCESSFULLY\n', '*' * 30)

    def insert(self, table, **kwargs):
        keys = ["%s" % k for k in kwargs]
        values = ["'%s'" % v for v in kwargs.values()]
        sql = list()
        sql.append("INSERT INTO %s (" % table)
        sql.append(", ".join(keys))
        sql.append(") VALUES (")
        if 'description' in keys:
            sql.append(", ".join(['?' for i in range(0, len(keys))]))
            sql.append(")")
            return "".join(sql), values
        sql.append(", ".join(values))
        sql.append(");")
        return "".join(sql)

    def execute_query(self, query, data=None, message=None):
        try:
            self.cursor.execute(query) if not data else self.cursor.execute(query, (*data,))
            self.connection.commit()
            print(message) if message else None
            if 'INSERT' in query:
                return self.cursor.lastrowid
        except Error as e:
            raise Error(f"The error '{e}' occurred, query = {query}")


def start():
    sql = Sql('pythorg_data.db')
    user_agent = UserAgent()
    url = 'https://blog.python.org/'

    print('GET THE POSTS\n\n')
    page = requests.get(url=url, headers={'user-agent': f'{user_agent.random}'})
    entries = BeautifulSoup(page.text, 'html.parser').find_all('div', class_='date-outer')
    count = 1
    while True:
        count += 1
        if count % 5 == 0:
            time.sleep(2)

        page = requests.get(url=f'{url}search?updated-max={entries[len(entries) - 1].find("abbr", class_="published").get("title")}&max-results=7',
                            headers={'user-agent': f'{user_agent.random}'})
        is_entry = BeautifulSoup(page.text, 'html.parser').find_all('div', class_='date-outer')
        if len(is_entry) == 0:
            break
        entries.extend(is_entry)
        print(f"page№ {count}, posts = {len(entries)}")
        print('-----')

    print('Next step\n\n')

    post_not_parsed = {}
    link_not_parsed = {}
    for index, entry in enumerate(entries):
        if index+1 % 7 == 0:
            time.sleep(3)
        try:
            body = entry.find('div', class_='post-body entry-content')
            data = {
                'author': entry.find('div', class_='post-footer').find('span', class_='fn').text.strip(),
                'title': entry.find('h3', class_='post-title entry-title').text.strip(),
                'description': body.text,
                'datetime': entry.find('abbr', class_='published').get('title')[:19].replace('T', ' '),
            }
            query, p_data = sql.insert('posts', **data)
            post_id = sql.execute_query(query=query, data=p_data, message=f'post {data["datetime"]} attach successfully\n')
        except Exception as e:
            data = entry.find('abbr', class_='published').get('title')[:19].replace('T', ' ')
            print(f'SOMETHING WAS WRONG WITH POST {data}\n')
            post_not_parsed[data] = e
            print(e)
            continue

        links = [link.get('href') for link in body.find_all('a') if
                 link.get('href') and 'release/py' in link.get('href')]
        for i, link in enumerate(links):
            try:
                if i+1 % 10 == 0:
                    time.sleep(3)
                page = BeautifulSoup(requests.get(url=link, headers={'user-agent': f'{user_agent.random}'}).text, 'html.parser')
                page = page.find('div', class_='content-wrapper')
                h1_title = page.find('h1', class_='page-title')
                files = page.find('table').find('tbody').find_all('tr')
                desc = page.find('article', class_='text')
                desc.find('table').clear()
                desc.find_all('header', class_="article-header")[1].clear()

                release_data = {
                    'link': link,
                    'title': h1_title.text.strip() if h1_title else '',
                    'description': desc.text,
                    'datetime': page.find('p').text.split(': ')[1].strip(),
                    'post_id': post_id
                }
                query, r_data = sql.insert('releases', **release_data)
                release_id = sql.execute_query(query=query, data=r_data, message=f'release {link} attach successfully\n')
            except Exception as e:
                print(f'SOMETHING WAS WRONG WITH LINK {link}')
                print(e)
                link_not_parsed[data['datetime']] = {link: e}
                continue

            pep_links = [a.get('href') for a in page.find_all('a') if a.get('href') and 'peps/p' in a.get('href')]
            try:
                for pep in pep_links:
                    pep_data = {
                        'link': pep,
                        'release_id': release_id,
                    }
                    query = sql.insert('pep', **pep_data)
                    sql.execute_query(query=query)
                print(f'ALL PEP LINKS FOR RELEASE {link} ATTACHED SUCCESSFULLY\n')
            except Error as e:
                print(f'SOMETHING WAS WRONG WITH PEP\n')
                print(e)

            try:
                for tr in files:
                    tr = tr.find_all('td')
                    files_data = {
                        'version': tr[0].text.strip(),
                        'version_link': tr[0].find('a').get('href'),
                        'operation_system': tr[1].text.strip(),
                        'description': tr[2].text.strip(),
                        'md5_sum': tr[3].text.strip(),
                        'file_size': tr[4].text.strip(),
                        'gpg': tr[5].find('a').get('href'),
                        'release_id': release_id,
                    }
                    query, f_data = sql.insert('files', **files_data)
                    sql.execute_query(query=query, data=f_data)
                    print(f'ALL FILES FOR RELEASE {link} ATTACHED SUCCESSFULLY\n')
            except Exception as e:
                print(f'SOMETHING WAS WRONG WITH PEP\n')
                print(e)

    pprint(post_not_parsed)
    pprint(link_not_parsed)


if __name__ == '__main__':
    start()
