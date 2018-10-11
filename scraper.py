import os
import sqlite3
import string
from datetime import datetime
from typing import Optional, Any, Dict, List

import requests
from lxml import html


class Attendance:
    """
    The attendance record of senators and members for each sitting day.
    The final data must be written to an SQLite database called "data.sqlite" in the current working directory,
    which has at least a table called "data".
    """

    rss_url = 'http://parlinfo.aph.gov.au/parlInfo/feeds/rss.w3p;adv=yes;orderBy=date-eFirst;page={page};query=Title%3Aattendance;resCount=Default'
    item_content_xpath = '//div[@id="documentContentPanel"]//text()'
    item_metadata_dt_xpath = '//div[@class="metadata"]//dt[@class="mdLabel"]'
    item_metadata_dd_xpath = '//div[@class="metadata"]//dd[@class="mdValue"]'
    allowed_chars = string.digits + string.ascii_letters + string.punctuation
    sqlite_db_file = 'data.sqlite'
    cache_chars = string.digits + string.ascii_letters
    local_cache_dir = 'cache'

    db_table_sitting_day = [
        'id', 'title', 'database', 'date', 'source', 'number', 'page', 'status', 'federation_chamber_main_committee',
        'system_id', 'house', 'clerk', 'retrieve_date']
    db_table_attendance = ['id', 'sitting_day_id', 'name', 'on_leave']

    def run(self):
        db_conn = None
        try:
            db_conn = self.get_sqlite_db()
            self.create_sqlite_tables(db_conn)
            page_index = 0
            while True:
                print('Processing links - page {}'.format(page_index + 1))
                link_url = self.build_rss_url(page_index)
                page_index += 1

                link_page = self.download_html(link_url)
                if link_page is None:
                    break

                item_urls = self.parse_rss_page(link_page)

                for item_url in item_urls:
                    content_page = self.download_html(item_url)
                    content_item = self.parse_content_page(item_url, content_page)

                    if not content_item or not content_item.get('content'):
                        continue

                    attendance_item = self.parse_attendance(content_item['content'])
                    if attendance_item is None:
                        continue

                    db_data = self.build_rows(content_item, attendance_item)
                    print('Processing {} - {}'.format(db_data['sitting_day']['title'], item_url))

                    sitting_day_id = self.sqlite_sitting_day_row(db_conn, db_data['sitting_day'])

                    self.sqlite_attendance_rows(db_conn, sitting_day_id, db_data['people'])

                    db_conn.commit()
                    a = 1
        finally:
            if db_conn:
                db_conn.close()

    # ------------- Parsing ------------------------------

    def parse_house(self, content) -> Optional[str]:
        # which house?
        is_senate = 'senate' in content.lower()
        is_reps = 'house of representatives' in content.lower()

        if is_reps and not is_senate:
            house = 'House of Representatives'
        elif not is_reps and is_senate:
            house = 'Senate'
        else:
            # raise ValueError('Could not determine house.')
            return None

        return house

    def parse_clerk(self, lines):
        # clerk name
        clerk = None
        for index, line in enumerate(lines):
            if 'Clerk of the House of Representatives' in line:
                clerk = lines[index - 1].strip()
                break
            elif 'Clerk of the Senate' in line:
                clerk = lines[index - 1].strip()
                break
            elif line == 'Clerk':
                clerk = lines[index - 1].strip()
                break
            elif line == 'Acting Clerk':
                clerk = lines[index - 1].strip()
                break

        if not clerk:
            # raise ValueError('Could not determine clerk')
            return None

        return clerk

    def parse_attendance(self, content) -> Optional[Dict[str, str]]:
        retrieved_date = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
        people = []

        lines = content.split('\n')

        house = self.parse_house(content)
        if not house:
            return None

        clerk = self.parse_clerk(lines)
        if not clerk:
            return None

        # Senator / member names and leave status
        all_attended = False

        # -> reps
        for line in lines:
            if 'All Members attended (at some time during the sitting) except ' in line:
                names_raw = line.replace('All Members attended (at some time during the sitting) except ', '')
                names_raw = names_raw.replace(' and ', ', ')
                names_raw = names_raw.replace('* ', '*, ')
                names_raw = names_raw.replace(' *', ', *')

                names_norm = names_raw.strip('.')
                names = [i.strip() for i in names_norm.split(',') if i.strip()]

                for name in names:
                    name = name.strip()
                    on_leave = name.startswith('*') or name.endswith('*')
                    name = name.strip('*')
                    people.append((self.normalise_string(name), on_leave))

            elif 'All Members attended (at some time during the sitting).' in line:
                all_attended = True

            elif 'Present, all senators except ' in line:
                names_raw = line.replace('Present, all senators except Senators ', '')
                names_raw = names_raw.replace('Present, all senators except Senator ', '')
                names_raw = names_raw.replace(' and ', ', ')
                names_raw = names_raw.replace(' (*on leave).', '')
                names_raw = names_raw.replace(' (on leave).', '')
                names_raw = names_raw.replace('* ', '*, ')
                names_raw = names_raw.replace(' *', ', *')

                names_norm = names_raw.strip('.')
                names = [i.strip() for i in names_norm.split(',') if i.strip()]

                for name in names:
                    name = name.strip()
                    on_leave = name.startswith('*') or name.endswith('*') or '(on leave)' in line
                    name = name.strip('*')
                    people.append((self.normalise_string(name), on_leave))

            elif 'Present, all senators.' in line:
                all_attended = True

        # -> senate

        if not people and not all_attended:
            raise ValueError('Could not extract names.')

        # result
        result = {
            'house': house,
            'clerk': clerk,
            'retrieved_date': retrieved_date,
            'people': people
        }

        return result

    def parse_content_page(self, url, tree) -> Dict[str, Any]:
        result = {
            'content': None,
            'url': None,
            'metadata': {}
        }

        # get the content
        content_raw = [str(l).replace('\xa0', ' ') for l in tree.xpath(self.item_content_xpath) if l]
        if not content_raw:
            return result

        content_lines = [l.strip().replace('\n', ' ') for l in content_raw if l.strip()]
        content_text = '\n'.join(content_lines)
        result['content'] = content_text
        result['url'] = url

        # get the metadata
        metadata_raw = tree.xpath(self.item_metadata_dt_xpath + '|' + self.item_metadata_dd_xpath)
        metadata_stop = int(len(metadata_raw) / 2)

        for index in range(0, metadata_stop):
            key_index = index * 2
            value_index = key_index + 1

            key = self.normalise_string(metadata_raw[key_index].text_content())
            if not key:
                continue

            value = self.normalise_string(metadata_raw[value_index].text_content())

            metadata_key = '{:02d}_{}'.format(index, key).lower()

            if metadata_key.endswith('_date'):
                result['metadata'][metadata_key] = datetime.strptime(value, '%d-%m-%Y').strftime('%Y-%m-%d')
            else:
                result['metadata'][metadata_key] = value

        return result

    def normalise_string(self, value):
        if not value:
            return ''

        value = value.replace('’', "'")
        remove_newlines = value.replace('\n', ' ').replace('\r', ' ').strip()
        result = ''.join(c if c in self.allowed_chars else ' ' for c in remove_newlines).strip()
        return result

    def build_rows(self, content_item: Dict[str, Any], attendance_item: Dict[str, Any]) -> Dict[str, Any]:
        """Create rows to be inserted into sqlite db."""
        content = content_item['content']
        url = content_item['url']
        metadata = content_item['metadata']

        # datetimes TEXT: YYYY-MM-DD HH:MM:SS.SSS
        # booleans INTEGER: 0 (false), 1 (true)
        # others are TEXT

        title = content_item['metadata'].get('00_title')
        database_name = content_item['metadata'].get('01_database')
        sitting_date = content_item['metadata'].get('02_date')
        source = content_item['metadata'].get('03_source')
        number = content_item['metadata'].get('04_number') or content_item['metadata'].get('05_number')
        parl_number = content_item['metadata'].get('04_parl no.')
        page = content_item['metadata'].get('05_page') or content_item['metadata'].get('06_page')
        status = content_item['metadata'].get('06_status') or content_item['metadata'].get('07_status')
        federation_chamber_main_committee = content_item['metadata'].get('07_federation chamber / main committee')
        system_id = content_item['metadata'].get('08_system id')

        data = {
            'sitting_day': {
                'url': content_item['url'],
                'title': title,
                'database_name': database_name,
                'sitting_date': sitting_date,
                'source': source,
                'number': number,
                'parl_number': parl_number,
                'page': page,
                'status': status,
                'federation_chamber_main_committee': federation_chamber_main_committee,
                'system_id': system_id,
                'house': attendance_item['house'],
                'clerk': attendance_item['clerk'],
                'retrieved_date': attendance_item['retrieved_date'],
                'raw_content': content_item['content'],
            },
            'people': []
        }

        for person in attendance_item['people']:
            data['people'].append({
                'name': person[0],
                'on_leave': 1 if person[1] is True else 0
            })

        return data

    # ---------- SQLite Database -------------------------

    def sqlite_sitting_day_row(self, db_conn, row: Dict[str, Any]) -> int:
        c = db_conn.execute(
            'INSERT INTO sitting_day '
            '(url,title, database_name, sitting_date, source, number, '
            'parl_number, page, status, federation_chamber_main_committee, system_id, '
            'house, clerk, retrieved_date, raw_content) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            (row['url'], row['title'], row['database_name'], row['sitting_date'], row['source'], row['number'],
             row['parl_number'], row['page'], row['status'], row['federation_chamber_main_committee'], row['system_id'],
             row['house'], row['clerk'], row['retrieved_date'], row['raw_content'], ))

        row_id = c.lastrowid

        return row_id

    def sqlite_attendance_rows(self, db_conn, sitting_day_id: int, rows: List[Dict[str, Any]]) -> int:
        new_rows = [(sitting_day_id, i['name'], i['on_leave']) for i in rows]
        c = db_conn.executemany(
            "INSERT INTO attendance (sitting_day_id, name, on_leave) VALUES (?, ?, ?)", new_rows)
        count = c.rowcount
        return count

    def get_sqlite_db(self):
        conn = sqlite3.connect(self.sqlite_db_file)
        return conn

    def create_sqlite_tables(self, db_conn):
        db_conn.execute(
            'CREATE TABLE '
            'IF NOT EXISTS '
            'sitting_day '
            '('
            'id INTEGER PRIMARY KEY,'
            'url TEXT,'
            'title TEXT,'
            'database_name TEXT,'
            'sitting_date TEXT,'
            'source TEXT,'
            'number TEXT,'
            'parl_number TEXT,'
            'page TEXT,'
            'status TEXT,'
            'federation_chamber_main_committee TEXT,'
            'system_id TEXT,'
            'house TEXT,'
            'clerk TEXT,'
            'retrieved_date TEXT,'
            'raw_content TEXT'
            ')')

        db_conn.execute(
            'CREATE TABLE '
            'IF NOT EXISTS '
            'attendance '
            '('
            'id INTEGER PRIMARY KEY,'
            'sitting_day_id INTEGER,'
            'name TEXT,'
            'on_leave INTEGER'
            ')')

    # ---------- Downloading -----------------------------

    def build_rss_url(self, page_index: int):
        url = self.rss_url.format(page=page_index)
        return url

    def parse_rss_page(self, tree):
        links = [l.tail.strip() for l in tree.cssselect("link")]
        return links

    def download_html(self, url: str):
        content = self.load_page(url)
        if not content:
            page = requests.get(url)
            if page.is_redirect or page.is_permanent_redirect or page.status_code != 200:
                content = None
            else:
                content = page.content
                self.save_page(url, content)

        if not content:
            return

        tree = html.fromstring(content)
        return tree

    # ---------- Local Cache -----------------------------

    def cache_item_id(self, url):
        item_id = ''.join(c if c in self.cache_chars else '' for c in url).strip()
        return item_id

    def save_page(self, url, content) -> None:
        os.makedirs(self.local_cache_dir, exist_ok=True)
        item_id = self.cache_item_id(url)
        file_path = os.path.join(self.local_cache_dir, item_id + '.txt')

        with open(file_path, 'wb') as f:
            f.write(content)

    def load_page(self, url) -> Optional[bytes]:
        os.makedirs(self.local_cache_dir, exist_ok=True)
        item_id = self.cache_item_id(url)
        file_path = os.path.join(self.local_cache_dir, item_id + '.txt')

        if not os.path.isfile(file_path):
            return None

        with open(file_path, 'rb') as f:
            return f.read()


attendance = Attendance()
attendance.run()
