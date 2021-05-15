from datetime import date, timedelta
from urllib.parse import quote

from attendance.cache_files import CacheFiles
from attendance.parser import Parser
from attendance.store_sqlite import StoreSqlite


class Main:
    """
    The attendance record of senators and members for each sitting day.
    The final data must be written to an SQLite database called "data.sqlite" in the current working directory,
    which has at least a table called "data".
    """

    def __init__(self):
        self._cache = CacheFiles()
        self._parser = Parser()
        self._store = StoreSqlite()

    def build_attendance_url(
        self,
        start_date: date,
        end_date: date,
        ascending: bool = False,
        page: int = 0,
        count: int = 10,
    ):
        if not start_date or not end_date:
            raise ValueError("Start date and end date must be provided.")
        if start_date >= end_date:
            raise ValueError(f"Start date {start_date} must be before {end_date}.")
        order_by = "date-eLast" if ascending is True else "date-eFirst"

        print(
            f"Building attendance url for {start_date} to {end_date} page {page} count {count}."
        )

        url_base = "https://parlinfo.aph.gov.au/parlInfo/feeds/rss.w3p;adv=yes;"
        date_format = "%d/%m/%Y"
        start_date_str = start_date.strftime(date_format)
        end_date_str = end_date.strftime(date_format)

        query_dict = {
            "Content": "Attendance",
            "Title": "Attendance",
            "Date": quote(f"{start_date_str} >> {end_date_str}"),
            "Dataset": "votes,voteshistorical,journals,journalshistorical",
        }
        query_1 = ["%3A".join([k, v]) for k, v in query_dict.items()]
        query_2 = "%20".join(query_1)
        query_3 = query_2.replace("/", "%2F")

        raw = {
            "orderBy": order_by,
            "page": str(page),
            "query": query_3,
            "resCount": str(count),
        }
        pairs = ["=".join([k, v]) for k, v in raw.items()]
        joined = ";".join(pairs)
        return url_base + joined

    def get_page_data(self, start_date: date, end_date: date = None):
        if not end_date:
            end_date = start_date + timedelta(weeks=4)
        page = 0
        while True:
            print(f"Processing page {page}.")
            page_url = self.build_attendance_url(start_date, end_date, page=page)
            page_tree = self._cache.download_html(page_url)
            if page_tree is None:
                print("No content for url, stopping.")
                break

            page_urls = [line.tail.strip() for line in page_tree.cssselect("link")]
            if not page_urls:
                page += 1
                continue

            for item_index, item_url in enumerate(page_urls):
                content_tree = self._cache.download_html(item_url)
                items = self._parser.page_content(item_url, content_tree)
                items_count = len(items)

                if items_count < 1:
                    print(f"No attendance items found for item {item_index}.")
                else:
                    sitting_date = items[-1].sitting_date
                    assembly = items[-1].assembly
                    print(
                        f"Parsed {items_count} items for {assembly} sitting on {sitting_date} for item {item_index}."
                    )

                total, added = self._store.add_items(items)
                print(f"Added {added} new attendance items of {total} total.")

            page += 1

        print(f"Finished gathering attendance.")
