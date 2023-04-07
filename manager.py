#!/usr/bin/env python

import re
import sqlite3
from pathlib import Path

from postgrest import AsyncPostgrestClient  # type: ignore


class WebService:
    def __init__(self, host: str, table: str, token: str):
        self.host = host
        self.table = table
        self.token = token


class Book:
    def __init__(self, title: str, bundle: str):
        self.title = title
        self.bundle = bundle

    def __repr__(self):
        return f'{self.title} ({self.bundle})'

    def __str__(self):
        return self.__repr__()

    def __eq__(self, other):
        return self.title == other.title and self.bundle == other.bundle

    def __hash__(self):
        return hash((self.title, self.bundle))


def _get_sqlite_cursor(db_path: Path) -> sqlite3.Cursor:
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    return c


def read_file(file_path: Path) -> list[Book] | None:
    with open(file_path, 'r') as file:
        raw_bundle_title = file.readline().strip()  # read the first line as the bundle title

        # parses bundle title out of quotes
        match = re.search(r'^[\w ]+".+:\s+(.+)"\s+\(\d+\s+items\)$', raw_bundle_title)
        if not match:
            return None

        bundle_title = match.group(1)

        next(file)  # skip the next line (it's just a separator line)

        book_list = []
        for line in file:
            book_title = line.strip('- \n')

            if not book_title:
                return None

            book_list.append(Book(book_title, bundle_title))  # add the book to the list with the bundle title

    return book_list


def write_to_sqlite(db_path: Path, *books: Book) -> None:
    cursor = _get_sqlite_cursor(db_path)

    cursor.execute('''CREATE TABLE IF NOT EXISTS books
             (title text, bundle text, unique(title))''')

    for book in books:
        title, bundle = (book.title, book.bundle)

        title_without_edition = title.split(', ')[0]

        # check if the book already exists in the table
        cursor.execute(
            '''SELECT * FROM books
            WHERE instr(title, ?) > 0;''', (title_without_edition,))

        result = cursor.fetchone()
        if result:  # if the book already exists in the table
            # check if the new bundle is newer than the existing one
            if title > result[0]:
                # update the book with the new bundle
                cursor.execute(
                    '''UPDATE books
                    SET bundle=?, title=?
                    WHERE instr(title, ?) > 0;''', (bundle, title, title_without_edition))

        else:  # if the book doesn't exist in the table
            # insert the new book
            cursor.execute(
                '''INSERT INTO books (title, bundle)
                VALUES (?, ?)''', (title, bundle))

    cursor.connection.commit()
    cursor.connection.close()


async def write_to_postgrest(psgrst_service: WebService, *books: Book) -> None:
    async with AsyncPostgrestClient(psgrst_service.host,
                                    headers={'Authorization': f'Bearer {psgrst_service.token}'}) as client:
        for book in books:
            title, bundle = (book.title, book.bundle)
            title_without_edition = title.split(', ')[0]

            result = await client \
                .from_(psgrst_service.table) \
                .select('id, title') \
                .ilike('title', f'{title_without_edition}*') \
                .execute()

            if result.data:
                if title > result.data[0]['title']:
                    # update the book with the new bundle
                    await client \
                        .from_(psgrst_service.table) \
                        .update({'title': title, 'bundle': bundle}) \
                        .eq('id', result.data[0]['id']) \
                        .execute()
            else:
                # insert the new book
                await client \
                    .from_(psgrst_service.table) \
                    .insert({'title': title, 'bundle': bundle}) \
                    .execute()
