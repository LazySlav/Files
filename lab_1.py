import sqlite3
import uuid
import os
from typing import Optional
import threading
import time

DATA_PATH = f'{os.path.dirname(os.path.abspath(__file__))}/data.db'


conn = sqlite3.connect(DATA_PATH, check_same_thread=0)
cur = conn.cursor()
cur.execute('''CREATE TABLE IF NOT EXISTS files(
   uid TEXT PRIMARY KEY,
   filepath TEXT,
   data BLOB)
''')
conn.commit()


class Files:

    @staticmethod
    def __hashify(given: str) -> str:
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, given))

    @staticmethod
    def __get_number_of_backups() -> int:
        return len([i[1] for i in cur.execute('PRAGMA table_info(files)').fetchall()])-3

    @staticmethod
    def __check_entry(hash: str) -> None:
        if Files.get(hash) is None:
            raise sqlite3.DataError('No such entry')

    @staticmethod
    def get(hash: str) -> Optional[str]:
        cur.execute('SELECT filepath FROM files WHERE uid = ?', (hash,))
        if (out := cur.fetchone()) is not None:
            return out[0]
        else:
            return None

    @staticmethod
    def get_list(hashList: list[str]) -> list[str] | Optional[str]:
        if len(hashList) <= 1:
            return Files.get(*hashList)
        cur.execute(
            f'SELECT filepath FROM files WHERE uid IN {tuple(hashList)}')
        return [i[0] for i in cur.fetchall()]

    @staticmethod
    def get_all() -> list[tuple[str]]:
        cur.execute('SELECT uid,filepath FROM files')
        return cur.fetchall()

    @staticmethod
    def save(path: str) -> str:
        try:
            open(path, 'rb')
        except (FileNotFoundError, IsADirectoryError):
            raise
        else:
            hash = Files.__hashify(path)
            cur.execute('INSERT OR IGNORE INTO files (uid,filepath,data) VALUES(?, ?, ?)',
                        (hash, path, open(path, 'rb').read()))
            conn.commit()
            return hash

    @staticmethod
    def change(old_hash: str, new_hash: str) -> None:
        Files.__check_entry(old_hash)
        cur.execute(
            f'UPDATE files SET uid  = ? WHERE uid = ?', (new_hash, old_hash))
        conn.commit()

    @staticmethod
    def delete(hash: str) -> None:
        Files.__check_entry(hash)
        cur.execute(
            f'DELETE FROM files WHERE uid = ?', (hash,))
        conn.commit()

    @staticmethod
    def backup_save(hash: str) -> None:
        Files.__check_entry(hash)
        rowid: int = cur.execute(
            f'SELECT rowid FROM files WHERE uid = ?', (hash,)).fetchone()[0]
        last_backup = len([i for i in cur.execute(
            'SELECT * FROM files WHERE rowid= ?', (rowid,)).fetchone() if i is not None])-3
        if Files.__get_number_of_backups() == last_backup:
            cur.execute(
                f'ALTER TABLE files ADD backup_{Files.__get_number_of_backups()+1}')
        cur.execute(f'UPDATE files SET backup_{last_backup+1} = ? WHERE rowid = ?', (open(
            Files.get(hash), 'rb').read(), rowid))
        conn.commit()

    @staticmethod
    def backup_save_all_interval(interval: float) -> None:
        while True:
            Files.backup_save_all()
            time.sleep(interval)

    @staticmethod
    def backup_save_all() -> None:
        [Files.backup_save(i[0]) for i in Files.get_all()]

    @staticmethod
    def backup_get(hash: str, version: int) -> bytes:
        if (out := cur.execute(f'SELECT backup_{version} FROM files WHERE uid = ?', (hash,)).fetchone()[0]) is not None:
            return out
        raise sqlite3.DatabaseError('No backup found')

    @staticmethod
    def backup_delete_all() -> None:
        [cur.execute(f"ALTER TABLE files DROP COLUMN backup_{i}")
         for i in range(1, Files.__get_number_of_backups()+1)]


if __name__ == '__main__':
    INTERVAL = 60*60*24
    x = threading.Thread(
        target=Files.backup_save_all_interval, args=(INTERVAL,))
    x.start()

    # * Tests
    # print(Files.save(''))
    # print(Files.save('/home/lazyslav/Desktop/tested'))
    # print(Files.save('/home/lazyslav/Desktop/data.csv'))
    # print(Files.get(''))
    # print(Files.get('5513a852-9a72-57c0-92cf-11b8ef9ada63'))
    # print(Files.get_list(['', '']))
    print(Files.get_list(['5513a852-9a72-57c0-92cf-11b8ef9ada63']))
    # print(Files.get_list(['5513a852-9a72-57c0-92cf-11b8ef9ada63',
    #                       '4f83bc43-d9a7-5ff6-8d48-b8d7cca9cc22']))
    # print(Files.get_all())
    # Files.delete('')
    # print(Files.get_all())
    # Files.delete('4f83bc43-d9a7-5ff6-8d48-b8d7cca9cc22')
    # print(Files.get_all())
    # Files.change('5513a852-9a72-57c0-92cf-11b8ef9ada63', '1')
    # print(Files.backup_save('3d5a8fed-280c-549c-bbf7-77a1f77a0bfd'))
    # print(Files.backup_save('5513a852-9a72-57c0-92cf-11b8ef9ada63'))
