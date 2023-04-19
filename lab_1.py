import sqlite3
import uuid
import os
from typing import Optional
import threading
import time

DATA_PATH = f'{os.path.dirname(os.path.abspath(__file__))}/data.db'
MINIMUM_COLUMNS = 3


class Files:

    def __init__(self) -> None:
        self.conn = sqlite3.connect(DATA_PATH, check_same_thread=0, timeout=2)
        self.cur = self.conn.cursor()
        self.cur.execute('''CREATE TABLE IF NOT EXISTS files(
        uid TEXT PRIMARY KEY,
        filepath TEXT,
        data BLOB)
        ''')
        self.conn.commit()

    def __hashify(self, given: str) -> str:
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, given))

    def __get_number_of_backups(self, ) -> int:
        return len([i[1] for i in self.cur.execute('PRAGMA table_info(files)').fetchall()])-MINIMUM_COLUMNS

    def __check_entry(self, hash: str) -> None:
        if self.get(hash) is None:
            raise sqlite3.DataError('No such entry')

    def get(self, hash: str) -> Optional[str]:
        self.cur.execute('SELECT filepath FROM files WHERE uid = ?', (hash,))
        return out[0] if (out := self.cur.fetchone()) is not None else None

    def get_list(self, hashList: list[str]) -> list[str]:
        res = ', '.join('?' for unused in hashList)
        self.cur.execute(
            'SELECT filepath FROM files WHERE uid IN (%s)' % res, hashList)
        return [i[0] for i in self.cur.fetchall()]

    def get_all(self, ) -> list[tuple[str]]:
        self.cur.execute('SELECT uid,filepath FROM files')
        return self.cur.fetchall()

    def save(self, path: str) -> str:
        try:
            open(path, 'rb')
        except (FileNotFoundError, IsADirectoryError):
            raise
        else:
            hash = self.__hashify(path)
            self.cur.execute('INSERT OR IGNORE INTO files (uid,filepath,data) VALUES(?, ?, ?)',
                             (hash, path, open(path, 'rb').read()))
            self.conn.commit()
            return hash

    def change(self, old_hash: str, new_hash: str) -> None:
        self.cur.execute(
            f'UPDATE files SET uid  = ? WHERE uid = ?', (new_hash, old_hash))
        self.conn.commit()

    def delete(self, hash: str) -> None:
        self.cur.execute(
            f'DELETE FROM files WHERE uid = ?', (hash,))
        self.conn.commit()

    def backup_save(self, hash: str) -> None:
        rowid: int = self.cur.execute(
            f'SELECT rowid FROM files WHERE uid = ?', (hash,)).fetchone()[0]
        last_backup: int = len([i for i in self.cur.execute(
            'SELECT * FROM files WHERE rowid= ?', (rowid,)).fetchone() if i is not None])-MINIMUM_COLUMNS
        if self.__get_number_of_backups() == last_backup:
            self.cur.execute(
                f'ALTER TABLE files ADD backup_{self.__get_number_of_backups()+1}')
        self.cur.execute(f'UPDATE files SET backup_{last_backup+1} = ? WHERE rowid = ?', (open(
            self.get(hash), 'rb').read(), rowid))
        self.conn.commit()

    def backup_save_all_interval(self, interval: float) -> None:
        while True:
            self.backup_save_all()
            time.sleep(interval)

    def backup_save_all(self) -> None:
        [self.backup_save(i[0]) for i in self.get_all()]

    def backup_get(self, hash: str, version: int) -> bytes:
        if (out := self.cur.execute(f'SELECT backup_{version} FROM files WHERE uid = ?', (hash,)).fetchone()[0]) is not None:
            return out
        raise sqlite3.DatabaseError('No backup found')

    def backup_delete_all(self, ) -> None:
        [self.cur.execute(f"ALTER TABLE files DROP COLUMN backup_{i}")
         for i in range(1, self.__get_number_of_backups()+1)]


if __name__ == '__main__':
    INTERVAL = 3
    a, b = Files(), Files()
    thread_b = threading.Thread(
        target=b.backup_save_all_interval, args=(INTERVAL,))
    thread_b.start()

    # * Tests
    print(a.save('/home/lazyslav/Desktop/tested'))
    print(a.save('/home/lazyslav/Desktop/data.csv'))
    print(a.get(''))
    print(a.get('5513a852-9a72-57c0-92cf-11b8ef9ada63'))
    print(a.get_list(['', '']))
    print(a.get_list(['5513a852-9a72-57c0-92cf-11b8ef9ada63', ""]))
    time.sleep(3)
    print(a.get_list(['5513a852-9a72-57c0-92cf-11b8ef9ada63',
                      '4f83bc43-d9a7-5ff6-8d48-b8d7cca9cc22']))
    print(a.get_all())
    a.delete('')
    print(a.get_all())
    a.delete('4f83bc43-d9a7-5ff6-8d48-b8d7cca9cc22')
    print(a.get_all())
    # a.change('5513a852-9a72-57c0-92cf-11b8ef9ada63', '1')
    print(a.backup_save('3d5a8fed-280c-549c-bbf7-77a1f77a0bfd'))
    print(a.backup_save('5513a852-9a72-57c0-92cf-11b8ef9ada63'))
