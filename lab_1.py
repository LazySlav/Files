import sqlite3
import uuid
import os
from typing import Optional
data_path = f'{os.path.dirname(os.path.abspath(__file__))}/data.db'
conn = sqlite3.connect(data_path)
cur = conn.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS files(
   uid TEXT PRIMARY KEY,
   filepath TEXT
   data BLOB);
""")
conn.commit()


class Files:

    def get(hash: str) -> Optional[str]:
        cur.execute(f"SELECT filepath FROM files WHERE uid = ?;", (hash,))
        if (out := cur.fetchone()) is not None:
            return out[0]
        else:
            return None

    def getlist(hashList: list[str]) -> list[str]:
        if len(hashList) <= 1:
            return Files.get(*hashList)
        cur.execute(
            f"SELECT filepath FROM files WHERE uid IN {tuple(hashList)};")
        return [i[0] for i in cur.fetchall()]

    def getall() -> list[str]:
        cur.execute("SELECT uid,filepath FROM files")
        return cur.fetchall()

    def save(path: str) -> str:
        try:
            open(path, 'rb')
        except (FileNotFoundError, IsADirectoryError):
            raise
        else:
            hash = str(uuid.uuid5(uuid.NAMESPACE_DNS, path))
            cur.execute("INSERT OR IGNORE INTO files VALUES(?, ?,?);",
                        (hash, path, open(path, "rb").read()))
            conn.commit()
            return hash

    def change(old_hash: str, new_hash: str) -> None:
        cur.execute(
            f"UPDATE files SET uid  = ? WHERE uid = ?", (new_hash, old_hash))
        conn.commit()

    def delete(hash: str) -> None:
        cur.execute(
            f"DELETE FROM files WHERE uid = ?", (hash,))
        conn.commit()


# print(Files.save(""))
# print(Files.save('/home/lazyslav/Desktop/a'))
# print(Files.get(""))
# print(Files.get("5513a852-9a72-57c0-92cf-11b8ef9ada63"))
# print(Files.getlist(["", ""]))
# print(Files.getlist(["5513a852-9a72-57c0-92cf-11b8ef9ada63"]))
# print(Files.getlist(["5513a852-9a72-57c0-92cf-11b8ef9ada63",
#       "4f83bc43-d9a7-5ff6-8d48-b8d7cca9cc22"]))
# print(Files.getall())
# Files.delete("")
# print(Files.getall())
# Files.delete("4f83bc43-d9a7-5ff6-8d48-b8d7cca9cc22")
# print(Files.getall())
# Files.change("5513a852-9a72-57c0-92cf-11b8ef9ada63", "1")
with open("/home/lazyslav/Desktop/Screenshot_20230213_224219.png", 'rb') as file:
    print(file.read())
