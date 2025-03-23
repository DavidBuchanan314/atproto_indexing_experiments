"""
The purpose of this script is to scrape records from the firehose and put them in a DB,
to form a dataset for perf-testing my indexer.

At this point it kinda only needs to work once, but I might use it as the basis for a more robust firehose ingester in future.

(as is it's really dumb and fragile, with no reconnect logic)
"""

import aiohttp
import asyncio
import sqlite3
import cbrrr
import io
from atmst.blockstore.car_file import ReadOnlyCARBlockStore

async def main():
	with sqlite3.connect("/mnt/tmp/atproto/firehose_snapshot.db") as con:
		con.execute("pragma journal_mode=wal")
		con.execute("CREATE TABLE IF NOT EXISTS records(seq INTEGER NOT NULL, aturi TEXT NOT NULL, value BLOB NOT NULL) STRICT") # nb: no primary key here
		async with aiohttp.ClientSession() as session:
			async with session.ws_connect("wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos?cursor=0") as ws:
				while True:
					header, body = cbrrr.decode_dag_cbor(b"\x82" + await ws.receive_bytes())
					if header.get("op") != 1:
						print(header, body)
						continue
					if header.get("t") == "#commit":
						did = body["repo"]
						seq = body["seq"]
						try:
							if seq % 1000 == 0:
								print(seq, body["time"])
							#print(body)
							bs = ReadOnlyCARBlockStore(io.BytesIO(body["blocks"]))
							for op in body["ops"]:
								if op.get("action") == "create":
									path = op["path"]
									aturi = f"at://{did}/{path}"
									value = bs.get_block(bytes(op["cid"]))
									con.execute("INSERT INTO records (seq, aturi, value) VALUES (?, ?, ?)", (seq, aturi, value))
							con.commit()
						except Exception as e:
							print("error processing seq", seq, e)
							raise e

if __name__ == "__main__":
	asyncio.run(main())
