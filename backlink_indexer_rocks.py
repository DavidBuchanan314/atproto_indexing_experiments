from typing import Iterator
import sqlite3
import rocksdb
from tqdm import tqdm
import cbrrr
import itertools
import zstandard

def deep_iter(obj: cbrrr.DagCborTypes) -> Iterator[cbrrr.DagCborTypes]:
	sentinel = object()
	# "stack" will consist of recursively chained iterators
	stack = iter([([], obj), sentinel])
	while (frame := next(stack)) is not sentinel:
		route, item = frame
		match item:
			case str():
				yield route, item
			case dict():
				stack = itertools.chain([(route+[k], v) for k, v in item.items()], stack)
			case list():
				#stack = itertools.chain([(route+[i], v) for i, v in enumerate(item)], stack)
				stack = itertools.chain([(route+["$[]"], v) for v in item], stack)

"""

rocksdb key prefixes:

d = did: backlink
a = at://did: backlink
r = at://did: record

"""
mydict = zstandard.ZstdCompressionDict(open("dict_firehose_cbor.bin", "rb").read())
params = zstandard.ZstdCompressionParameters.from_level(
	3,
	format=zstandard.FORMAT_ZSTD1_MAGICLESS,
	write_content_size=False,
	write_dict_id=False
)
compressor = zstandard.ZstdCompressor(dict_data=mydict, compression_params=params)

with sqlite3.connect("/mnt/tmp/atproto/firehose_snapshot.db") as read_con:
	db = rocksdb.DB("/mnt/tmp/atproto/backlinks_rocks.db", rocksdb.Options(create_if_missing=True))
	for aturi, rvalue in tqdm(read_con.execute("SELECT aturi, value FROM records")):
		_, _, src_did, src_collection, src_rkey = aturi.split("/")
		parsed = cbrrr.decode_dag_cbor(rvalue)
		batch = rocksdb.WriteBatch()
		# store the record
		record_key = ("r" + aturi.removeprefix("at://did:")).encode()
		batch.put(record_key, compressor.compress(rvalue))
		for route, item in deep_iter(parsed):
			if item.startswith("did:"):
				key = " ".join(["d" + item.removeprefix("did:"), src_collection, " ".join(route), src_did, src_rkey])
				batch.put(key.encode(), b"")
			elif item.startswith("at://did:"):
				key = " ".join(["a" + item.removeprefix("at://did:"), src_collection, " ".join(route), src_did, src_rkey])
				batch.put(key.encode(), b"")
		db.write(batch)
