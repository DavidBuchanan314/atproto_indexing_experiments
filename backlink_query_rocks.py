import rocksdb
import zstandard
import time
import cbrrr

mydict = zstandard.ZstdCompressionDict(open("dict_firehose_cbor.bin", "rb").read())
decompressor = zstandard.ZstdDecompressor(
	dict_data=mydict,
	format=zstandard.FORMAT_ZSTD1_MAGICLESS
)

def query_prefix(db, prefix: str):
	# TODO: consider "prefix extractor" API
	if prefix.startswith("did:"):
		prefix = "d" + prefix.removeprefix("did:")
	elif prefix.startswith("at://did:"):
		prefix = "a" + prefix.removeprefix("at://did:")
	else:
		raise ValueError("bad prefix")
	prefix = prefix.encode()
	it = db.iterkeys()
	it.seek(prefix)
	for link in it:
		if not link.startswith(prefix):
			break
		target_uri, src_collection, *path, src_did, src_rkey = link.decode().split(" ")
		src_uri = f"at://{src_did}/{src_collection}/{src_rkey}"
		yield target_uri, src_uri, path

def query_record(db, aturi: str):
	assert(aturi.startswith("at://did:"))
	key = ("r" + aturi.removeprefix("at://did:")).encode()
	return cbrrr.decode_dag_cbor(decompressor.stream_reader(db.get(key)).read())

db = rocksdb.DB("/mnt/tmp/atproto/backlinks_rocks.db", rocksdb.Options(create_if_missing=True))
start = time.time()

# who followed me?
print("follows:")
for target_uri, src_uri, path in query_prefix(db, "did:plc:vwzwgnygau7ed7b7wt5ux7y2 app.bsky.graph.follow subject "):
	print(target_uri, src_uri, path)
print()

# who blocked me?
print("blocks:")
for target_uri, src_uri, path in query_prefix(db, "did:plc:vwzwgnygau7ed7b7wt5ux7y2 app.bsky.graph.block subject "):
	print(target_uri, src_uri, path)
print()

# who @ mentioned me?
print("@ mentions:")
for target_uri, src_uri, path in query_prefix(db, "did:plc:vwzwgnygau7ed7b7wt5ux7y2 app.bsky.feed.post facets $[] features $[] did "):
	print(target_uri, src_uri, path)
print()

# who replied to that one post?
print("replies:")
for target_uri, src_uri, path in query_prefix(db, "at://did:plc:vwzwgnygau7ed7b7wt5ux7y2/app.bsky.feed.post/3lkx2a3sagk27 app.bsky.feed.post reply parent uri "):
	print(target_uri, src_uri, path)
	# lets print the replies
	print(query_record(db, src_uri))
print()

# how many people liked it?
print("likes:")
print(len(list(query_prefix(db, "at://did:plc:vwzwgnygau7ed7b7wt5ux7y2/app.bsky.feed.post/3lkx2a3sagk27 app.bsky.feed.like subject uri "))))
print()

print("everything involving me:")
for target_uri, src_uri, path in query_prefix(db, "at://did:plc:vwzwgnygau7ed7b7wt5ux7y2/"):
	#print(target_uri, src_uri, path)
	pass # there's kinda too much to print lol
print()

print(f"all queries complete in {time.time()-start:.6f} seconds")
