import sqlite3
import zstandard
from tqdm import tqdm
import cbrrr
import json

JSON_MODE = False

if JSON_MODE:
	mydict = zstandard.ZstdCompressionDict(open("dict_firehose_json.bin", "rb").read())
else:
	mydict = zstandard.ZstdCompressionDict(open("dict_firehose_cbor.bin", "rb").read())

# NOTE: I'm not setting a higher compression level, I found it made it much slower for little compression ratio gain.
params = zstandard.ZstdCompressionParameters.from_level(
	3,
	format=zstandard.FORMAT_ZSTD1_MAGICLESS,
	write_content_size=False,
	write_dict_id=False
)
compressor = zstandard.ZstdCompressor(dict_data=mydict, compression_params=params)

decompressor = zstandard.ZstdDecompressor(
	dict_data=mydict,
	format=zstandard.FORMAT_ZSTD1_MAGICLESS
)

total_records = 0
total_uncompressed_bytes = 0
total_compressed_bytes = 0
with sqlite3.connect("/mnt/tmp/atproto/firehose_snapshot.db") as con:
	for record, *_ in tqdm(con.execute("SELECT value FROM records")):
		total_records += 1
		total_uncompressed_bytes += len(record)

		if JSON_MODE:
			parsed = cbrrr.decode_dag_cbor(record, atjson_mode=True)
			json_str = json.dumps(
				obj=parsed,
				ensure_ascii=False,
				check_circular=False,
				separators=(',', ':')
			)
			record = json_str.encode()

		compressed = compressor.compress(record)
		#header_overhead = zstandard.frame_header_size(compressed)
		total_compressed_bytes += len(compressed)# - header_overhead

		# sanity check:
		decompressed = decompressor.stream_reader(compressed).read()
		assert(decompressed == record)


print(f"{total_records = }")
print(f"{total_uncompressed_bytes = }")
print(f"{total_compressed_bytes   = }")
print("ratio", total_compressed_bytes/total_uncompressed_bytes)
