import zstandard
import sqlite3
import cbrrr
import json


with sqlite3.connect("/mnt/tmp/atproto/firehose_snapshot.db") as con:
	samples = [x[0] for x in con.execute("SELECT value FROM records LIMIT 1000000")] # TODO sample randomly?
	dictionary = zstandard.train_dictionary(
		dict_size=0x100000,
		samples=samples,
		threads=-1, # autodetect
	)
	with open("dict_firehose_cbor.bin", "wb") as dictfile:
		dictfile.write(dictionary.as_bytes())
	
	json_samples = []
	for sample in samples:
		parsed = cbrrr.decode_dag_cbor(sample, atjson_mode=True)
		json_str = json.dumps(
			obj=parsed,
			ensure_ascii=False,
			check_circular=False,
			separators=(',', ':')
		)
		json_samples.append(json_str.encode())

	dictionary = zstandard.train_dictionary(
		dict_size=0x100000,
		samples=json_samples,
		threads=-1, # autodetect
	)
	with open("dict_firehose_json.bin", "wb") as dictfile:
		dictfile.write(dictionary.as_bytes())
