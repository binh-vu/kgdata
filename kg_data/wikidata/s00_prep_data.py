"""This is the module that split and prepare the raw data downloaded from Wikidata to make it easier to process with Spark"""
import bz2, os, shutil, gzip, glob
from tqdm.auto import tqdm
from multiprocessing import Process, Queue, Pool

from kg_data.config import WIKIDATA_DIR


def save2file(outfile, q):
    file_counter = 0
    n_records_per_file = 64000

    writer = gzip.open(outfile.format(auto=file_counter), "wb")
    n_records = 0

    while True:
        record = q.get()
        if record is None:
            break

        n_records += 1
        if n_records % n_records_per_file == 0:
            writer.close()
            file_counter += 1
            writer = gzip.open(outfile.format(auto=file_counter), "wb")

        # fix the json record (replace the `inplace_fix_file_in_prep01` function)
        if record[-3:] == b"},\n":
            record = record[:-3]
            writer.write(record)
            writer.write(b"}\n")
        elif record[-2:] == b"}\n":
            writer.write(record)
        elif record == b']\n':
            continue
        else:
            print(record)
            raise Exception("Unreachable!")

    writer.close()


def prep01(
        infile: str = os.path.join(WIKIDATA_DIR, "step_0/wikidata-20200831-all.json.bz2"),
        outdir: str = os.path.join(WIKIDATA_DIR, "step_1")):
    if os.path.exists(outdir):
        shutil.rmtree(outdir)
    os.mkdir(outdir)

    n_writers = 8

    queues = []
    writers = []

    for i in range(n_writers):
        outfile = os.path.join(outdir, "{auto:05d}.%s.gz" % i)
        queues.append(Queue())
        writers.append(Process(target=save2file, args=(outfile, queues[i])))
        writers[i].start()

    with bz2.open(infile, "rb") as f:
        line = f.readline()[:-1]
        assert line == b'['

        for i, line in tqdm(enumerate(f), total=85883865):
            queues[i % n_writers].put(line)

    print(">>> Finish! Waiting to exit...")
    for q in queues:
        q.put(None)

    for p in writers:
        p.join()


if __name__ == "__main__":
    prep01()
