import glob
import os
import re
import gzip
import csv
from pathlib import Path
from typing import List, Union
from collections import defaultdict
from kgdata.config import WIKIDATA_DIR
from tqdm import tqdm
from multiprocessing import Process, Queue


def save_record_id_to_wikidata_id(outfile, q):
    file_counter = 0
    n_records_per_file = 64000

    writer = gzip.open(outfile.format(auto=file_counter), "wb")
    n_records = 0

    while True:
        record = q.get()
        if record is None:
            break

        if not record.startswith("INSERT INTO"):
            continue

        for v in parse_sql_values(record):
            if is_wd_item(v[2]):
                pair = v[0], v[2]
                writer.write("\t".join(pair).encode("utf-8"))
                writer.write(b"\n")

        n_records += 1

        if n_records % n_records_per_file == 0:
            writer.close()
            file_counter += 1
            writer = gzip.open(outfile.format(auto=file_counter), "wb")

    writer.close()


def extract_redirections(
    page_dump: Union[str, Path],
    redirect_dump: Union[str, Path],
    outdir: Union[str, Path],
) -> Path:
    """Extract redirections from Wikidata redirection dumps and store the mapping in a key value file

    Args:
        page_dump: wikidata page dump (e.g., wikidatawiki-page.sql.gz)
        redirect_dump: wikidata redirect dump (e.g., wikidatawiki-redirect.sql.gz)
        outdir: output directory

    Returns: a list of tsv files containing the redirection from old item to new item
    """
    # step 0 is to gather the internal id of each wikidata item
    out_step0 = Path(outdir) / "step_0"
    out_step0.mkdir(exist_ok=True, parents=True)

    redirection_file = Path(outdir) / "redirections.tsv"
    if redirection_file.exists():
        return redirection_file

    if not (out_step0 / "_SUCCESS").exists():
        n_writers = 8
        queues = []
        writers = []

        for i in range(n_writers):
            outfile = os.path.join(out_step0, "{auto:05d}.%s.gz" % i)
            queues.append(Queue())
            writers.append(
                Process(target=save_record_id_to_wikidata_id, args=(outfile, queues[i]))
            )
            writers[i].start()

        with gzip.open(str(page_dump), "rt", encoding="utf-8", newline="\n") as f:
            for i, line in tqdm(enumerate(f), total=11514):
                queues[i % n_writers].put(line)

            print(">>> Finish! Waiting to exit...")
            for q in queues:
                q.put(None)

            for p in writers:
                p.join()

            with open(out_step0 / "_SUCCESS", "w") as g:
                g.write("done")

    i_wd = {}
    with tqdm(desc="load internal wikidata id") as pbar:
        for file in glob.glob(str(out_step0 / "*.gz")):
            with gzip.open(file, "r") as f:
                for line in f:
                    i, wd = line.decode().strip().split("\t")
                    assert i not in i_wd
                    i_wd[i] = wd
                    pbar.update()

    buff_obj = []
    with gzip.open(str(redirect_dump), "rt", encoding="utf-8", newline="\n") as f:
        with tqdm(desc="Wikidata redirects") as p_bar:
            for line in f:
                if not line.startswith("INSERT INTO"):
                    continue
                for v in parse_sql_values(line):
                    if is_wd_item(v[2]) and i_wd.get(v[0]):
                        p_bar.update()
                        buff_obj.append((i_wd[v[0]], v[2]))

    # if you encounter any error, that means the following logic is no longer correct,
    # uncomment the following line, and run the function verify_redirection to verify them one by one
    # with open(redirection_file, "w") as f:
    #     for k, v in buff_obj:
    #         f.write("\t".join([k, v]) + "\n")

    redirections = defaultdict(set)
    for before_item, next_item in tqdm(buff_obj):
        redirections[before_item].add(next_item)

    for before_item in tqdm(list(redirections.keys())):
        next_items = redirections[before_item]
        if len(next_items) == 1:
            redirections[before_item] = list(next_items)[0]
            continue

        # there must be only one final item on this list, otherwise, it's not consistent
        final_items = [item for item in next_items if item not in redirections]
        assert len(final_items) == 1, next_items
        final_item = final_items[0]
        redirect_items = [item for item in next_items if item != final_item]

        # now verify for each item, it is being redirected to the same final item
        for item in redirect_items:
            ptr = item
            for i in range(1000):
                if redirections[ptr] == final_item:
                    break
                ptr = redirections[ptr]
                if ptr not in redirections:
                    raise Exception(
                        f"Item {before_item} is mapped to both {final_item} and {item} -> {ptr}"
                    )
            else:
                raise Exception("Likely to encounter a redirection loop.")
        redirections[before_item] = final_item

    with open(redirection_file, "w") as f:
        for k, v in redirections.items():
            f.write("\t".join([k, v]) + "\n")

    return redirection_file


def verify_redirection(infile: Union[str, Path]):
    """Verify the extracted redirection"""
    print("Verify redirection:")
    with open(infile, "r") as f:
        redirections = [line.strip().split("\t") for line in f]

    counter = defaultdict(set)
    for prev, now in redirections:
        counter[prev].add(now)

    for prev, now in counter.items():
        if len(now) > 1:
            print("Redirect to more than one entity:", prev, list(now))


"""
Some functions below are borrowed from Phuc Nguyen <phucnt@nii.ac.jp> in his mtab_dev repo on Github (MIT License).
"""


def convert_num(text):
    if not text:
        return None
    try:
        text = removeCommasBetweenDigits(text)
        # tmp = representsFloat(text)
        # if not tmp:
        #     return None
        #
        # return parseNumber(text)
        return float(text)
    except ValueError:
        return None


def get_wd_int(wd_id):
    result = None
    if wd_id and len(wd_id) and wd_id[0].lower() in ["p", "q"] and " " not in wd_id:
        result = convert_num(wd_id[1:])
    return result


def is_wd_item(wd_id):
    if get_wd_int(wd_id) is None:
        return False
    else:
        return True


def removeCommasBetweenDigits(text):
    """
    :example:
    >>> removeCommasBetweenDigits("sfeyv dsf,54dsf ef 6, 6 zdgy 6,919 Photos and 3,3 videos6,")
    'sfeyv dsf,54dsf ef 6, 6 zdgy 6919 Photos and 33 videos6,'
    """
    if text is None:
        return None
    else:
        return re.sub(r"([0-9]),([0-9])", "\g<1>\g<2>", text)


def parse_sql_values(line):
    values = line[line.find("` VALUES ") + 9 :]
    latest_row = []
    reader = csv.reader(
        [values],
        delimiter=",",
        doublequote=False,
        escapechar="\\",
        quotechar="'",
        strict=True,
    )
    for reader_row in reader:
        for column in reader_row:
            if len(column) == 0 or column == "NULL":
                latest_row.append(chr(0))
                continue
            if column[0] == "(":
                new_row = False
                if len(latest_row) > 0:
                    if latest_row[-1][-1] == ")":
                        latest_row[-1] = latest_row[-1][:-1]
                        new_row = True
                if new_row:
                    yield latest_row
                    latest_row = []
                if len(latest_row) == 0:
                    column = column[1:]
            latest_row.append(column)
        if latest_row[-1][-2:] == ");":
            latest_row[-1] = latest_row[-1][:-2]
            yield latest_row


if __name__ == "__main__":
    extract_redirections(
        Path(WIKIDATA_DIR) / "20211220/step_0/wikidatawiki-page.sql.gz",
        Path(WIKIDATA_DIR) / "20211220/step_0/wikidatawiki-redirect.sql.gz",
        Path(WIKIDATA_DIR) / "20211220/redirections",
    )
    verify_redirection(Path(WIKIDATA_DIR) / "20211220/redirections/redirections.tsv")
