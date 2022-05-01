"""Functions to split a big file into smaller files.
"""

from bz2 import BZ2File
from gzip import GzipFile
import shutil
from pathlib import Path
from typing import BinaryIO, Callable, ContextManager, Iterable, Tuple, Union, cast

from sm.misc import get_open_fn, datasize, identity_func, import_func, percentage
from tqdm import tqdm
from multiprocessing import Process, Queue


def split_a_file(
    infile: Union[str, Path, Callable[[], Tuple[int, ContextManager[BinaryIO]]]],
    outfile: Union[str, Path],
    record_iter: Callable[
        [Union[BZ2File, GzipFile, BinaryIO]], Iterable[bytes]
    ] = identity_func,
    record_postprocess: str = "kgdata.splitter.strip_newline",
    override: bool = False,
    n_writers: int = 8,
    n_records_per_file: int = 64000,
):
    """Split a file containing a list of records into smaller files stored in a directory.
    The list of records are written in a round-robin fashion by multiple writers (processes)
    in parallel but read process is run in sequence.

    Args:
        infile: path of input file (e.g., '/data/input/bigfile.json.gz') or a function
            that returns a file object (opened in binary mode) and its size in bytes.
        outfile: template of path of output file (e.g., '/data/outputs/smallfile.json.gz') from
            the template, this function will write to files in the parent folder (e.g., '/data/outputs')
            with files named 'smallfile-<number>.json.gz' and an extra file named '_SUCCESS' to
            indicate that the job is done.
        record_iter: a function that returns an iterator of records given a file object, by default it returns the file object itself.
        record_postprocess: name/path to import the function that post-process an record. by default we strip the newline from the end of the string.
            when the function returns None, skip the record.
        override: whether to override existing files.
        n_writers: number of parallel writers.
        n_records_per_file: number of records per file.
    """
    outfile = Path(outfile)
    outdir = outfile.parent

    if outdir.exists():
        if not override and (outdir / "_SUCCESS").exists():
            return
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True)

    queues = []
    writers = []

    for i in range(n_writers):
        name_parts = outfile.name.split(".", 1)
        name_parts[0] = name_parts[0] + "-%02d{auto:05d}" % i
        writer_file = str(outdir / ".".join(name_parts))

        queues.append(Queue())
        writers.append(
            Process(
                target=write_to_file,
                args=(writer_file, n_records_per_file, record_postprocess, queues[i]),
            )
        )
        writers[i].start()

    if isinstance(infile, (str, Path)):
        file_object = get_open_fn(infile)(infile, "rb")
        file_size = Path(infile).stat().st_size
    else:
        assert isinstance(infile, Callable)
        file_size, file_object = infile()

    if file_size == 0:
        file_size = 1

    data_size_file_size = datasize(file_size)
    try:
        with file_object as f, tqdm(total=file_size, desc="splitting") as pbar:
            last_bytes = 0
            for i, line in enumerate(record_iter(f)):
                queues[i % n_writers].put(line)
                current_bytes = f.tell()
                pbar.set_postfix(
                    processed_bytes=f"%.2f%% (%s/%s)"
                    % (
                        current_bytes * 100 / file_size,
                        datasize(current_bytes),
                        data_size_file_size,
                    )
                )
                pbar.update(f.tell() - last_bytes)
                last_bytes = f.tell()
    finally:
        print(">>> Finish! Waiting to exit...")
        for q in queues:
            q.put(None)

        for p in writers:
            p.join()

    (outdir / "_SUCCESS").touch()


def write_to_file(
    outfile_template: str,
    n_records_per_file: int,
    record_postprocessing: str,
    queue: Queue,
):
    """Write records from a queue to a file.

    Args:
        outfile_template: template of path of output file
        n_records_per_file: number of records per file
        record_postprocessing: name/path to import the function that post-process an record. the function can return None to skip the record.
        queue: a queue that yields records to be written to a file, when it yields None, the writer stops.
    """
    file_counter = 0

    outfile = outfile_template.format(auto=file_counter)
    writer = get_open_fn(outfile)(outfile, "wb")
    n_records = 0

    postprocess_fn = import_func(record_postprocessing)

    while True:
        record = queue.get()
        if record is None:
            break

        n_records += 1
        if n_records % n_records_per_file == 0:
            writer.close()
            file_counter += 1
            outfile = outfile_template.format(auto=file_counter)
            writer = get_open_fn(outfile)(outfile, "wb")

        record = postprocess_fn(record)
        if record is None:
            continue

        writer.write(record)
        writer.write(b"\n")

    writer.close()


def strip_newline(line: bytes) -> bytes:
    """Strip newline from a line."""
    return line.rstrip(b"\n")
