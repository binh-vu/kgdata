"""Functions to split a big file into smaller files.
"""

from bz2 import BZ2File
from gzip import GzipFile
from io import TextIOWrapper
import shutil
from pathlib import Path
from typing import (
    BinaryIO,
    Callable,
    ContextManager,
    Iterable,
    List,
    Tuple,
    Union,
    cast,
)

from sm.misc import get_open_fn, datasize, identity_func, import_func, percentage
from sm.misc.deser import serialize_byte_lines
from tqdm import tqdm
from multiprocessing import Process, Queue


def default_currentbyte_constructor(
    file_object: Union[BZ2File, GzipFile, BinaryIO, TextIOWrapper]
) -> Callable[[], int]:
    """Get a function that returns the current byte position that the file reader is currently at."""
    if isinstance(file_object, BZ2File):
        return file_object.buffer._buffer.raw._fp.tell  # type: ignore

    if isinstance(file_object, GzipFile):
        return file_object.fileobj.tell  # type: ignore

    return file_object.tell


def split_a_file(
    infile: Union[str, Path, Callable[[], Tuple[int, ContextManager[BinaryIO]]]],
    outfile: Union[str, Path],
    record_iter: Callable[
        [Union[BZ2File, GzipFile, BinaryIO]], Iterable[bytes]
    ] = identity_func,
    record_postprocess: str = "kgdata.splitter.strip_newline",
    currentbyte_constructor: Callable[
        [Union[BZ2File, GzipFile, BinaryIO]], Callable[[], int]
    ] = default_currentbyte_constructor,
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
        currentbyte_constructor: a function that returns a function that returns the current byte position of a file object.
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
        with file_object as f, tqdm(
            total=file_size,
            desc="splitting",
            unit="B",
            unit_scale=True,
        ) as pbar:
            last_bytes = 0
            tell = currentbyte_constructor(f)
            for i, line in enumerate(record_iter(f)):
                queues[i % n_writers].put(line)
                current_bytes = tell()
                pbar.update(current_bytes - last_bytes)
                last_bytes = current_bytes
    finally:
        print(">>> Finish! Waiting to exit...")
        for q in queues:
            q.put(None)

        success = True
        for p in writers:
            p.join()
            success = success and (p.exitcode == 0)

        if success:
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


def split_a_list(
    lst: List[bytes], outfile: Union[str, Path], n_records_per_file: int = 64000
):
    outfile = Path(outfile)
    outfile.parent.mkdir(exist_ok=True, parents=True)

    name_parts = outfile.name.split(".", 1)
    name_parts[0] = name_parts[0] + "-{auto:05d}"

    name_template = str(outfile.parent / ".".join(name_parts))
    counter = 0

    for i in tqdm(range(0, len(lst), n_records_per_file), desc="splitting"):
        serialize_byte_lines(
            lst[i : i + n_records_per_file], name_template.format(auto=counter)
        )
        counter += 1
