import glob
from pathlib import Path
import shutil
from hugedict.prelude import rocksdb_load, RocksDBOptions, RocksDBCompressionOptions
from tap import Tap


class LoadDBArgs(Tap):
    name: str
    option: str


db_options = {
    "default": {"name": "", "opts": RocksDBOptions(create_if_missing=True)},
    "compress-1": {
        "name": "--compress-type=zstd",
        "opts": RocksDBOptions(create_if_missing=True, compression_type="zstd"),
    },
    "compress-2": {
        "name": "--compress-type=lz4",
        "opts": RocksDBOptions(create_if_missing=True, compression_type="lz4"),
    },
    "compress-3": {
        "name": "--compress-type=zstd-6",
        "opts": RocksDBOptions(
            create_if_missing=True,
            compression_type="zstd",
            compression_opts=RocksDBCompressionOptions(
                window_bits=15,
                level=6,
                strategy=0,
                max_dict_bytes=0,
                # max_dict_bytes=16 * 1024,
            ),
            # zstd_max_train_bytes=100 * 16 * 1024,
        ),
    },
    "compress-4": {
        "name": "--compress-type=zstd-6-rem",
        "opts": RocksDBOptions(
            create_if_missing=True,
            compression_type="zstd",
            compression_opts=RocksDBCompressionOptions(
                window_bits=15,
                level=6,
                strategy=0,
                max_dict_bytes=16 * 1024,
            ),
            zstd_max_train_bytes=100 * 16 * 1024,
        ),
    },
    "compress-5": {
        "name": "--compress-type=zstd-6-w14",
        "opts": RocksDBOptions(
            create_if_missing=True,
            compression_type="zstd",
            compression_opts=RocksDBCompressionOptions(
                window_bits=14,
                level=6,
                strategy=0,
                max_dict_bytes=0,
            ),
        ),
    },
    "compress-6": {
        "name": "--compress-type=zstd-6-w14-rem",
        "opts": RocksDBOptions(
            create_if_missing=True,
            compression_type="zstd",
            compression_opts=RocksDBCompressionOptions(
                window_bits=14,
                level=6,
                strategy=0,
                max_dict_bytes=16 * 1024,
            ),
            zstd_max_train_bytes=100 * 16 * 1024,
        ),
    },
    "compress-7": {
        "name": "--compress-type=bottom-zstd-6-w14-rem",
        "opts": RocksDBOptions(
            create_if_missing=True,
            compression_type="lz4",
            bottommost_compression_type="zstd",
            bottommost_compression_opts=RocksDBCompressionOptions(
                window_bits=14,
                level=6,
                strategy=0,
                max_dict_bytes=16 * 1024,
            ),
            bottommost_zstd_max_train_bytes=100 * 16 * 1024,
        ),
    },
    "compress-8": {
        "name": "--compress-type=zstd-6-w14-d10-rem",
        "opts": RocksDBOptions(
            create_if_missing=True,
            compression_type="zstd",
            compression_opts=RocksDBCompressionOptions(
                window_bits=14,
                level=6,
                strategy=0,
                max_dict_bytes=16 * 1024,
            ),
            zstd_max_train_bytes=10 * 16 * 1024,
        ),
    },
    "compress-9": {
        "name": "--compress-type=zstd-3-w14-rem",
        "opts": RocksDBOptions(
            create_if_missing=True,
            compression_type="zstd",
            compression_opts=RocksDBCompressionOptions(
                window_bits=14,
                level=3,
                strategy=0,
                max_dict_bytes=16 * 1024,
            ),
            zstd_max_train_bytes=100 * 16 * 1024,
        ),
    },
}


def load(data_dir: Path, name: str, opt_name: str, opts):
    dbpath = data_dir / "databases" / (name + opt_name)
    if dbpath.exists():
        shutil.rmtree(dbpath)

    rocksdb_load(
        str(dbpath),
        opts,
        glob.glob(str(data_dir / name / "part-*.gz")),
        {
            "record_type": {"type": "bin_kv", "key": None, "value": None},
            "is_sorted": False,
        },
        True,
        True,
    )


if __name__ == "__main__":
    data_dir = Path(__file__).parent.parent / "data"

    args = LoadDBArgs().parse_args()
    load(
        data_dir,
        args.name,
        db_options[args.option]["name"],
        db_options[args.option]["opts"],
    )
