from __future__ import annotations

import os
import signal
import time
from pathlib import Path
from subprocess import Popen
from typing import Literal

import click
from loguru import logger


@click.command("Start the server")
@click.option("--db", required=True, multiple=True, help="Database name(s)")
@click.option("--socket-dir", required=True, help="Directory to store the socket")
@click.option("--dbpath", required=True, help="Directory containing databases")
@click.option("-n", "--n-workers", type=int, default=1, help="Number of DB instances")
def start_server(
    db: tuple[Literal["entity", "entity_metadata"], ...],
    socket_dir: str | Path,
    dbpath: str,
    n_workers: int = 1,
):
    assert isinstance(db, tuple)
    pid_file = Path(__file__).parent / f"started_{'__'.join(sorted(db))}_servers.pid"
    socket_dir = Path(socket_dir).absolute()

    if pid_file.exists():
        logger.error(
            "Server already started or not closed properly. Check the {} files",
            pid_file,
        )
        return

    processes = []

    try:
        for dbname in db:
            paspath = str(
                Path(dbpath)
                / {"entity": "entities.db", "entity_metadata": "entity_metadata.db"}[
                    dbname
                ]
            )
            for i in range(n_workers):
                cmd = [
                    "cargo",
                    "run",
                    "--release",
                    "--",
                    dbname,
                    f"ipc://{socket_dir}/{dbname}.{i:03d}.ipc",
                    paspath,
                ]
                logger.debug("Execute command: {}", " ".join(cmd))
                p = Popen(cmd, cwd=str(Path(__file__).parent.parent), env=os.environ)
                processes.append(p)
                time.sleep(0.5)

        with open(pid_file, "w") as f:
            for p in processes:
                f.write(f"{p.pid}\n")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt. Stopping the servers")
    finally:
        logger.info("Waiting for all processes to finish")
        for p in processes:
            p.send_signal(signal.SIGINT)

        is_finished = [False for p in processes]
        wait_time = [0 for p in processes]
        max_wait_time = 30  # 30 seconds
        for i, p in enumerate(processes):
            if is_finished[i]:
                continue

            try:
                p.wait(1)
                is_finished[i] = True
                logger.info(
                    "Stopped process {} with return code {} ({}/{})",
                    p.pid,
                    p.returncode,
                    sum(is_finished),
                    len(processes),
                )
            except TimeoutError:
                wait_time[i] += 1
                if all(
                    not finish and t >= max_wait_time
                    for t, finish in zip(wait_time, is_finished)
                ):
                    # empty still true -- all jobs either finished or timeout
                    break

        # remove the unfinished processes from the files.
        if all(is_finished):
            pid_file.unlink()
            logger.info("Stop all processes successfully")
        else:
            with open(pid_file, "w") as f:
                for finish, p in zip(is_finished, processes):
                    if not finish:
                        f.write(f"{p.pid}\n")
            logger.error(
                "Some processes are not finished. Check the {} file for their PIDs",
                pid_file,
            )


if __name__ == "__main__":
    start_server()
