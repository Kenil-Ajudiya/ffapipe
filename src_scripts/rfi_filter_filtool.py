####################################################################################################################################################
# Python script to run the RFI Mitigation module named "filtool", which is developed by Yunpeng Men for TRAPUM's PulsarX. The script uses docker
# to run the filtool inside a container. This code parallelizes the RFI mitigation task across CPU threads and docker containers. 
#
# Output:
#	Overwrite original filterbanks (pf=1)
#	Save RFI mitigated filterbanks separately at /path/to/filterbanks/RFI_Mitigated_fils (pf=0)
#
# NOTE: Use appropriate env before running this script:
# $ source /lustre_archive/apps/tdsoft/env.sh
#
# Last Update: December 08, 2025; Kenil Ajudiya (kenilr@iisc.ac.in)
####################################################################################################################################################

import os
import argparse
import subprocess
import logging
from pathlib import Path
from mpi4py import MPI
from multiprocessing import Pool
from functools import partial

from utilities import MultiColorFormatter

def configure_logger(log_path: Path, name: str = "rfi_filter", level=logging.INFO):
    """Configure a logger that writes to file and console with MultiColorFormatter."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # File formatter consistent with wkr_manager.py
    file_formatter = logging.Formatter(fmt="%(asctime)s || %(name)s || %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    if not logger.handlers:
        fh = logging.FileHandler(log_path)
        fh.setFormatter(file_formatter)
        logger.addHandler(fh)

        sh = logging.StreamHandler()
        sh.setFormatter(MultiColorFormatter())
        logger.addHandler(sh)

    return logger

def run_filtool(fil_file: Path, threads=1, logger: logging.Logger = None):
    """
    Run filtool inside Docker for a single filterbank file.
    """

    cmd = [
        "docker", "run", "--rm",
        "-u", f"{os.getuid()}:{os.getgid()}",
        "-v", f"{Path(fil_file).parent}:/data",
        "ypmen/pulsarx",
        "sh", "-c",
        f"filtool -v -t {threads} --filplan /data/filplan.json "
        f"-f /data/{fil_file.name} -o /data/{fil_file.stem}_RFI_Mitigated"
    ]
    # Run filtool and let any stderr/stdout surface; failures raise CalledProcessError
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    expected_out_path = fil_file.parent / f"{fil_file.stem}_RFI_Mitigated_01.fil"
    if not expected_out_path.exists():
        raise FileNotFoundError(f"Expected output not found: {expected_out_path}")

    # Log completion
    if logger:
        logger.info(f"Processed {fil_file.name}")

    return fil_file

def main():
    parser = argparse.ArgumentParser(prog='rfi_filter', description="RFI Mitigation using filtool via PulsarX Docker container.")
    parser.add_argument("fil_dir", type=Path, help="Directory containing filterbank files")
    # parser.add_argument("-f", "--filplan", type=Path, help="Path to filplan.json")
    parser.add_argument("-o", "--overwrite", type=bool, default=False, help="Overwrite flag: True to overwrite original files, False to save mitigated files separately (default: False)")
    parser.add_argument("-w", "--workers", type=int, default=None, help="Max parallel workers")
    parser.add_argument("-t", "--threads", type=int, default=None, help="Threads per filtool run")
    args = parser.parse_args()

    fil_dir: Path = args.fil_dir.resolve()
    log_path = fil_dir / "rfi_mitigation_log.txt"
    logger = configure_logger(log_path, name=f"rfi_filter_{fil_dir.name}")

    # Set up the MPI environment.
    COMM = MPI.COMM_WORLD
    RANK = COMM.Get_rank()
    NRANKS = COMM.Get_size()

    if RANK == 0:
        open(log_path, "w").close()  # Clear the log file at the start
        # shutil.copy(args.filplan, fil_dir)
        # start_time = timeit.default_timer()

    all_fil_files: list[Path] = [f.resolve() for f in fil_dir.glob("BM*.down.fil")]
    n_files_per_rank = len(all_fil_files) / NRANKS
    if n_files_per_rank % 1 != 0:
        n_files_per_rank = int(n_files_per_rank) + 1
    else:
        n_files_per_rank = int(n_files_per_rank)

    if RANK == NRANKS - 1: # If this is the last rank, process all the remaining files.
        fil_files = all_fil_files[RANK * n_files_per_rank : ]
    else:
        fil_files = all_fil_files[RANK * n_files_per_rank : (RANK + 1) * n_files_per_rank]

    # # MPI barrier to ensure all ranks have the filplan before proceeding.
    # COMM.Barrier()

    # Start multiprocessing pool to run filtool on assigned files
    nprocess = args.workers if args.workers else len(fil_files)
    nthreads = args.threads if args.threads else os.cpu_count() // nprocess
    logger.info(f"Rank {RANK} processing {len(fil_files)} files with {nprocess} processes and {nthreads} threads each.")

    pool = Pool(processes=nprocess)
    worker = partial(run_filtool, threads=nthreads, logger=logger)
    pool.map(worker, fil_files)
    pool.close()
    pool.join()

    # COMM.Barrier()  # Ensure all ranks have completed processing before moving files

    # if RANK == 0:
    #     (fil_dir / "filplan.json").unlink()  # Clean up filplan.json after processing
    #     if args.overwrite:
    #         for f in Path(fil_dir).glob("*_RFI_Mitigated_01.fil"):
    #             original_f = fil_dir / f.name.replace("_RFI_Mitigated_01", "")
    #             shutil.move(f, original_f)
    #         logger.info("Overwrote original filterbanks with RFI mitigated files.")
    #     else:
    #         rfi_dir = fil_dir / "RFI_Mitigated_fils"
    #         os.makedirs(rfi_dir, exist_ok=True)
    #         for f in Path(fil_dir).glob("*_RFI_Mitigated_01.fil"):
    #             shutil.move(f, rfi_dir)
    #         logger.info(f"Copied all RFI mitigated files to {rfi_dir}")

    #     total_time = timeit.default_timer() - start_time
    #     h, rem = divmod(total_time, 3600)
    #     m, s = divmod(rem, 60)
    #     formatted_time = f"{int(h):02d}:{int(m):02d}:{s:05.2f}"

    #     # Prepend final summary
    #     with open(log_path, "r+") as f:
    #         old_content = f.read()
    #         f.seek(0)
    #         f.write(f"Completed RFI Mitigation for all the files in {formatted_time}\n{old_content}")

    #     logger.log(MultiColorFormatter.LOG_LEVEL_NUM, f"Completed RFI Mitigation for all the files in {formatted_time}")

if __name__ == "__main__":
    main()
