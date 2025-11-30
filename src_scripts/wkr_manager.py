# type: ignore

### Standard imports ###

import os
from glob import glob
import timeit
import pickle
import logging

from datetime import timedelta
from concurrent.futures import ProcessPoolExecutor as Pool

### Local imports ###

from .utilities import unpickler, grouper, filter_by_ext, MultiColorFormatter
from .filterbank_no_rfifind import Filterbank

# from .filterbank import Filterbank

def make_dirs(dirs=[]):
    """Make the appropriate directories.
    """
    for dir in dirs:
        os.makedirs(dir, exist_ok=True)

class PipelineWorker(object):

    """Function-like object that takes a single date as an argument and processes all
    filterbank files in it. This is to circumvent a limitation of the map() method of the
    "ProcessPoolExecutor" class of "concurrent.futures" module which requires the mapped
    function to:

    - take just one argument.
    - be pickle-able, and therefore be defined at the top level of a module.

    It borrows this limitation from the "multiprocessing.Pool" class, since it serves as
    wrapper for that class. However, it is used here, instead of the "multiprocessing"
    module, since it allows launching parallel processes from within each of its parallel
    processes, a feature required for this pipeline to function.
    """

    def __init__(self, config):

        """Create a PipelineWorker object.

        Parameters:
        -----------
        config: CfgManager
            A CfgManager object storing the current configuration of the pipeline.
        """

        self.config = config

    def raw_or_fil(self, path):

        """Function to determine whether the pipeline should start with creating filterbank
        files from raw data or are filterbank files already present. Returns the corresponding
        extension ("*.raw" or "*.fil") depending on whether the former or latter is true,
        otherwise returns None, in which case we have a problem since there is no data to analyse.

        Parameters:
        -----------
        path: str or Path-like
            The absolute path to the directory where the data files are supposed to be.
        """

        RAW_FILES = filter_by_ext(path, extension=".raw")

        try:

            RAW_FILES.__next__()

        except StopIteration:

            try:
                FIL_FILES = filter_by_ext(path, extension=".fil")
                FIL_FILES.__next__()

            except StopIteration:
                return None

            else:
                EXT = ".fil"
                return EXT

        else:

            EXT = ".raw"
            return EXT

    def configure_logger(self, log_path, level=logging.INFO):

        """ Configure the logger for this filterbank file. """

        _logger_name_ = self.date
        self.logger = logging.getLogger(_logger_name_)
        self.logger.setLevel(level)

        formatter = logging.Formatter(fmt="%(asctime)s || %(name)s || %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

        if not self.logger.handlers:
            handler = logging.FileHandler(log_path)
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(MultiColorFormatter())
            self.logger.addHandler(stream_handler)

    def cumulative_walltime(self):

        """Returns the total amount of time spent in processing the whole
        date directory, returned in the appropriate format.
        """

        total_time = timedelta(seconds=self._cumulative_walltime)
        return total_time

    def __call__(self, date):

        """Processes all the filterbank files in a particular date directory.

        Parameters:
        -----------
        date: str
            The date for which data is to be processed.
        """

        #####################################################################################

        self.date = date
        self.date_path = os.path.join(self.config.store_path, self.date)
        self.beam_rfi_path = os.path.join(self.config.rfi_path, self.date)
        self.beam_state_path = os.path.join(self.config.state_path, self.date)

        #####################################################################################

        # Make the appropriate directories.

        make_dirs(dirs=[self.beam_rfi_path, self.beam_state_path])

        # Path to the log file.

        self.log_file = os.path.join(self.beam_state_path, f"{self.date}.log")

        # Configure the logger.

        self.configure_logger(self.log_file)

        # Determine where to start from, raw data or filterbank data.

        EXT = self.raw_or_fil(self.date_path)

        # Start processing all files in the given date directory.

        if EXT:

            self.logger.log(MultiColorFormatter.LOG_LEVEL_NUM, f"Start processing data for date {self.date}.")
            start_time = timeit.default_timer()

            _files_ = filter_by_ext(self.date_path, extension=EXT)
            FILES = list(_files_)
            _proc_files_ = []

            RANK = self.config.RANK
            NRANKS = self.config.NRANKS

            #####################################################################################

            # Unpickle list of Meta objects and get the filenames from them.

            metas_file_list = glob(f"{self.config.logs_path}/{self.date}.history_*.log")
            if not metas_file_list:
                metas_file_list = [f"{self.config.logs_path}/{self.date}.history_{rnk}.log" for rnk in range(NRANKS)]

            for metas_file in metas_file_list:
                _metalist_ = unpickler(metas_file)

                # Decide which mode to open the hidden log file in based on whether it already exists
                # or not and filter the list of files accordingly and iterate through them to process
                # the remaining files. If the hidden log doesn't exist, just return a list of all files.

                try:
                    next(_metalist_)

                    # The pipeline has run before. Restore previous state.
                    for meta in _metalist_:
                        _proc_files_.append(meta["fname"])
                
                except StopIteration:
                    with open(metas_file, "wb+") as _mem_:
                        pickle.dump(self.config, _mem_)
                    continue

            # Remove the files that have already been processed.
            FILES = [FILE for FILE in FILES if FILE.name not in _proc_files_]
            
            # Distribute the files to be processed across different nodes.
            n_files_per_rank = len(FILES) / NRANKS
            # Note that (len(_files_) % NRANKS) might not be zero.
            # If the number of files is not divisible by the number of ranks,
            # the last rank will have to process lesser number of files than all the other ranks.
            if n_files_per_rank % 1 != 0:
                n_files_per_rank = int(n_files_per_rank) + 1
            else:
                n_files_per_rank = int(n_files_per_rank)

            if RANK == NRANKS - 1: # If this is the last rank, process all the remaining files.
                FILES = FILES[RANK * n_files_per_rank : ]
            else:
                FILES = FILES[RANK * n_files_per_rank : (RANK + 1) * n_files_per_rank]

            metas_file = f"{self.config.logs_path}/{self.date}.history_{RANK}.log"

            #####################################################################################

            # Path to the human readable version of the log file.

            filelist = os.path.join(self.beam_state_path, "filelist")

            #####################################################################################

            for FILE in FILES:

                with open(metas_file, "ab") as _mem_, open(filelist, "a") as _list_:

                    # Initialise the filterbank file and process it.

                    FIL_NAME = FILE.name
                    FIL_PATH = FILE.path
                    filterbank = Filterbank(
                        FIL_NAME,
                        FIL_PATH,
                        self.date,
                        self.beam_rfi_path,
                        self.beam_state_path,
                        self.config,
                    )
                    filterbank.process()

                    # Pickle Metadata object corresponding to the filterbank file
                    # just processed into a hidden log file and store the name of
                    # the file in a human readable filelist.

                    pickle.dump(filterbank.metadata, _mem_)
                    _list_.write(f"{FIL_NAME}\n")

                    # Delete the filterbank file if we started with a "*.raw" file.
                    # Otherwise leave it be.

                    # if (EXT == ".raw") and (filterbank.path.endswith(".fil")):
                    #     os.remove(filterbank.path)
                with open(filelist, "r") as f:
                    line_count = sum(1 for _ in f)
                self.logger.info(f"Number of beams processed so far: {line_count}")

            #####################################################################################

        end_time = timeit.default_timer()
        self._cumulative_walltime = end_time - start_time
        self.logger.log(MultiColorFormatter.LOG_LEVEL_NUM, f"Done processing date {self.date}.")
        self.logger.log(MultiColorFormatter.LOG_LEVEL_NUM, f"Total processing time: {self.cumulative_walltime()}.")


class PipelineManager(object):

    """ Class that handles the parallelisation of the pipeline across multiple dates. """

    def __init__(self, config):

        """Create a PipelineManager object.

        Parameters:
        -----------
        config: CfgManager
            A CfgManager object storing the current configuration of the pipeline.
        """

        self.config = config
        make_dirs(dirs=[self.config.rfi_path, self.config.state_path])

        self.Worker = PipelineWorker(config)

    def process(self):

        """Processes several dates in parallel using the "concurrent.futures"
        module's "ProcessPoolExecutor".
        """

        with Pool() as pool:
            [p for p in pool.map(self.Worker, self.config.analysis_dates)]
