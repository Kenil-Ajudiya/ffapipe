# type: ignore

import os
from mpi4py import MPI
import textwrap
import argparse

from src_scripts.cfg_manager import CfgManager
from src_scripts.wkr_manager import PipelineManager

# Parse arguments.
parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    usage=argparse.SUPPRESS,
    description=textwrap.dedent(
        """
    ################################################
    GHRSS Survey FFA Pipeline: Multiple Nodes Script
    ################################################
    
    Launches the GHRSS Survey FFA Pipeline on mutliple
    machines. All dates are processed in parallel.
    
    usage: python %(prog)s -c [config_file] -b [backend]"""
    ),
)

parser.add_argument(
    "-c",
    type=str,
    default="./configurations/LPTs_config.yaml",
    help=textwrap.dedent("""The absolute path to the "LPTs_config.yaml" file."""),
)

parser.add_argument(
    "-b",
    type=str,
    help=textwrap.dedent(
        """The specific backend which produced the data. 
                    Could be one of:
                    
                        1. GMRT Software Backend (GSB), 
                        2. GMRT Wideband Backend (GWB),
                        3. SPOTLIGHT, or
                        4. SIMulated GWB data (SIM).
                    The SIM backend is used only for testing purposes."""
    ),
)

parser.add_argument(
    "-a",
    action="store_true",
    help=textwrap.dedent("""Flag to process all the scans in the observation directory (store_path in the config file) in one go. If provided, this flag overrides the date option of the config file."""),
)

try:
    args = parser.parse_args()

except:
    parser.print_help()
    parser.exit(1)

# Convert any relative paths to absolute paths, just in case.
# Store all arguments.
config_file = os.path.realpath(args.c)
backend = args.b
allscans = args.a

# Set up the MPI environment.
COMM = MPI.COMM_WORLD
RANK = COMM.Get_rank()
NRANKS = COMM.Get_size()

# Get the pipeline configuration.
cfg = CfgManager(config_file, backend, allscans, RANK, NRANKS)

# Initialise the pipeline.
manager = PipelineManager(cfg)
manager.process()