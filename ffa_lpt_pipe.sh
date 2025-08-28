#!/bin/bash

usage() {
cat <<EOF
This script sets up and runs the FFA pipeline using MPI.

Usage: $(basename "$0") [OPTIONS]

Options:
    -i INPUT,       Specify the input observation file (default: None).
    -o OUTPUT,      Specify the output directory (default: None).
    -n NODESLIST,   Specify the file listing the nodes to use.
    -b BACKEND,     Specify the backend to use (possible values: GSB, GWB, SPOTLIGHT, SIM; default: SPOTLIGHT).
    [-a],           Optional: Analyse all the scans in the observation director(y/ies).
                    If '-a' flag is not passed, process only the scans mentioned in the LPTs_config.yaml file.
    [-h],           Show this help message and exit.

Examples:
    $(basename "$0") -i /path/to/input_obs_dirs.txt -o /path/to/output_dir -n /path/to/nodes.list -b SPOTLIGHT -a
    $(basename "$0") -i /path/to/input_obs_dirs.txt -o /path/to/output_dir -n /path/to/nodes.list -b GWB
    $(basename "$0") -h

Author: Kenil Ajudiya (kenilr@alum.iisc.ac.in);       Date: 2025-08-18
EOF
}

FFA_PIPE_REPO="/lustre_archive/gnsmdev/Kenil/ffapipe"
LAUNCH_FFAPIPE="${FFA_PIPE_REPO}/launch_ffapipe.sh"
FFA_CONFIG="${FFA_PIPE_REPO}/configurations/LPTs_config.yaml"
MPI_CONFIG_DIR="${FFA_PIPE_REPO}/configurations/MPI_config"
HOSTFILE="${MPI_CONFIG_DIR}/hosts.txt"
RANKFILE="${MPI_CONFIG_DIR}/ranks.txt"

# Initialize variables for the flags and option
flag_a=false # Flag for -a
backend="SPOTLIGHT" # Stores the value for the -b option
input_obs_file="" # Stores the value for the -i option
output_dir="" # Stores the value for the -o option
nodes_list=() # Array to store the list of nodes to be used for processing

# Parse command line options
if [[ $# -eq 0 ]]; then
    usage
    exit 0
fi
while getopts "ab:i:o:n:h" opt; do
    case $opt in
    a)
        flag_a=true # Set the flag for -a
        ;;
    b)
        # Check if a value is provided for -b
        if [[ -z "$OPTARG" || "$OPTARG" == -* ]]; then
            echo "ERROR: -b option requires a value."
            exit 1
        fi
        
        case "$OPTARG" in
            GSB|GWB|SPOTLIGHT|SIM)
                # The value is one of the allowed backends
                backend="$OPTARG"
                ;;
            *)
                # The value is NOT one of the allowed backends
                echo "ERROR: '$OPTARG' is not a valid backend. Allowed backends are: GSB, GWB, SPOTLIGHT, SIM."
                exit 1
                ;;
        esac
        ;;
    i)
        # Check if a value is provided for -i
        if [[ -z "$OPTARG" || "$OPTARG" == -* ]]; then
            echo "ERROR: -i option requires a value."
            exit 1
        else
            if [[ ! -f "$OPTARG" ]]; then
                echo "ERROR: Input observation file '$OPTARG' does not exist."
                exit 1
            else
                input_obs_file="$OPTARG"
                echo "Input observation file is set to: $input_obs_file"
            fi
        fi
        ;;
    o)
        # Check if a value is provided for -o
        if [[ -z "$OPTARG" || "$OPTARG" == -* ]]; then
            echo "ERROR: -o option requires a value."
            exit 1
        else
            if [[ ! -d "$OPTARG" ]]; then
                echo "WARNING: Output directory '$OPTARG' does not exist. Creating it along with all the missing parent directories."
                mkdir -p "$OPTARG"
                output_dir="$OPTARG"
                echo "Output directory is set to: $output_dir"
            else
                output_dir=$(realpath "$OPTARG") # Get the absolute path of the output directory
                echo "The absolute path of the output directory is: $output_dir"
            fi
        fi
        ;;
    n)
        # Check if a value is provided for -n
        if [[ -z "$OPTARG" || "$OPTARG" == -* ]]; then
            echo "ERROR: -n option requires a value."
            exit 1
        else
            if [[ ! -f "$OPTARG" ]]; then
                echo "ERROR: Nodes list file '$OPTARG' does not exist."
                exit 1
            fi
        fi
        
        # Split the nodes by comma and store them in the array
        while read -r line; do
            nodes_list+=("$line")
        done < $OPTARG
        ;;
    h)
        usage
        exit 0
        ;;
    \?)
        echo "ERROR: Unknown option: $OPTARG"
        usage
        exit 1
        ;;
    *)
        usage
        exit 1
        ;;
    esac
done

# Check if the input observation file is provided
if [[ -z "$input_obs_file" ]]; then
    echo "ERROR: Input observation file is not provided. Use -i to specify it."
    exit 1
fi

NOBS=$(sed -e '/^[[:space:]]*#/d' -e '/^[[:space:]]*$/d' "$input_obs_file" | grep -c "")
if [[ -z "$NOBS" || "$NOBS" -eq 0 ]]; then
    echo "ERROR: Either the input observation file is empty or it does not have any uncommented lines."
    exit 1
else
    echo "Number of observation directories to process: $NOBS."
fi

if [[ $NOBS -gt 1 && "$flag_a" == "false" ]]; then
    echo "WARNING: The '-a' flag is strictly enforced when multiple directories are to be processed."
    flag_a=true
fi

# Check if the output directory is provided
if [[ -z "$output_dir" ]]; then
    echo "ERROR: Output directory is not provided. Use -o to specify it."
    exit 1
fi

# Check if the nodes list is provided
NRANKS=${#nodes_list[@]}
if [[ $NRANKS -eq 0 ]]; then
    echo "ERROR: Nodes list is not provided. Use -n to specify it."
    exit 1
fi

# Generate the hostfile and rankfile based on the nodes list
echo "Generating hostfile and rankfile based on the provided nodes list..."
echo "Nodes list: ${nodes_list[*]}"
# Create or clear the hostfile
echo -n "" > "$HOSTFILE"
echo -n "" > "$RANKFILE"
for i in "${!nodes_list[@]}"; do
    node="${nodes_list[$i]}"
    echo "$node slots=1" >> "$HOSTFILE" # Add the node to the hostfile
    echo "rank $i=$node slot=0" >> "$RANKFILE" # Add the node and its rank to the rankfile
done

source /lustre_archive/apps/tdsoft/env.sh && conda activate FFA

# Read non-comment, non-empty lines into an array
mapfile -t obs_dirs < <(grep -v '^[[:space:]]*#' "$input_obs_file" | grep -v '^[[:space:]]*$')

# Final checks and launch the FFA processing script
for OBS_DIR in "${obs_dirs[@]}"; do
    # Skip lines that are empty or start with a comment
    [[ -z "$OBS_DIR" ]] && continue
    [[ "$OBS_DIR" =~ ^[[:space:]]*# ]] && continue
    
    if [[ -z "$OBS_DIR" ]]; then
        echo "Skipping empty line in input observation file."
        continue # Skip empty lines
    fi

    if [[ ! -d "$OBS_DIR" ]]; then
        echo "ERROR: Observation directory '$OBS_DIR' does not exist or is not a directory."
        continue # Skip to the next line if the directory is invalid
    fi

    echo "Starting to process the following observation: $OBS_DIR"
    # Modify the LPTs_config.yaml file with the input observation directory.
    sed -i "81c\    store_path: $OBS_DIR" $FFA_CONFIG
    sed -i "82c\    output_path: $output_dir" $FFA_CONFIG
    
    # Launch the FFA processing script with the specified parameters
    mpirun \
        -np ${NRANKS} \
        --hostfile "${HOSTFILE}" \
        --map-by rankfile:file="${RANKFILE}" \
        "${LAUNCH_FFAPIPE}" "$flag_a" "$backend"

done
