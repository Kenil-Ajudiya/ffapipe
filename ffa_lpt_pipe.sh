#!/usr/bin/env bash

usage() {
cat <<EOF
This script sets up and runs the FFA pipeline using MPI.

Usage: $(basename "$0") [OPTIONS]

Options:
    -i INPUT,       Specify the input observation file (default: None).
    -o OUTPUT,      Specify the output directory (default: 'obs').
                    If 'obs' is provided, the output will be stored in a directory named FFAPipeData in the input observation directory.
    -m NODESLIST,   Specify the file listing the nodes to use.
    [-b] BACKEND,   Specify the backend to use (possible values: GSB, GWB, SPOTLIGHT, SIM; default: SPOTLIGHT).
    [-a],           Optional: Analyse all the scans in the observation director(y/ies).
                    If '-a' flag is not passed, process only the scans mentioned in the LPTs_config.yaml file.
    [-t] TBIN,      Specify the time binning (default: 10).
    [-f] FBIN,      Specify the frequency binning (default: 4).
    [-p] NHOSTS,    Specify the number of beam hosts (default: 16).
    [-n] NBEAMS,    Specify the number of beams per host (default: 10).
    [-j] NJOBS,     Specify the number of jobs for the xtract2fil command.
                    It is recommended that NJOBS <= NHOSTS. (default: 16).
    [-s] OFFSET,    Specify the offset for the xtract2fil command (default: 64).
    [-h],           Show this help message and exit.

Examples:
    $(basename "$0") -i /path/to/input_obs_dirs.txt -o /path/to/output_dir -n /path/to/nodes.list -b SPOTLIGHT -a
    $(basename "$0") -i /path/to/input_obs_dirs.txt -o /path/to/output_dir -n /path/to/nodes.list -b GWB
    $(basename "$0") -h

Author: Kenil Ajudiya (kenilr@alum.iisc.ac.in);       Date: 2025-09-01
EOF
}

sanity_checks() {
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
    else
        echo "Number of nodes to be used for processing: $NRANKS."
    fi
}

generate_mpi_files() {
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
    if [[ ! -s "$HOSTFILE" || ! -s "$RANKFILE" ]]; then
        echo "ERROR: Failed to generate hostfile or rankfile."
        exit 1
    fi
}

xtract_N_chk() {
    RAW_FILES=$(eval echo "$BEAM_DIR/$SCAN.raw.{0..$UPPER}")
    remote_output=$(ssh -t -t "${nodes_list[0]}" "
        source ${TDSOFT}/env.sh;
        xtract2fil \
            ${RAW_FILES} \
            --output ${OBS_DIR} \
            --scan ${SCAN} \
            --dual \
            --tbin ${tbin} \
            --fbin ${fbin} \
            --nbeams ${nbeams} \
            --njobs ${njobs} \
            --offset ${offset};
        echo \$?")

    local_ssh_status=$?
    if [ "$local_ssh_status" -eq 255 ]; then
        echo "SSH connection failed."
        echo "HELP: SSH command exit status: $local_ssh_status"
        exit 1
    fi
    exit_code=$(echo "$remote_output" | tail -n 1 | tr -d '\r')
    if [ "$exit_code" -eq 0 ]; then
        echo "INFO: Successfully extracted the beams into filterbank files for scan: $SCAN"
        cp "$BEAM_DIR/$SCAN.raw.0.ahdr" "$output_dir"
    else
        echo "$remote_output"
        echo "ERROR: xtract2fil command failed for scan: $SCAN with exit code $exit_code."
    fi
}

analysis() {
    EXP_COUNT=$((nbeams*nhosts))
    UPPER=$((nhosts-1))

    # Read non-comment, non-empty lines into an array
    mapfile -t obs_dirs < <(grep -v '^[[:space:]]*#' "$input_obs_file" | grep -v '^[[:space:]]*$')
    echo "Observation directories to be processed: ${obs_dirs[*]}"

    # Final checks and launch the FFA processing script
    for OBS_DIR in "${obs_dirs[@]}"; do
        if [[ ! -d "$OBS_DIR" ]]; then
            echo "ERROR: Observation directory '$OBS_DIR' does not exist or is not a directory."
            continue # Skip to the next line if the directory is invalid
        fi

        echo "Starting to process the following observation: $OBS_DIR"

        if [[ "$output_dir" == "obs" ]]; then
            output_dir="$OBS_DIR/FFAPipeData"
            if [[ ! -d "$output_dir" ]]; then
                mkdir "$output_dir"
            fi
        fi

        if [[ "$backend" == "SPOTLIGHT" ]]; then
            BEAM_DIR="${OBS_DIR}/BeamData"
            FIL_DIR="${OBS_DIR}/FilData_dwnsmp"

            # Modify the LPTs_config.yaml file with the input observation directory.
            sed -i "81c\    store_path: $FIL_DIR" $FFA_CONFIG
            sed -i "82c\    output_path: $output_dir" $FFA_CONFIG

            if [[ ! -d "$BEAM_DIR" ]]; then
                echo "ERROR: Beam data directory '$BEAM_DIR' does not exist."
                continue # Skip to the next observation directory
            else
                for SCAN in "$BEAM_DIR"/*.raw.0.ahdr; do
                    SCAN=$(basename -s .raw.0.ahdr "$SCAN")
                    if [[ -d "$FIL_DIR/$SCAN" ]] && (( $(ls "$FIL_DIR/$SCAN" | wc -l) == EXP_COUNT )); then
                        echo "INFO: Found the expected number of filterbank files for the scan: $SCAN."
                    else
                        echo "INFO: Couldn't find the expected number of filterbank files for the scan: $SCAN."
                        echo "Attempting to extract beams using xtract2fil."
                        if [[ ! -f "$BEAM_DIR/$SCAN.raw.0" ]]; then
                            echo "ERROR: Raw file '$BEAM_DIR/$SCAN.raw.0' does not exist. Cannot run xtract2fil."
                            continue
                        else
                            STATUS=0
                            xtract_N_chk
                            if [[ $STATUS == "1" ]]; then continue; fi
                        fi
                    fi
                    # Modify the LPTs_config.yaml file with the scan directory.
                    sed -i "75c\        SPOTLIGHT: $SCAN" $FFA_CONFIG
                    # Launch the FFA processing script with the specified parameters
                    mpirun \
                        -np ${NRANKS} \
                        --hostfile "${HOSTFILE}" \
                        --map-by rankfile:file="${RANKFILE}" \
                        "${LAUNCH_FFAPIPE}" "false" "$backend"

                    # Combine all the PDFs in the BM** directories into a single PDF.
                    OP_SCAN_DIR=${output_dir}/state/$SCAN
                    pdfunite "$OP_SCAN_DIR"/*/candidates/candidate_plts.pdf "$OP_SCAN_DIR/$(basename "$OP_SCAN_DIR")_candidates.pdf"
                done
            fi
        else
            # Modify the LPTs_config.yaml file with the input observation directory.
            sed -i "81c\    store_path: $OBS_DIR" $FFA_CONFIG
            sed -i "82c\    output_path: $output_dir" $FFA_CONFIG
            
            # Launch the FFA processing script with the specified parameters
            mpirun \
                -np ${NRANKS} \
                --hostfile "${HOSTFILE}" \
                --map-by rankfile:file="${RANKFILE}" \
                "${LAUNCH_FFAPIPE}" "$flag_a" "$backend"
        fi
    done
}

main() {
    echo "Starting FFA pipeline setup and execution..."
    
    # Initialize variables for the flags and option
    flag_a=false # Flag for -a
    backend="SPOTLIGHT" # Stores the value for the -b option
    input_obs_file="" # Stores the value for the -i option
    output_dir="obs" # Stores the value for the -o option
    nodes_list=() # Array to store the list of nodes to be used for processing
    tbin=10 # Time binning
    fbin=4 # Frequency binning
    nhosts=16 # Number of beam hosts
    nbeams=10 # Number of beams per host
    njobs=16 # Number of jobs for the xtract2fil command
    offset=64 # Offset for the xtract2fil command

    # Parse command line options
    if [[ $# -eq 0 ]]; then
        usage
        exit 0
    fi
    while getopts "ab:f:i:j:m:n:o:p:s:t:h" OPT; do
        case $OPT in
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
                if [[ "$OPTARG" == "obs" ]]; then
                    echo "The output will be stored in a directory named FFAPipeData in the input observation directory."
                    output_dir=$OPTARG
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
            fi
            ;;
        m)
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
        t)
            if [[ "$OPTARG" =~ ^-?[0-9]+$ ]] && (( OPTARG < 0 )); then
                echo "ERROR: -t option requires a positive value."
                exit 1
            fi
            tbin="$OPTARG"
            ;;
        f)
            if [[ "$OPTARG" =~ ^-?[0-9]+$ ]] && (( OPTARG < 0 )); then
                echo "ERROR: -f option requires a positive value."
                exit 1
            fi
            fbin="$OPTARG"
            ;;
        n)
            if [[ "$OPTARG" =~ ^-?[0-9]+$ ]] && (( OPTARG < 0 )); then
                echo "ERROR: -n option requires a positive value."
                exit 1
            fi
            nbeams="$OPTARG"
            ;;
        p)
            if [[ "$OPTARG" =~ ^-?[0-9]+$ ]] && (( OPTARG < 0 )); then
                echo "ERROR: -p option requires a positive value."
                exit 1
            fi
            nhosts="$OPTARG"
            ;;
        s)
            if [[ "$OPTARG" =~ ^-?[0-9]+$ ]]; then
                echo "ERROR: -s option requires an integer value."
                exit 1
            fi
            offset="$OPTARG"
            ;;
        j)
            if [[ "$OPTARG" =~ ^-?[0-9]+$ ]] && (( OPTARG < 0 )); then
                echo "ERROR: -j option requires a positive value."
                exit 1
            fi
            njobs="$OPTARG"
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

    echo "Performing sanity checks..."
    sanity_checks
    echo "Sanity checks passed."
    echo "Generating MPI hostfile and rankfile..."
    generate_mpi_files
    echo "MPI hostfile and rankfile generated."
    echo "Starting analysis..."
    echo "FFAPipe status = ON" > /lustre_archive/spotlight/data/MON_DATA/das_log/FFAPipe_status.log
    echo "Nodes = ${nodes_list[@]}" >> /lustre_archive/spotlight/data/MON_DATA/das_log/FFAPipe_status.log
    analysis
    echo "Analysis completed."
    echo "FFAPipe status = OFF" > /lustre_archive/spotlight/data/MON_DATA/das_log/FFAPipe_status.log
}

TDSOFT="/lustre_archive/apps/tdsoft"
source $TDSOFT/env.sh && conda activate FFA

FFA_PIPE_REPO="${TDSOFT}/ffapipe"
LAUNCH_FFAPIPE="${FFA_PIPE_REPO}/launch_ffapipe.sh"
FFA_CONFIG="${FFA_PIPE_REPO}/configurations/LPTs_config.yaml"
MPI_CONFIG_DIR="${FFA_PIPE_REPO}/configurations/MPI_config"
HOSTFILE="${MPI_CONFIG_DIR}/hosts.txt"
RANKFILE="${MPI_CONFIG_DIR}/ranks.txt"

main "$@"
