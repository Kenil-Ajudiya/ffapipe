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
    [-b] BACKEND,   Optional: Specify the backend to use (possible values: GSB, GWB, SPOTLIGHT, SIM; default: SPOTLIGHT).
    [-a],           Optional: Analyse all the scans in the observation director(y/ies).
                    If '-a' flag is not passed, process only the scans mentioned in the LPTs_config.yaml file.
    [-t] TBIN,      Optional: Specify the time binning (default: 10).
    [-f] FBIN,      Optional: Specify the frequency binning (default: 4).
    [-j] NJOBS,     Optional: Specify the number of jobs for the xtract2fil command.
                    It is recommended that NJOBS <= NHOSTS. (default: 16).
    [-s] OFFSET,    Optional: Specify the offset for the xtract2fil command (default: 64).
    [-h],           Optional: Print this help message and exit.

Examples:
    $(basename "$0") -i /path/to/input_obs_dirs.txt -o /path/to/output_dir -n /path/to/nodes.list -b SPOTLIGHT -a
    $(basename "$0") -i /path/to/input_obs_dirs.txt -o /path/to/output_dir -n /path/to/nodes.list -b GWB
    $(basename "$0") -h

Author: Kenil Ajudiya (kenilr@iisc.ac.in);       Date: 2025-12-01
EOF
}

def_colors() {
    # Bright foreground colors
    BRED='\033[91m'           # Bright Red
    BGRN='\033[92m'           # Bright Green
    BYLW='\033[93m'           # Bright Yellow
    BBLU='\033[94m'           # Bright Blue
    BMAG='\033[95m'           # Bright Magenta
    BCYN='\033[96m'           # Bright Cyan
    BWHT='\033[97m'           # Bright White

    # Bold
    BLD='\033[1m'

    # Reset formatting
    RST='\033[0m'
}

print_art() {
    echo -e ""
    echo -e "${BLD}${BRED}          ███████╗ ███████╗  █████╗  ██████╗  ██╗ ██████╗  ███████╗          ${RST}"
    echo -e "${BLD}${BRED}          ██╔════╝ ██╔════╝ ██╔══██╗ ██╔══██╗ ██║ ██╔══██╗ ██╔════╝          ${RST}"
    echo -e "${BLD}${BRED}          █████╗   █████╗   ███████║ ██████╔╝ ██║ ██████╔╝ █████╗            ${RST}"
    echo -e "${BLD}${BRED}          ██╔══╝   ██╔══╝   ██╔══██║ ██╔═══╝  ██║ ██╔═══╝  ██╔══╝            ${RST}"
    echo -e "${BLD}${BRED}          ██║      ██║      ██║  ██║ ██║      ██║ ██║      ███████╗          ${RST}"
    echo -e "${BLD}${BRED}          ╚═╝      ╚═╝      ╚═╝  ╚═╝ ╚═╝      ╚═╝ ╚═╝      ╚══════╝          ${RST}"
    echo -e "${BLD}${BWHT}-----------------------------------------------------------------------------${RST}"
    echo -e "${BLD}${BWHT}------------------------ SPOTLIGHT FFA PIPELINE v2.0 ------------------------${RST}"
    echo -e "${BLD}${BWHT}-----------------------------------------------------------------------------${RST}"
    echo -e ""
    echo -e "${BLD}${BGRN}                     Copyright © 2025 The SPOTLIGHT Team                     ${RST}"
    echo -e "${BLD}${BGRN}               Code at https://github.com/Kenil-Ajudiya/ffapipe              ${RST}"
    echo -e "${BLD}${BGRN}     Report any issues at https://github.com/Kenil-Ajudiya/ffapipe/issues    ${RST}"
    echo -e ""
    echo -e ""
}

sanity_checks() {
    # Check if the input observation file is provided
    if [[ -z "$input_obs_file" ]]; then
        echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # Input observation file is not provided. Use -i to specify it.${RST}"
        exit 1
    fi

    NOBS=$(sed -e '/^[[:space:]]*#/d' -e '/^[[:space:]]*$/d' "$input_obs_file" | grep -c "")
    if [[ -z "$NOBS" || "$NOBS" -eq 0 ]]; then
        echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # Either the input observation file is empty or it does not have any uncommented lines.${RST}"
        exit 1
    else
        echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} Number of observation directories to process: $NOBS."
    fi

    if [[ $backend != "SPOTLIGHT" && $NOBS -gt 1 && "$flag_a" == "false" ]]; then
        echo -e "${BLD}${BYLW}$(date '+%Y-%m-%d %H:%M:%S') # WARNING # The '-a' flag is strictly enforced when multiple directories are to be processed.${RST}"
        flag_a=true
    fi

    # Check if the output directory is provided
    if [[ -z "$output_dir" ]]; then
        echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # Output directory is not provided. Use -o to specify it.${RST}"
        exit 1
    fi

    # Check if the nodes list is provided
    NRANKS=${#nodes_list[@]}
    if [[ $NRANKS -eq 0 ]]; then
        echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # Nodes list is not provided. Use -n to specify it.${RST}"
        exit 1
    else
        echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} Number of nodes to be used for processing: $NRANKS."
    fi
}

generate_mpi_files() {
    # Generate the hostfile and rankfile based on the nodes list
    echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} Nodes list:"
    for node in "${nodes_list[@]}"; do
        echo "                                      - $node"
    done

    # Create or clear the hostfile
    echo -n "" > "$HOSTFILE"
    echo -n "" > "$RANKFILE"
    for i in "${!nodes_list[@]}"; do
        node="${nodes_list[$i]}"
        echo "$node slots=1" >> "$HOSTFILE" # Add the node to the hostfile
        echo "rank $i=$node slot=0" >> "$RANKFILE" # Add the node and its rank to the rankfile
    done
    if [[ ! -s "$HOSTFILE" || ! -s "$RANKFILE" ]]; then
        echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # Failed to generate hostfile or rankfile.${RST}"
        exit 1
    fi
}

xtract_N_chk() {
    RAW_FILES=$(eval echo "$BEAM_DIR/$SCAN.raw.{0..$UPPER}")

    REMOTE_OUTPUT=$(ssh -t -t "${nodes_list[0]}" "
        source ${TDSOFT}/env.sh;
        xtract2fil \
            ${RAW_FILES} \
            --output ${OBS_DIR} \
            --scan ${SCAN} \
            --dual \
            --tbin ${tbin} \
            --fbin ${fbin} \
            --nbeams ${NBEAMS} \
            --njobs ${njobs} \
            --offset ${offset};
        echo \$?")

    LOCAL_SSH_STATUS=$?
    if [[ "$LOCAL_SSH_STATUS" -eq 255 ]]; then
        echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # SSH connection failed.${RST}"
        echo -e "${BLD}${BMAG}$(date '+%Y-%m-%d %H:%M:%S') # HELP # SSH command exit status: $LOCAL_SSH_STATUS${RST}"
        exit 1
    fi
    EXIT_CODE=$(echo "$REMOTE_OUTPUT" | tail -n 1 | tr -d '\r')
    if [[ "$EXIT_CODE" -eq 0 ]]; then
        echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Successfully extracted the beams into filterbank files for scan: $SCAN${RST}"
        cp "${AHDR_FILES[@]}" "$output_dir/state/$SCAN"
        cp "${AHDR_FILES[@]}" "$FIL_DIR/$SCAN"
        cp "${AHDR_FILES[@]}" "$OBS_DIR/FilData/$SCAN"
        rm -f $RAW_FILES
        return 0
    else
        echo "$REMOTE_OUTPUT"
        echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # xtract2fil command failed for scan: $SCAN with exit code $EXIT_CODE.${RST}"
        rm -rf $FIL_DIR/$SCAN
        return 1
    fi
}

filter_RFI(){
    echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Starting RFI filtering for scan: $SCAN${RST}"

    cp ${FFA_PIPE_REPO}/configurations/filplan.json "${FIL_DIR}/${SCAN}"

    $MPIRUN $MPI_ARGS "python" "${RFI_FILTER_SCRIPT}" --workers 48 "${FIL_DIR}/${SCAN}"

    rm "${FIL_DIR}/${SCAN}/filplan.json"

    if [[ $? -eq 0 ]]; then
        echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Successfully completed RFI filtering for scan: $SCAN${RST}"

        if (( $(eval ls -1 "${FIL_DIR}/${SCAN}/BM*.down_RFI_Mitigated_01.fil" | wc -l) == $TOTAL_BMS )); then
            rm "${FIL_DIR}/${SCAN}"/BM*.down.fil
        fi

        return 0
    else
        echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # RFI filtering failed for scan: $SCAN.${RST}"
        return 1
    fi
}

run_ffapipe(){
    echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Launching the FFA pipeline.${RST}"

    if [[ "$flag_a" == "true" ]]; then
        $MPIRUN $MPI_ARGS "python" "${FFA_EXE}" "-c" "${FFA_CONFIG}" "-b" "$backend" "-a"
    else
        $MPIRUN $MPI_ARGS "python" "${FFA_EXE}" "-c" "${FFA_CONFIG}" "-b" "$backend"
    fi

    if [[ $? -eq 0 ]]; then
        echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Successfully completed pipeline run for scan: $SCAN${RST}"
        return 0
    else
        echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # Pipeline run failed for scan: $SCAN.${RST}"
        return 1
    fi
}

filter_candidates(){
    echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Starting candidate filtering for scan: $SCAN${RST}"

    SUMMARY_FILES=("$OP_SCAN_DIR"/BM*/candidates/summary.csv)
    if [[ ${#SUMMARY_FILES[@]} -eq 0 ]]; then
        echo -e "${BLD}${BYLW}$(date '+%Y-%m-%d %H:%M:%S') # WARNING # No candidates found for scan: $SCAN. Skipping candidate filtering step.${RST}"
        return 1
    else
        echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Found ${#SUMMARY_FILES[@]} candidate summary files for scan: $SCAN. Starting candidate filtering step.${RST}"
        ssh -t -t "${nodes_list[0]}" "
            source ${TDSOFT}/env.sh;
            python -u ${FFA_PIPE_REPO}/src_scripts/cand_filter.py ${OP_SCAN_DIR}"

        EXIT_CODE=$?
        if [[ "$EXIT_CODE" -eq 255 ]]; then
            echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # SSH connection failed.${RST}"
            echo -e "${BLD}${BMAG}$(date '+%Y-%m-%d %H:%M:%S') # HELP # SSH command exit status: $EXIT_CODE${RST}"
            exit 1
        fi
        if [[ "$EXIT_CODE" -eq 0 ]]; then
            echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Successfully completed candidate optimisation for scan: $SCAN${RST}"
            return 0
        else
            echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # Candidate optimisation failed for scan: $SCAN.${RST}"
            return 1
        fi
    fi
}

classify_candidates(){
    echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Starting ML classification for scan: $SCAN${RST}"

    if [[ ! -f "${OP_SCAN_DIR}/combined_candidates.csv" ]]; then
        echo -e "${BLD}${BYLW}$(date '+%Y-%m-%d %H:%M:%S') # WARNING # Combined candidates file not found for scan: $SCAN. Cannot perform ML classification.${RST}"
        return 1
    else
        echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Found combined candidates file for scan: $SCAN. Starting ML classification step.${RST}"
        ssh -t -t "${nodes_list[0]}" "
            source ${TDSOFT}/env.sh;
            conda activate ghvfdt_env;
            export PYTHONPATH="$TDSOFT/riptide-0.0.1/:$PYTHONPATH";
            python -u ${TDSOFT}/ghvfdt/GHVFDT_pipeline.py -c ${OP_SCAN_DIR}/combined_candidates.csv -o ${OP_SCAN_DIR}/${SCAN}_positive_candidates.pdf"
        # Classify only the filtered candidates using GHVFDT after converging to a robust candidate sifting algorithm.

        EXIT_CODE=$?
        if [[ "$EXIT_CODE" -eq 255 ]]; then
            echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # SSH connection failed.${RST}"
            echo -e "${BLD}${BMAG}$(date '+%Y-%m-%d %H:%M:%S') # HELP # SSH command exit status: $EXIT_CODE${RST}"
            exit 1
        fi
        if [[ "$EXIT_CODE" -eq 0 ]]; then
            echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Successfully completed ML classification for scan: $SCAN${RST}"
            return 0
        else
            echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # ML classification failed for scan: $SCAN.${RST}"
            return 1
        fi
    fi
}

analysis() {
    # Read non-comment, non-empty lines into an array
    mapfile -t OBS_DIRS < <(grep -v '^[[:space:]]*#' "$input_obs_file" | grep -v '^[[:space:]]*$')
    echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} Observation directories to be processed:"
    for OBS_DIR in "${OBS_DIRS[@]}"; do
        echo "                                                                  - $(basename "$OBS_DIR")"
    done

    # Final checks and launch the FFA processing script
    for OBS_DIR in "${OBS_DIRS[@]}"; do
        if [[ ! -d "$OBS_DIR" ]]; then
            echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # Observation directory '$OBS_DIR' does not exist or is not a directory.${RST}"
            continue # Skip to the next line if the directory is invalid
        fi

        echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Starting to process the following observation: $(basename $OBS_DIR)${RST}"

        if [[ "$op_dir_flag" == "true" ]]; then
            output_dir="$OBS_DIR/FFAPipeData"
            if [[ ! -d "$output_dir" ]]; then
                mkdir "$output_dir"
            fi
        fi

        if [[ "$backend" == "SPOTLIGHT" ]]; then
            # Assume successful processing unless an error occurs.
            OBS_PROC_STATUS=0   # 1 - Unsuccessful; 0 - Successful
            OBS_PROC_STATUS_LOG_FILE="$output_dir/state/$(basename $OBS_DIR)_proc_status.log"
            mkdir -p "$(dirname $OBS_PROC_STATUS_LOG_FILE)"
            BEAM_DIR="${OBS_DIR}/BeamData"
            FIL_DIR="${OBS_DIR}/FilData_dwnsmp"

            # Modify the LPTs_config.yaml file with the input observation directory.
            sed -i "81c\    store_path: $FIL_DIR" $FFA_CONFIG
            sed -i "82c\    output_path: $output_dir" $FFA_CONFIG

            if [[ ! -d "$BEAM_DIR" ]]; then
                echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # Beam data directory '$BEAM_DIR' does not exist.${RST}"
                continue # Skip to the next observation directory
            else
                echo "Starting to process the following observation: $(basename $OBS_DIR). Check $OBS_PROC_STATUS_LOG_FILE." >> $PROC_LOG_FILE
                for SCAN in "$BEAM_DIR"/*.raw.0.ahdr; do
                    SCAN=$(basename -s .raw.0.ahdr "$SCAN")
                    AHDR_FILES=("$BEAM_DIR/$SCAN.raw."*.ahdr)
                    echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} Found ${#AHDR_FILES[@]} .ahdr files for scan: $SCAN."

                    NBEAMS=$(grep -m1 "Total No. of Beams/host[[:space:]]*=" "${AHDR_FILES[0]}" | awk -F= '{print $2}' | xargs)
                    # TOTAL_BMS=$(grep -m1 "Total No. of Beams[[:space:]]*=" "${AHDR_FILES[0]}" | awk -F= '{print $2}' | xargs)
                    # The above is the total number of beams formed in the observation, all of which might not have been recorded.
                    # However, NBEAMS gives the number of beams recorded per host, which is what we need here.
                    NHOSTS="${#AHDR_FILES[@]}"
                    TOTAL_BMS=$((NBEAMS*NHOSTS))
                    UPPER=$((NHOSTS-1))
                    echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} Processing $TOTAL_BMS beams from $NHOSTS hosts and $NBEAMS beams per host."

                    OP_SCAN_DIR=${output_dir}/state/${SCAN}
                    mkdir -p "$OP_SCAN_DIR"
                    SCAN_PROC_STATUS_LOG_FILE="${OP_SCAN_DIR}/${SCAN}_proc_status.log"
                    echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Starting to process the following scan: $SCAN${RST}"
                    echo "Starting to process the following scan: $SCAN. Check ./${SCAN}/${SCAN}_proc_status.log for details." >> $OBS_PROC_STATUS_LOG_FILE
                    # Check if the expected number of filterbank files are already present and are valid, and extract if not.
                    if [[ -d "$FIL_DIR/$SCAN" ]] && (( $(eval ls -1 "$FIL_DIR/$SCAN/*.fil" | wc -l) == TOTAL_BMS )); then
                        echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Found the expected number of filterbank files.${RST}"
                        # Even if the filterbank files are present, check if xtract_N_chk had succeeded earlier by looking for the .ahdr files.
                        MISSING_AHDRS=0
                        for i in $(seq 0 $UPPER); do
                            if [[ ! -f "${FIL_DIR}/${SCAN}/${SCAN}.raw.$i.ahdr" ]]; then
                                MISSING_AHDRS=1
                                break
                            fi
                        done
                        if [[ $MISSING_AHDRS -eq 1 ]]; then
                            echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} Some .ahdr files are missing. Re-running xtract2fil to ensure consistency."
                            xtract_N_chk
                            if [[ $? -eq 0 ]]; then
                                echo "xtract_N_chk succeeded." >> $SCAN_PROC_STATUS_LOG_FILE
                            else
                                echo "xtract_N_chk failed." >> $SCAN_PROC_STATUS_LOG_FILE
                                OBS_PROC_STATUS=1
                                continue
                            fi
                        else
                            echo "Valid filterbank and .ahdr files found." >> $SCAN_PROC_STATUS_LOG_FILE
                        fi
                    else
                        echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} Couldn't find the expected number of filterbank files."
                        echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} Attempting to extract beams using xtract2fil."
                        if [[ ! -f "$BEAM_DIR/$SCAN.raw.0" ]]; then
                            echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # Raw file '$BEAM_DIR/$SCAN.raw.0' does not exist. Cannot run xtract2fil.${RST}"
                            continue
                        else
                            xtract_N_chk
                            if [[ $? -eq 0 ]]; then
                                echo "xtract_N_chk succeeded." >> $SCAN_PROC_STATUS_LOG_FILE
                            else
                                echo "xtract_N_chk failed." >> $SCAN_PROC_STATUS_LOG_FILE
                                OBS_PROC_STATUS=1
                                continue
                            fi
                        fi
                    fi

                    # RFI filtering
                    if (( $(eval ls -1 "$FIL_DIR/$SCAN/*RFI_Mitigated_01.fil" 2>/dev/null | wc -l) == $TOTAL_BMS )); then
                        echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Found RFI mitigated filterbank files for scan: $SCAN. Skipping RFI filtering step.${RST}"
                        echo "RFI mitigated filterbank files found." >> $SCAN_PROC_STATUS_LOG_FILE
                    else
                        echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} RFI mitigated filterbank files not found for scan: $SCAN. Starting RFI filtering step."
                        filter_RFI
                        if [[ $? -eq 0 ]]; then
                            echo "RFI filtering succeeded." >> $SCAN_PROC_STATUS_LOG_FILE
                            continue
                        else
                            echo "RFI filtering failed." >> $SCAN_PROC_STATUS_LOG_FILE
                            OBS_PROC_STATUS=1
                            continue
                        fi
                    fi

                    # Modify the LPTs_config.yaml file with the scan directory.
                    sed -i "75c\        SPOTLIGHT: $SCAN" $FFA_CONFIG
                    run_ffapipe
                    if [[ $? -eq 0 ]]; then
                        echo "Pipeline run succeeded." >> $SCAN_PROC_STATUS_LOG_FILE
                    else
                        echo "Pipeline run failed." >> $SCAN_PROC_STATUS_LOG_FILE
                        OBS_PROC_STATUS=1
                        continue
                    fi

                    # Candidate optimisation
                    filter_candidates
                    if [[ $? -eq 0 ]]; then
                        echo "Candidate optimisation succeeded." >> $SCAN_PROC_STATUS_LOG_FILE
                    else
                        echo "Candidate optimisation failed." >> $SCAN_PROC_STATUS_LOG_FILE
                        OBS_PROC_STATUS=1
                        continue
                    fi

                    # ML classification
                    classify_candidates
                    if [[ $? -eq 0 ]]; then
                        echo "ML classification succeeded." >> $SCAN_PROC_STATUS_LOG_FILE
                    else
                        echo "ML classification failed." >> $SCAN_PROC_STATUS_LOG_FILE
                        OBS_PROC_STATUS=1
                        continue
                    fi
                    echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Done processing the following scan: $SCAN${RST}"
                done

                if [[ $OBS_PROC_STATUS -eq 0 ]]; then
                    echo "Observation processing succeeded." >> $OBS_PROC_STATUS_LOG_FILE
                else
                    echo "Observation processing failed." >> $OBS_PROC_STATUS_LOG_FILE
                fi
            fi
        else
            # Modify the LPTs_config.yaml file with the input observation directory.
            sed -i "81c\    store_path: $OBS_DIR" $FFA_CONFIG
            sed -i "82c\    output_path: $output_dir" $FFA_CONFIG
            
            # Launch the FFA processing script with the specified parameters
            run_ffapipe
        fi
        echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Done processing the following observation: $(basename $OBS_DIR)${RST}"
        echo "Finished processing the following observation: $(basename $OBS_DIR)." >> $PROC_LOG_FILE
    done
}

main() {
    # Initialize variables for the flags and option
    flag_a=false            # Flag for -a
    backend="SPOTLIGHT"     # Stores the value for the -b option
    input_obs_file=""       # Stores the value for the -i option
    output_dir="obs"        # Stores the value for the -o option
    op_dir_flag=false       # Flag to indicate if output directory is 'obs'
    nodes_list=()           # Array to store the list of nodes to be used for processing
    tbin=10                 # Time binning
    fbin=4                  # Frequency binning
    njobs=16                # Number of jobs for the xtract2fil command
    offset=64               # Offset for the xtract2fil command

    # Parse command line options
    if [[ $# -eq 0 ]]; then
        usage
        exit 0
    fi
    while getopts "ab:f:i:j:m:o:s:t:h" OPT; do
        case $OPT in
        a)
            flag_a=true # Set the flag for -a
            ;;
        b)
            # Check if a value is provided for -b
            if [[ -z "$OPTARG" || "$OPTARG" == -* ]]; then
                echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # -b option requires a value.${RST}"
                exit 1
            fi
            
            case "$OPTARG" in
                GSB|GWB|SPOTLIGHT|SIM)
                    # The value is one of the allowed backends
                    backend="$OPTARG"
                    ;;
                *)
                    # The value is NOT one of the allowed backends
                    echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # '$OPTARG' is not a valid backend. Allowed backends are: GSB, GWB, SPOTLIGHT, SIM.${RST}"
                    exit 1
                    ;;
            esac
            ;;
        i)
            # Check if a value is provided for -i
            if [[ -z "$OPTARG" || "$OPTARG" == -* ]]; then
                echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # -i option requires a value.${RST}"
                exit 1
            else
                if [[ ! -f "$OPTARG" ]]; then
                    echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # Input observation file '$OPTARG' does not exist.${RST}"
                    exit 1
                else
                    input_obs_file="$OPTARG"
                    echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} Input observation file is set to: $input_obs_file"
                fi
            fi
            ;;
        o)
            # Check if a value is provided for -o
            if [[ -z "$OPTARG" || "$OPTARG" == -* ]]; then
                echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # -o option requires a value.${RST}"
                exit 1
            else
                if [[ "$OPTARG" == "obs" ]]; then
                    echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} The output will be stored in a directory named ${BBLU}FFAPipeData${RST} in the input observation directory."
                    op_dir_flag=true
                else
                    if [[ ! -d "$OPTARG" ]]; then
                        echo -e "${BLD}${BYLW}$(date '+%Y-%m-%d %H:%M:%S') # WARNING # Output directory '$OPTARG' does not exist. Creating it along with all the missing parent directories.${RST}"
                        mkdir -p "$OPTARG"
                        output_dir="$OPTARG"
                        echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} Output directory is set to: $output_dir"
                    else
                        output_dir=$(realpath "$OPTARG") # Get the absolute path of the output directory
                        echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} The absolute path of the output directory is: $output_dir"
                    fi
                fi
            fi
            ;;
        m)
            # Check if a value is provided for -n
            if [[ -z "$OPTARG" || "$OPTARG" == -* ]]; then
                echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # -n option requires a value.${RST}"
                exit 1
            else
                if [[ ! -f "$OPTARG" ]]; then
                    echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # Nodes list file '$OPTARG' does not exist.${RST}"
                    exit 1
                fi
            fi
            
            # Split the nodes by comma and store them in the array
            mapfile -t nodes_list < <(grep -v '^[[:space:]]*#' "$OPTARG" | grep -v '^[[:space:]]*$')
            ;;
        t)
            if [[ "$OPTARG" =~ ^-?[0-9]+$ ]] && (( OPTARG < 0 )); then
                echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # -t option requires a positive value.${RST}"
                exit 1
            fi
            tbin="$OPTARG"
            ;;
        f)
            if [[ "$OPTARG" =~ ^-?[0-9]+$ ]] && (( OPTARG < 0 )); then
                echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # -f option requires a positive value.${RST}"
                exit 1
            fi
            fbin="$OPTARG"
            ;;
        s)
            if [[ "$OPTARG" =~ ^-?[0-9]+$ ]]; then
                echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # -s option requires an integer value.${RST}"
                exit 1
            fi
            offset="$OPTARG"
            ;;
        j)
            if [[ "$OPTARG" =~ ^-?[0-9]+$ ]] && (( OPTARG < 0 )); then
                echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # -j option requires a positive value.${RST}"
                exit 1
            fi
            njobs="$OPTARG"
            ;;
        h)
            usage
            exit 0
            ;;
        \?)
            echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # Unknown option: $OPTARG${RST}"
            usage
            exit 1
            ;;
        *)
            usage
            exit 1
            ;;
        esac
    done

    echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Starting FFA pipeline setup and execution...${RST}"
    echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Performing sanity checks...${RST}"
    sanity_checks
    echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Sanity checks passed.${RST}"
    echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Generating MPI hostfile and rankfile based on the provided nodes list...${RST}"
    generate_mpi_files
    echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # MPI hostfile and rankfile generated.${RST}"
    echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Starting analysis...${RST}"
    STATUS_LOG_FILE="/lustre_archive/spotlight/data/MON_DATA/das_log/FFAPipe_status.log"
    echo "FFAPipe status = ON" > $STATUS_LOG_FILE
    echo "Nodes = ${nodes_list[@]}" >> $STATUS_LOG_FILE
    analysis
    echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # SUCCESS # Analysis completed.${RST}"
    echo "FFAPipe status = OFF" > $STATUS_LOG_FILE
}

def_colors

print_art

TDSOFT="/lustre_archive/apps/tdsoft"
source $TDSOFT/env.sh && conda activate FFA;
OMPI_PATH="/lustre_archive/apps/correlator/mpi"
export PATH="${OMPI_PATH}/bin/:$PATH"
export PYTHONPATH="$TDSOFT/riptide-0.0.1/:$PYTHONPATH"

FFA_PIPE_REPO="${TDSOFT}/ffapipe"
FFA_EXE="${FFA_PIPE_REPO}/multi_config.py"
RFI_FILTER_SCRIPT="${FFA_PIPE_REPO}/src_scripts/rfi_filter_filtool.py"
FFA_CONFIG="${FFA_PIPE_REPO}/configurations/LPTs_config.yaml"

MPI_CONFIG_DIR="${FFA_PIPE_REPO}/configurations/MPI_config"
HOSTFILE="${MPI_CONFIG_DIR}/hosts.txt"
RANKFILE="${MPI_CONFIG_DIR}/ranks.txt"
MPIRUN="${OMPI_PATH}/bin/mpirun"
MPI_ARGS="--rankfile "${RANKFILE}" \
          --prefix ${OMPI_PATH} \
          -x PATH -x LD_LIBRARY_PATH -x PYTHONPATH \
          --mca rmaps_dist_device mlx5_0 \
          --mca btl_openib_if_include mlx5_0:1 \
          --mca oob_tcp_if_include ib0 \
          --mca btl_openib_allow_ib true \
          --mca btl smcuda,self"

STATUS_LOG_FILE="/lustre_archive/spotlight/data/MON_DATA/das_log/FFAPipe_status.log"
LOG_DIR="/lustre_data/spotlight/data/watched/FFAPipe_logs"
PROC_LOG_FILE="${LOG_DIR}/observation_processing.log"
STD_LOG="$LOG_DIR/std_logs/$(date +%Y%m%d_%H%M%S).log"

# redirect stdout and stderr to both the log and the terminal
exec > >(tee -a "$STD_LOG") 2> >(tee -a "$STD_LOG" >&2)

main "$@"