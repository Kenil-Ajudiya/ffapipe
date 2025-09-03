#!/usr/bin/env bash
TDSOFT="/lustre_archive/apps/tdsoft"
source $TDSOFT/env.sh && conda activate FFA
# Riptide-v0.0.1 is not directly insatallable via conda or pip, so we need to set the PYTHONPATH.
export PYTHONPATH="$TDSOFT/riptide-0.0.1/:$PYTHONPATH"

FFA_PIPE_REPO="${TDSOFT}/ffapipe"
FFA_EXE="${FFA_PIPE_REPO}/multi_config.py"
FFA_CONFIG="${FFA_PIPE_REPO}/configurations/LPTs_config.yaml"

if [[ "$1" == "true" ]]; then
    "python3" "${FFA_EXE}" "-c" "${FFA_CONFIG}" "-b" "$2" "-a"
else
    "python3" "${FFA_EXE}" "-c" "${FFA_CONFIG}" "-b" "$2"
fi