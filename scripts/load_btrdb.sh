#!/bin/bash

# Ensure loader is available
EXE_FILE_NAME=${EXE_FILE_NAME:-$(which tsbs_load_btrdb)}
if [[ -z "$EXE_FILE_NAME" ]]; then
    echo "tsbs_load_btrdb not available. It is not specified explicitly and not found in \$PATH"
    exit 1
fi

# Load parameters - common
DATA_FILE_NAME=${DATA_FILE_NAME:-btrdb-data.gz}
DATABASE_PORT=${DATABASE_PORT:-2333}

EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/load_common.sh

# Load new data
cat ${DATA_FILE} | gunzip | $EXE_FILE_NAME \
                                --db-name=${DATABASE_NAME} \
                                --backoff=${BACKOFF_SECS} \
                                --workers=${NUM_WORKERS} \
                                --batch-size=${BATCH_SIZE} \
                                --reporting-period=${REPORTING_PERIOD} \
                                --url=${DATABASE_HOST}:${DATABASE_PORT} \
                                --do-create-db=false \
                                --use-batch-insert=true

