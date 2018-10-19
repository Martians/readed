#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`
MAX_KEYS=${MAX_KEYS:-"100000"}
THREADS=${THREADS:-"8"}
ITERATIONS=${ITERATIONS:-"1"}

for ((i=0; i<$ITERATIONS; i++)) ; do
    $HT_HOME/bin/ht-start-test-servers.sh --clear --no-thriftbroker \
        --Hypertable.RangeServer.Range.SplitSize=2500K \
        --Hypertable.RangeServer.AccessGroup.MaxMemory=400K \
        --Hypertable.RangeServer.MaintenanceThreads=$THREADS \
        --Hypertable.RangeServer.Maintenance.Interval=10k \
        --Hypertable.RangeServer.AccessGroup.CellCache.PageSize=5k

    $HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql

    $SCRIPT_DIR/dump-loop.sh &

    echo "=================================="
    echo "$THREADS Maintenence Threads WRITE test"
    echo "=================================="

    SPEC="$SCRIPT_DIR/../../data/random-test.spec"
    $HT_HOME/bin/ht load_generator update --spec-file=$SPEC \
        --max-keys=$MAX_KEYS --rowkey-seed=42

    kill %1

    dump_it() {
        $HT_HOME/bin/ht shell --batch < $SCRIPT_DIR/dump-table.hql | wc -l
    }

    count=`dump_it`

    if test $count -ne $MAX_KEYS; then
        echo "Expected: $expected, got: $count"
        # try dump again to see if it's a transient problem
        echo "Second dump: `dump_it`"
        #exec 1>&-
        #sleep 86400
        exit 1
    fi
done

exit 0
