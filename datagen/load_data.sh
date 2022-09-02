#!/bin/sh

set -x

SCALE=${1:-2}

echo "Converting data to ORC"
hive -f ddl/bin_flat/llaptables.sql

# Load the keys.
echo "Loading PK/FK"
beeline -u jdbc:hive2://localhost:10500/llap -f ddl/bin_flat/add_constraints.sql
