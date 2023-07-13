#!/bin/bash

set -e

path=$(dirname "$0")
cd "$path"

if [ ! -f "./my_udf.component.wasm" ]; then
    echo "my_udf.component.wasm not found, please run ./build.sh first"
    exit 1
fi

echo "size of wasm: $(stat -f "%z" my_udf.component.wasm) bytes"
encoded=$(base64 -i my_udf.component.wasm)
echo "size of encoded wasm: ${#encoded} bytes"
# debug:   23557258
# release: 12457072

psql -h localhost -p 4566 -d dev -U root -c "DROP FUNCTION IF EXISTS is_positive;"
sql="CREATE FUNCTION is_positive (x bigint) RETURNS BOOL LANGUAGE wasm_v1 USING BASE64 '$encoded';"
echo "$sql" > create.sql
psql -h localhost -p 4566 -d dev -U root -f ./create.sql

# test
# FIXME: can we let int work? (auto type conversion) https://github.com/risingwavelabs/risingwave/issues/9998
psql -h localhost -p 4566 -d dev -U root -c "SELECT is_positive(1::bigint);"
psql -h localhost -p 4566 -d dev -U root -c "SELECT is_positive(0::bigint);"
psql -h localhost -p 4566 -d dev -U root -c "SELECT is_positive(-1::bigint);"
