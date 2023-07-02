#!/bin/bash

set -e

path=$(dirname "$0")
cd "$path"

./build.sh

echo "size of wasm: $(stat -f "%z" wit_example.wasm) bytes"
encoded=$(base64 -i wit_example.wasm)
echo "size of encoded wasm: ${#encoded} bytes"

sql="CREATE FUNCTION is_positive (x bigint) RETURNS BOOL LANGUAGE wasm_v1 USING BASE64 '$encoded';"
echo "$sql" > create.sql
psql -h localhost -p 4566 -d dev -U root -f ./create.sql

# test
# FIXME: can we let int work? (auto type conversion)
# FIXME: very slow now.
psql -h localhost -p 4566 -d dev -U root -c "SELECT is_positive(1::bigint);"
psql -h localhost -p 4566 -d dev -U root -c "SELECT is_positive(0::bigint);"
psql -h localhost -p 4566 -d dev -U root -c "SELECT is_positive(-1::bigint);"
