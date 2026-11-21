#!/usr/bin/env bash
# Write the OpenAPI and llms.txt artifacts from docs/api-contract.json.
#
#   autumn-harvest-plugin/openapi.json  compact OpenAPI 3.1, compiled in and served.
#   docs/openapi.json                   pretty-printed OpenAPI 3.1, for reading.
#   autumn-harvest-plugin/llms.txt      Markdown discovery index, compiled in and served.
#   docs/llms.txt                       identical Markdown discovery index, for reading.
#
# `cargo test -j 2 -p autumn-harvest-plugin --test openapi_spec` fails on drift.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${root}"

contract="docs/api-contract.json"
pretty="docs/openapi.json"
compact="autumn-harvest-plugin/openapi.json"
llms_docs="docs/llms.txt"
llms_crate="autumn-harvest-plugin/llms.txt"

# The crate compiles in ${compact} and ${llms_crate}, so the example cannot
# build without both files present. Seed placeholders on a first run.
[ -f "${compact}" ] || echo '{}' > "${compact}"
[ -f "${llms_crate}" ] || : > "${llms_crate}"

jobs="${CARGO_BUILD_JOBS:-2}"
cargo build --quiet -j "${jobs}" -p autumn-harvest-plugin --example emit_openapi
emit="${CARGO_TARGET_DIR:-target}/debug/examples/emit_openapi"

"${emit}" "${contract}"            > "${pretty}.tmp"
"${emit}" "${contract}" --compact  > "${compact}.tmp"
"${emit}" "${contract}" --llms-txt > "${llms_docs}.tmp"
cp "${llms_docs}.tmp" "${llms_crate}.tmp"
mv "${pretty}.tmp" "${pretty}"
mv "${compact}.tmp" "${compact}"
mv "${llms_docs}.tmp" "${llms_docs}"
mv "${llms_crate}.tmp" "${llms_crate}"

echo "wrote ${pretty}, ${compact}, ${llms_docs}, and ${llms_crate}"
