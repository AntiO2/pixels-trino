#!/usr/bin/env bash
set -euo pipefail
if [[ $# -ne 1 ]]; then
    echo "Usage: $0 /path/to/pixels-checkout-with-ingest-contract" >&2
    exit 2
fi
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
pixels=$(cd -- "$1" && pwd)
build=$(mktemp -d)
trap 'rm -rf -- "$build"' EXIT
common="$pixels/pixels-common/src/main/java/io/pixelsdb/pixels/common/ingest"
retina="$pixels/pixels-retina/src/main/java/io/pixelsdb/pixels/retina/ingest"
writer="$repo/connector/src/main/java/io/pixelsdb/pixels/trino/write"
tests="$repo/connector/src/test/java/io/pixelsdb/pixels/trino/write"
# Does not compile the Trino SPI, protobuf adapters, native code, or Maven reactor.
javac --release 8 -Xlint:all -Xlint:-options -Werror -d "$build" \
    "$common"/*.java "$retina/LocalMutationJournal.java" \
    "$writer/PixelsMutationWriter.java" "$tests/MutationWriterContract.java" \
    "$repo/tools/ingest-contract/WriterJournalIntegration.java"
java -cp "$build" io.pixelsdb.pixels.trino.write.MutationWriterContract
java -cp "$build" WriterJournalIntegration
