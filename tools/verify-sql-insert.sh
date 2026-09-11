#!/usr/bin/env bash
# Real SQL -> connector -> TCP RPC -> Retina -> SQLite MainIndex/Pixels files.
set -euo pipefail
TRINO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PIXELS=$(cd "${1:?Usage: verify-sql-insert.sh /path/to/pixels [Maven arguments...]}" && pwd)
shift
MVN=${MVN:-mvn}
MAVEN_ARGS=("$@")
: "${PIXELS_HOME:?PIXELS_HOME must point to a built Pixels runtime with native Retina libraries}"
WORK=${SQL_E2E_WORK_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/pixels-sql-e2e-XXXXXXXX")}
mkdir -p "$WORK"
WORK=$(cd "$WORK" && pwd)
if [[ -e "$WORK/ready" || -e "$WORK/stop" ]]; then
    echo "Use a fresh SQL_E2E_WORK_DIR to avoid stale fixture state" >&2
    exit 2
fi
if [[ $(javac -version 2>&1) != 'javac 23'* ]]; then
    echo "Trino 466 SQL verification requires JDK 23" >&2
    exit 2
fi

if [[ ${SQL_E2E_SKIP_BUILD:-0} != 1 ]]; then
    "$MVN" -B -f "$PIXELS/pom.xml" "${MAVEN_ARGS[@]}" -DskipTests=true install > "$WORK/pixels-build.log" 2>&1
    "$MVN" -B -f "$TRINO/pom.xml" "${MAVEN_ARGS[@]}" -DskipTests=true test-compile > "$WORK/trino-build.log" 2>&1
fi
DEPENDENCIES=org.apache.maven.plugins:maven-dependency-plugin:2.10:build-classpath
"$MVN" -B -f "$TRINO/tools/ingest-contract/sql-runtime/pom.xml" "${MAVEN_ARGS[@]}" "$DEPENDENCIES" \
    -Dmdep.outputFile="$WORK/engine.cp" > "$WORK/engine-dependencies.log" 2>&1
"$MVN" -B -f "$TRINO/connector/pom.xml" "${MAVEN_ARGS[@]}" "$DEPENDENCIES" -Dmdep.includeScope=runtime \
    -Dmdep.outputFile="$WORK/plugin-dependencies.cp" > "$WORK/plugin-dependencies.log" 2>&1
"$MVN" -B -f "$PIXELS/pixels-daemon/pom.xml" "${MAVEN_ARGS[@]}" "$DEPENDENCIES" \
    -Dmdep.outputFile="$WORK/backend-dependencies.cp" > "$WORK/backend-dependencies.log" 2>&1

# Use a deployment-style plugin directory with normal filename ordering. Keep the
# full runtime dependency set, including Spike; do not expose it to engine Jersey.
mkdir -p "$WORK/plugin" "$WORK/driver-classes"
jar --create --file "$WORK/plugin/pixels-trino-connector.jar" -C "$TRINO/connector/target/classes" .
python3 - "$WORK" "$PIXELS" <<'PY'
import os
import sys
from pathlib import Path
work, pixels = map(Path, sys.argv[1:])
for item in (work / 'plugin-dependencies.cp').read_text().strip().split(os.pathsep):
    source = Path(item).resolve(strict=True)
    link = work / 'plugin' / source.name
    if link.exists() and link.resolve() != source:
        raise RuntimeError('Duplicate plugin artifact filename: ' + source.name)
    if not link.exists():
        link.symlink_to(source)
(work / 'plugin.cp').write_text(os.pathsep.join(str(p) for p in sorted((work / 'plugin').glob('*.jar'))))
backend = [pixels / 'pixels-daemon/target/test-classes', pixels / 'pixels-daemon/target/classes']
(work / 'backend.cp').write_text(os.pathsep.join(map(str, backend)) + os.pathsep + (work / 'backend-dependencies.cp').read_text().strip())
PY
javac -cp "$(cat "$WORK/engine.cp")" -d "$WORK/driver-classes" \
    "$TRINO/tools/ingest-contract/sql-runtime/src/main/java/io/pixelsdb/pixels/trino/testing/FullSqlInsert.java"

export LD_LIBRARY_PATH="$PIXELS_HOME/lib:${LD_LIBRARY_PATH:-}"
# Retina loads its linked native dependencies through JNI. Do not replace the
# allocator of the entire JVM (and every helper process) here. An explicitly
# configured LD_PRELOAD remains the caller's responsibility.
JAVA_ARGS=(-XX:ActiveProcessorCount=4 --enable-native-access=ALL-UNNAMED
    "-XX:ErrorFile=$WORK/hs_err_pid%p.log"
    --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED)
BACKEND_PID=''
cleanup() {
    local result=$?
    trap - EXIT
    printf '%s\n' "$result" > "$WORK/exit-code"
    touch "$WORK/stop"
    if [[ -n "$BACKEND_PID" ]]; then
        for _ in $(seq 1 50); do
            kill -0 "$BACKEND_PID" 2>/dev/null || break
            sleep .1
        done
        kill "$BACKEND_PID" 2>/dev/null || true
        sleep .1
        kill -KILL "$BACKEND_PID" 2>/dev/null || true
        wait "$BACKEND_PID" 2>/dev/null || true
    fi
    if [[ "$result" != 0 ]]; then
        tail -80 "$WORK/sql.log" 2>/dev/null || true
        tail -30 "$WORK/backend.log" 2>/dev/null || true
        tail -30 "$WORK/runtime.log" 2>/dev/null || true
    fi
    printf 'SQL verification exit=%s evidence=%s\n' "$result" "$WORK"
    exit "$result"
}
trap cleanup EXIT

# Capture failures that occur before the backend reaches its Java main method.
java "${JAVA_ARGS[@]}" -version > "$WORK/runtime.log" 2>&1
{
    printf '\nRetina native dependencies:\n'
    ldd "$PIXELS_HOME/lib/libpixels-retina.so"
} >> "$WORK/runtime.log" 2>&1

# The backend is a separate process with its own dependency graph. Only the
# catalog, node directory and external ID source are fixtures; writes are real.
java "${JAVA_ARGS[@]}" -Xmx1g -cp "$(cat "$WORK/backend.cp")" \
    io.pixelsdb.pixels.daemon.transaction.ingest.SqlIngestFixtureMain "$WORK" > "$WORK/backend.log" 2>&1 &
BACKEND_PID=$!
for _ in $(seq 1 300); do
    [[ -f "$WORK/ready" ]] && break
    kill -0 "$BACKEND_PID" 2>/dev/null || { echo "Backend exited before readiness" >&2; exit 1; }
    sleep .1
done
[[ -f "$WORK/ready" ]] || { echo "Backend readiness deadline exceeded" >&2; exit 1; }
export PIXELS_CONFIG="$WORK/pixels.properties"
timeout -k 10s 180s java "${JAVA_ARGS[@]}" -Xmx3g \
    -cp "$WORK/driver-classes:$(cat "$WORK/engine.cp")" \
    io.pixelsdb.pixels.trino.testing.FullSqlInsert "$WORK" "$WORK/plugin.cp" > "$WORK/sql.log" 2>&1
cat "$WORK/sql.log"
grep -q 'FULL_SQL_INSERT_E2E_PASS' "$WORK/sql.log"
