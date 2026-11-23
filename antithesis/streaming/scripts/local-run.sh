#!/usr/bin/env bash
# Local loop: build, start, activate the workload, run it, collect evidence.
# Usage: scripts/local-run.sh <seconds> [outdir]
#   EVENTUALLY_AFTER_SECS=N  run one eventually after N seconds (driver exits)
#   COVERAGE=1               build the producer/consumer with LLVM coverage
set -euo pipefail
here=$(cd "$(dirname "$0")/.." && pwd)
secs=${1:-300}
out=${2:-$here/runs/local-$(date +%Y%m%d-%H%M%S)}
mkdir -p "$out"; out=$(cd "$out" && pwd)
cd "$here/config"
files=docker-compose.yaml:../local/docker-compose.local.yaml
[ -n "${COVERAGE:-}" ] && files=$files:../local/docker-compose.coverage.yaml
export COMPOSE_FILE=$files COMPOSE_PROJECT_NAME=dds
if [ -z "${EVENTUALLY_AFTER_SECS:-}" ]; then export EVENTUALLY_DISABLED=1; fi
docker compose down -v -t 5 >/dev/null 2>&1 || true
docker compose up -d --build
timeout 600 sh -c 'until docker compose exec -T driver grep -qs antithesis_setup /tmp/sdk.jsonl; do sleep 5; done'
echo "setup complete; activating workload"
docker compose exec -T driver /opt/antithesis/test/v1/main/first_start_workload
sleep "$secs"
docker compose cp driver:/tmp/sdk.jsonl "$out/sdk.jsonl"
for svc in producer consumer; do
  if [ -n "${COVERAGE:-}" ]; then src=/coverage/$svc/sdk.jsonl; else src=/tmp/sdk.jsonl; fi
  docker compose cp "$svc:$src" "$out/sdk-$svc.jsonl" 2>/dev/null || true
done
docker compose logs --no-color > "$out/compose.log"
docker compose ps -a > "$out/ps.txt"
docker compose stop -t 30
if [ -n "${COVERAGE:-}" ]; then
  # Uncovered functions in the services' own source.
  for svc in producer consumer; do
    docker compose run --rm --no-deps -T --entrypoint sh "$svc" -c "
      set -e; cd /$svc; P=\$(ls -d /usr/local/rustup/toolchains/*/lib/rustlib/x86_64-unknown-linux-gnu/bin);
      \$P/llvm-profdata merge -sparse /coverage/$svc/*.profraw -o /tmp/$svc.profdata &&
      \$P/llvm-cov report target/x86_64-unknown-linux-gnu/release/$svc -instr-profile=/tmp/$svc.profdata src/main.rs &&
      \$P/llvm-cov report --show-functions target/x86_64-unknown-linux-gnu/release/$svc -instr-profile=/tmp/$svc.profdata src/main.rs" \
      > "$out/coverage-$svc.txt" 2>&1 || true
  done
fi
echo "evidence in $out"
# Assertions that would fail in a run:
jq -rs '[.[].antithesis_assert // empty] | group_by(.message)[]
  | select(if .[0].assert_type == "sometimes" then any(.hit and .condition) | not
           else any(.hit and (.condition | not)) or (.[0].must_hit and (any(.hit) | not)) end)
  | "\(.[0].display_type)\t\(.[0].message)"' "$out"/sdk*.jsonl
