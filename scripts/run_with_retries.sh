#!/usr/bin/env bash
set -euo pipefail

# ---- Config (overridable via env) ----
LAUNCH_URL="${LAUNCH_URL:-https://rose-cheetah.antithesis.com/api/v1/launch/async_platform}"
RETRY_COUNT="${RETRY_COUNT:-3}"
RETRY_INTERVAL_MIN="${RETRY_INTERVAL_MIN:-5}"

# ---- Required inputs ----
: "${USERNAME:?USERNAME is required}"
: "${PASSWORD:?PASSWORD is required}"
: "${DESCRIPTION:?DESCRIPTION is required}"
: "${DURATION:?DURATION is required}"
: "${EMAIL_RECIPIENTS:?EMAIL_RECIPIENTS is required}"

# Linear backoff in seconds: interval*1, interval*2, ... interval*N
RETRY_DELAYS=()
for ((i = 1; i <= RETRY_COUNT; i++)); do
    RETRY_DELAYS+=( $(( RETRY_INTERVAL_MIN * 60 * i )) )
done

# Surface status in the GitHub Actions run UI. No-op outside Actions, so local
# runs are unaffected. Annotations go to stderr (the runner still parses them);
# flip to stdout if a setup needs it.
gh_status() {   # $1=level(warning|error|notice)  $2=title  $3=message  $4=summary_md
    if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
        echo "::$1 title=$2::$3" >&2
        if [[ -n "${GITHUB_STEP_SUMMARY:-}" && -n "${4:-}" ]]; then
            printf '%b\n' "$4" >> "$GITHUB_STEP_SUMMARY"
        fi
    else
        echo "$3" >&2
    fi
}

payload=$(jq -n \
    --arg description "$DESCRIPTION" \
    --arg duration "$DURATION" \
    --arg recipients "$EMAIL_RECIPIENTS" \
    '{params: {
        "antithesis.description": $description,
        "custom.duration": $duration,
        "antithesis.config_image": "demo-data-streaming-config:latest",
        "antithesis.report.recipients": $recipients
    }}')

launch() {
    local body_file="$1"
    # Escape for curl's config-file parser, then quote, so secrets with special
    # chars survive and never appear on the command line / in ps.
    local cred="${USERNAME}:${PASSWORD}"
    cred="${cred//\\/\\\\}"
    cred="${cred//\"/\\\"}"
    printf 'user = "%s"\n' "$cred" | curl --silent --show-error \
        -o "$body_file" \
        -w '%{http_code}' \
        -X POST \
        -H "Content-Type: application/json" \
        --config - \
        --data "$payload" \
        "$LAUNCH_URL"
}

body_file=$(mktemp)
trap 'rm -f "$body_file"' EXIT

attempt=0
while true; do
    : > "$body_file"   # truncate from any previous attempt
    status=$(launch "$body_file") || status="000"

    if [[ "$status" == "429" ]]; then
        total=${#RETRY_DELAYS[@]}
        if (( attempt >= total )); then
            gh_status error "Launch gave up" \
                "Still rate limited after $total retries; giving up." \
                "| — | 429 | gave up after $total retries |"
            cat "$body_file" >&2
            exit 1
        fi
        # Write the summary table header once, on the first retry.
        if (( attempt == 0 )) && [[ "${GITHUB_ACTIONS:-}" == "true" \
                && -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
            printf '### ⏳ Launch retries (rate limited)\n\n| Attempt | HTTP | Next action |\n| --- | --- | --- |\n' \
                >> "$GITHUB_STEP_SUMMARY"
        fi
        delay=${RETRY_DELAYS[$attempt]}
        gh_status warning "Launch rate limited" \
            "Rate limited (HTTP 429): attempt $((attempt + 1))/$total — retrying in $((delay / 60)) min." \
            "| $((attempt + 1)) | 429 | wait $((delay / 60)) min |"
        sleep "$delay"
        attempt=$(( attempt + 1 ))
        continue
    fi

    if [[ "$status" =~ ^2 ]]; then
        if (( attempt > 0 )); then
            gh_status notice "Launch recovered" \
                "Launch succeeded (HTTP $status) after $attempt retries." \
                "\n✅ Succeeded after $attempt retries."
        fi
        echo "Launch succeeded (HTTP $status)."
        cat "$body_file"
        exit 0
    fi

    echo "Launch failed with HTTP $status (not retrying)." >&2
    cat "$body_file" >&2
    exit 1
done
