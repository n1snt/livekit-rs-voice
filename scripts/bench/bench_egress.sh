#!/usr/bin/env bash
set -euo pipefail

# Benchmarks an egress recorder — Rust `livekit-rs-egress` or Go
# `livekit/egress` — while it records real WebRTC Opus audio, measuring the
# egress's memory + CPU.
#
# Both stacks run fully in Docker via scripts/bench/{rust,go}-stack.yml (all
# containers on one bridge network so WebRTC media flows container-to-container)
# and the egress container's cgroup is sampled, plus the egress process's RSS
# via `ps` inside the container for a like-for-like process comparison.
#
# The workload is identical for both: STREAMS publishers stream Opus audio for
# DURATION+8s, a StartRoomCompositeEgress recording is triggered, stopped after
# DURATION seconds, and the egress resources are sampled over the recording.
# Each stack is driven by its native client: the Rust stack by the webrtc-rs
# `send_audio` publisher, the Go stack by a pion-based Go SDK publisher (the
# Rust webrtc-rs publisher cannot complete DTLS-SRTP against pion).
#
# Usage:
#   scripts/bench/bench_egress.sh --stack rust|go [--seconds N] [--runs N]
#                                 [--streams N] [--format wav|mp3]
#
# Prerequisites:
#   rust: Docker images livekit-rs-voice:local + livekit-rs-egress:local built
#         from the current source, and the publisher image:
#           docker build -f scripts/bench/Dockerfile.publisher -t lk-rs-bench-publisher:latest .
#   go:   livekit/livekit-server:v1.11.0 + livekit/egress:latest pulled, and the
#         publisher image built from Dockerfile.publisher-go:
#           docker build -f scripts/bench/Dockerfile.publisher-go -t lk-rs-bench-publisher-go:latest .

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
STACK=""
DURATION=20
RUNS=3
STREAMS=1
FORMAT=wav

usage() {
  cat <<'EOF'
usage: bench_egress.sh --stack rust|go [--seconds N] [--runs N] [--streams N] [--format wav|mp3]
EOF
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --stack) STACK="$2"; shift 2 ;;
    --seconds) DURATION="$2"; shift 2 ;;
    --runs) RUNS="$2"; shift 2 ;;
    --streams) STREAMS="$2"; shift 2 ;;
    --format) FORMAT="$2"; shift 2 ;;
    *) usage ;;
  esac
done

[[ "$STACK" == rust || "$STACK" == go ]] || usage

API_KEY=devkey
API_SECRET=secret
FILE_TYPE=0 # EncodedFileType::DEFAULT -> WAV
[[ "$FORMAT" == mp3 ]] && FILE_TYPE=2

# --- per-stack configuration ------------------------------------------------
case "$STACK" in
  rust)
    PORT=7991
    WS_URL=ws://server:7880
    OUT_DIR="$HERE/out-rust"
    PROJECT="lkbench-rust"
    NETWORK="${PROJECT}_default"
    EGRESS_CONTAINER="$PROJECT-egress-1"
    PUBLISHER_IMAGE=lk-rs-bench-publisher:latest
    ;;
  go)
    PORT=7990
    WS_URL=ws://server:7880
    OUT_DIR="$HERE/out-go"
    PROJECT="lkbench-go"
    NETWORK="${PROJECT}_default"
    EGRESS_CONTAINER="$PROJECT-egress-1"
    PUBLISHER_IMAGE=lk-rs-bench-publisher-go:latest
    ;;
esac

now_ms() {
  python3 -c "import time; print(int(time.time() * 1000))"
}

# A roomRecord-granted JWT for the Twirp Egress API.
record_token() {
  python3 - "$API_KEY" "$API_SECRET" <<'PY'
import base64, hashlib, hmac, json, sys, time
key, secret = sys.argv[1], sys.argv[2]
def b64(b): return base64.urlsafe_b64encode(b).rstrip(b"=")
now = int(time.time())
h = b64(json.dumps({"alg":"HS256","typ":"JWT"}).encode())
p = b64(json.dumps({"iss":key,"sub":"admin","iat":now,"nbf":now-5,"exp":now+3600,
                    "video":{"roomRecord":True}}).encode())
s = b64(hmac.new(secret.encode(), h+b"."+p, hashlib.sha256).digest())
print((h+b"."+p+b"."+s).decode())
PY
}

twirp() { # $1 method, $2 json body
  curl -s -X POST "http://127.0.0.1:$PORT/twirp/livekit.Egress/$1" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $(record_token)" \
    -d "$2"
}

wait_tcp() {
  local host=$1 port=$2 name=$3
  for _ in $(seq 1 60); do
    if nc -z -w 1 "$host" "$port" 2>/dev/null; then return 0; fi
    sleep 1
  done
  echo "timed out waiting for $name on $host:$port" >&2
  return 1
}

# --- stack lifecycle ---------------------------------------------------------
start_stack() {
  local compose_file="$HERE/$STACK-stack.yml"
  docker compose -p "$PROJECT" -f "$compose_file" up -d --wait \
    redis server egress 2>&1 | sed 's/^/  compose: /'
  wait_tcp 127.0.0.1 "$PORT" "server"
  local marker
  [[ "$STACK" == go ]] && marker="service ready" || marker="livekit-egress started"
  for _ in $(seq 1 60); do
    docker logs "$EGRESS_CONTAINER" 2>&1 | grep -q "$marker" && return 0
    sleep 1
  done
  echo "egress did not become ready" >&2
  return 1
}

stop_stack() {
  docker compose -p "$PROJECT" -f "$HERE/$STACK-stack.yml" down 2>&1 | sed 's/^/  compose: /'
}

# --- publisher management ----------------------------------------------------
start_publishers() { # $1 run index, $2 room
  local run=$1 room=$2 i
  stop_publishers
  for i in $(seq 1 "$STREAMS"); do
    docker run -d --rm --network "$NETWORK" --name "$PROJECT-pub-$run-$i" \
      "$PUBLISHER_IMAGE" --ws "$WS_URL" --key "$API_KEY" \
      --secret "$API_SECRET" --room "$room" --seconds $((DURATION + 8)) >/dev/null
  done
}

stop_publishers() {
  docker ps --filter "name=$PROJECT-pub-" --format '{{.Names}}' | \
    xargs -r -n1 docker rm -f >/dev/null 2>&1 || true
}

# --- sampling ----------------------------------------------------------------
start_sampler() { # $1 label, $2 stop-file, $3 out-file; sets SAMPLER_PID
  local label=$1 stop_file=$2 out_file=$3
  python3 "$HERE/sample_container.py" --container "$EGRESS_CONTAINER" \
    --duration $((DURATION + 20)) --label "$label" --stop-file "$stop_file" \
    > "$out_file" 2>&1 &
  SAMPLER_PID=$!
}

# Polls ListEgress until the recording reaches a terminal state; echoes it.
poll_egress() { # $1 room, $2 egress_id
  local room=$1 eid=$2 status
  for _ in $(seq 1 120); do
    status=$(twirp ListEgress "{\"roomName\":\"$room\"}" | \
      python3 -c "import sys,json;d=json.load(sys.stdin);print(next((e.get('status') for e in d.get('items',[]) if (e.get('egressId') or e.get('egress_id'))=='$eid'),'UNKNOWN'))" 2>/dev/null || echo UNKNOWN)
    case "$status" in
      EGRESS_COMPLETE|EGRESS_FAILED) echo "$status"; return 0 ;;
    esac
    sleep 0.5
  done
  echo "EGRESS_TIMEOUT"
}

describe_recording() { # $1 path
  python3 - "$1" <<'PY'
import struct, sys, os
p = sys.argv[1]
b = open(p, 'rb').read()
size = os.path.getsize(p)
if b[:4] == b'RIFF' and b[8:12] == b'WAVE':
    rate = struct.unpack('<I', b[24:28])[0]
    ch = struct.unpack('<H', b[22:24])[0]
    bits = struct.unpack('<H', b[34:36])[0]
    data = struct.unpack('<I', b[40:44])[0]
    dur = data / (rate * ch * (bits // 8)) if rate * ch * (bits // 8) else 0
    print(f"  output: wav {size}B {dur:.1f}s {rate}Hz {ch}ch {bits}bit")
else:
    print(f"  output: {os.path.basename(p)} {size}B (non-WAV)")
PY
}

run_once() { # $1 = run index (1-based)
  local run=$1 room="bench-$1" egress_id t_start t_done status
  local out_sample="$OUT_DIR/sample-run-$run.txt"
  local stop_file="$OUT_DIR/run-$run.done"

  rm -f "$stop_file"
  mkdir -p "$OUT_DIR"

  start_publishers "$run" "$room"
  # Wait for the room and the audio track to be established.
  sleep 4

  t_start=$(now_ms)
  egress_id=$(twirp StartRoomCompositeEgress \
    "{\"roomName\":\"$room\",\"audioOnly\":true,\"fileOutputs\":[{\"fileType\":$FILE_TYPE,\"filepath\":\"/out/rec\"}]}" | \
    python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('egressId') or d.get('egress_id',''))" 2>/dev/null || true)
  if [[ -z "$egress_id" ]]; then
    echo "run $run: failed to start egress" >&2
    return 1
  fi

  echo "run $run: recording $room ($STACK, ${DURATION}s, ${STREAMS} stream(s))"

  start_sampler "run-$run" "$stop_file" "$out_sample"
  local sample_pid=$SAMPLER_PID

  # Let it record for DURATION seconds, then stop it through the API.
  sleep "$DURATION"
  twirp StopEgress "{\"egressId\":\"$egress_id\"}" > /dev/null || true
  status=$(poll_egress "$room" "$egress_id")

  touch "$stop_file"
  wait "$sample_pid" || true
  t_done=$(now_ms)

  stop_publishers

  echo "run $run: $status, wall $(( (t_done - t_start) / 1000 ))s, egress_id=$egress_id"
  grep '^summary ' "$out_sample" || true
  local f
  for f in "$OUT_DIR"/rec.* "$OUT_DIR"/*.wav "$OUT_DIR"/*.ogg "$OUT_DIR"/*.mp3; do
    [[ -f "$f" ]] && describe_recording "$f"
  done
  rm -f "$OUT_DIR"/rec.* "$OUT_DIR"/*.wav "$OUT_DIR"/*.ogg "$OUT_DIR"/*.mp3
}

measure_idle() {
  local out_sample="$OUT_DIR/sample-idle.txt"
  local stop_file="$OUT_DIR/idle.done"
  rm -f "$stop_file"
  echo "== measuring idle footprint (10s, no recording) =="
  start_sampler "idle" "$stop_file" "$out_sample"
  local sample_pid=$SAMPLER_PID
  sleep 10
  touch "$stop_file"
  wait "$sample_pid" || true
  grep '^summary ' "$out_sample" || true
}

main() {
  start_stack
  echo "== $STACK egress benchmark: ${RUNS} runs x ${DURATION}s, ${STREAMS} stream(s), format=$FORMAT =="
  measure_idle
  for r in $(seq 1 "$RUNS"); do
    run_once "$r"
  done
  echo "== done =="
  stop_stack
}

main "$@"
