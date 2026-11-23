# Helpers for the multi-user acceptance tests. Source, do not execute.
# shellcheck shell=bash
# Drives JupyterHub over HTTP the way a browser does (signup, approve, login,
# spawn) and runs commands inside a user's Notebook Pod.
B="${LAB_URL:-http://localhost:8080}"
KC="kubectl --context docker-desktop"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/.lab/credentials.env"

PASS=0; FAIL=0
pass() { echo "PASS $*"; PASS=$((PASS + 1)); }
fail() { echo "FAIL $*"; FAIL=$((FAIL + 1)); }
summary() { echo "== $PASS passed, $FAIL failed"; [ "$FAIL" -eq 0 ]; }

xsrf_of() { # jar url -> the _xsrf form value of a page
  curl -s -c "$1" -b "$1" "$2" | sed -n 's/.*name="_xsrf" value="\([^"]*\)".*/\1/p' | head -1
}
cookie_xsrf() { awk '/\t_xsrf\t/{v=$7} END{print v}' "$1"; }

hub_login() { # jar user password -> HTTP code (302 = success)
  local x; x=$(xsrf_of "$1" "$B/hub/login")
  curl -s -c "$1" -b "$1" -o /dev/null -w '%{http_code}' \
    --data-urlencode "_xsrf=$x" --data-urlencode "username=$2" \
    --data-urlencode "password=$3" "$B/hub/login?next=%2Fhub%2Fhome"
}

hub_signup() { # jar user password
  local x; x=$(xsrf_of "$1" "$B/hub/signup")
  curl -s -c "$1" -b "$1" -o /dev/null \
    --data-urlencode "_xsrf=$x" --data-urlencode "username=$2" \
    --data-urlencode "signup_password=$3" \
    --data-urlencode "signup_password_confirmation=$3" "$B/hub/signup"
}

ADMIN_JAR=$(mktemp)
admin_login() { hub_login "$ADMIN_JAR" "$JUPYTER_USER" "$JUPYTER_PASSWORD" >/dev/null; }

# Create (if needed), approve, log in and start the server of a test user.
# Waits until jupyter-<user> in user-<user> is Ready.
ensure_user_running() { # user password
  local u=$1 pw=$2 jar
  jar=$(mktemp)
  hub_signup "$jar" "$u" "$pw"
  # /hub/authorize/<user> TOGGLES approval, so only call it when not approved yet.
  if [ "$(hub_login "$jar" "$u" "$pw")" != 302 ]; then
    curl -s -b "$ADMIN_JAR" -o /dev/null "$B/hub/authorize/$u"
    hub_login "$jar" "$u" "$pw" >/dev/null
  fi
  curl -s -b "$jar" -c "$jar" -o /dev/null -L "$B/hub/spawn"
  local i
  for i in $(seq 1 90); do
    $KC -n "user-$u" get pod "jupyter-$u" >/dev/null 2>&1 && break
    sleep 2
  done
  $KC -n "user-$u" wait --for=condition=Ready "pod/jupyter-$u" --timeout=180s >/dev/null
}

# Stop the server and delete the user through the Hub admin API.
delete_user() { # user
  local x; x=$(cookie_xsrf "$ADMIN_JAR")
  curl -s -b "$ADMIN_JAR" -X DELETE -o /dev/null "$B/hub/api/users/$1/server?_xsrf=$x"
  local i
  for i in $(seq 1 30); do
    $KC -n "user-$1" get pod "jupyter-$1" >/dev/null 2>&1 || break
    sleep 2
  done
  curl -s -b "$ADMIN_JAR" -X DELETE -o /dev/null -w '%{http_code}' "$B/hub/api/users/$1?_xsrf=$x"
}

# Run a shell command as the user inside their Notebook container.
in_nb() { # user command...
  local u=$1; shift
  $KC -n "user-$u" exec "jupyter-$u" -c notebook -- bash -lc "$*"
}

# Execute a notebook from the examples folder inside the user's Pod and print
# its stdout and errors. Extra args: python statements replacing cell 1
# (e.g. EXECUTORS = "k8s").
run_notebook() { # user notebook-file [cell1-override]
  local u=$1 nb=$2 override=${3:-}
  in_nb "$u" "cd /tmp && python3 - <<'PY'
import json
nb = json.load(open('/home/jovyan/work/examples/notebooks/$nb'))
if '''$override''':
    nb['cells'][1]['source'] = ['''$override''']
json.dump(nb, open('/tmp/run-$nb', 'w'))
PY
jupyter nbconvert --to notebook --execute --ExecutePreprocessor.timeout=300 \
  --output /tmp/out-$nb /tmp/run-$nb >/dev/null 2>&1
python3 - <<'PY'
import json
for c in json.load(open('/tmp/out-$nb'))['cells']:
    for o in c.get('outputs', []):
        if o.get('name') == 'stdout':
            print(''.join(o['text']).rstrip())
        if o.get('output_type') == 'error':
            print('NOTEBOOK-ERROR', o['ename'], o['evalue'][:300])
PY"
}
