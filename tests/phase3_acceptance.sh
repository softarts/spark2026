#!/usr/bin/env bash
# Phase 3 acceptance (docs/spark_usermanagement_design.md §7 Phase 3):
# History Server, admin-only Headlamp, Portal entries, event-log cleanup.
#   tests/phase3_acceptance.sh            run all tests, keep alice/bob at the end
#   tests/phase3_acceptance.sh --cleanup  also delete bob at the end
# Test 3.4 deletes alice on purpose.
set -u
source "$(dirname "$0")/lib.sh"
CLEANUP=0; [ "${1:-}" = "--cleanup" ] && CLEANUP=1

hs_apps() { curl -s "$B/history/api/v1/applications?limit=1000"; }
# Application IDs in the History Server whose sparkUser is $1
hs_ids_of() {
  hs_apps | python3 -c '
import json, sys
u = sys.argv[1]
for a in json.load(sys.stdin):
    if any(t.get("sparkUser") == u for t in a["attempts"]):
        print(a["id"], a["name"])' "$1"
}
silo_ls() { $KC -n spark-lab exec deploy/silo -- sh -c \
  'export MC_HOST_local="http://$MINIO_ROOT_USER:$MINIO_ROOT_PASSWORD@127.0.0.1:9000"; mcli ls "local/spark-data/$0"' "$1" 2>/dev/null; }

admin_login
echo "### setup: alice and bob running"
ensure_user_running alice alicepw12
ensure_user_running bob bobpw1234

# ---------------------------------------------------------------- 3.1
echo "### 3.1 finished applications appear in the History Server with their user"
run_notebook alice 01_wordcount_local.ipynb > /tmp/p3-a01.txt 2>&1
in_nb alice 'WORDCOUNT_EXECUTORS=1 python3 ~/work/examples/jobs/wordcount_executor.py 2>/dev/null | grep "Tasks ran on"' | sed 's/^/    alice client: /'
run_notebook bob 01_wordcount_local.ipynb > /tmp/p3-b01.txt 2>&1
# Cluster Mode without any S3A settings on the command line: the Driver Pod
# must get the event-log credentials from spark-defaults (secretKeyRef).
in_nb alice '/opt/spark/bin/spark-submit --master k8s://https://kubernetes.default.svc:443 --deploy-mode cluster \
  --name wordcount-cluster-evlog \
  --conf spark.kubernetes.namespace=$SPARK_LAB_NAMESPACE \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-runner \
  --conf spark.kubernetes.container.image=$SPARK_LAB_RUNTIME_IMAGE \
  --conf spark.kubernetes.container.image.pullPolicy=Never \
  --conf spark.executor.instances=1 --conf spark.kubernetes.executor.request.cores=500m \
  --conf spark.kubernetes.driver.request.cores=500m \
  local:///opt/spark-lab/examples/jobs/wordcount.py' > /tmp/p3-cluster.txt 2>&1
grep -m1 -o "phase: Succeeded" /tmp/p3-cluster.txt | sed 's/^/    cluster: /'
for i in $(seq 1 30); do
  [ "$(hs_ids_of alice | wc -l)" -ge 3 ] && [ "$(hs_ids_of bob | wc -l)" -ge 1 ] && break; sleep 3
done
echo "    alice in History Server:"; hs_ids_of alice | sed 's/^/      /'
echo "    bob in History Server:";   hs_ids_of bob   | sed 's/^/      /'
an=$(hs_ids_of alice | awk '{print $2}' | sort -u | tr '\n' ' ')
case "$an" in
  *WordCount-Local*WordCount-K8s-Client*|*WordCount-K8s-Client*WordCount-Local*) ok_ab=1;; *) ok_ab=0;;
esac
[ "$ok_ab" = 1 ] && echo "$an" | grep -q "WordCount-Kubernetes-Cluster" \
  && pass "3.1 alice: Local, Client and Cluster Mode applications listed as sparkUser=alice" || fail "3.1 alice: $an"
[ "$(hs_ids_of bob | grep -c WordCount-Local)" -ge 1 ] && pass "3.1 bob: Local Mode application listed as sparkUser=bob" || fail "3.1 bob"

# Browse one application through the /history/ prefix.
APP=$(hs_ids_of alice | awk '$2=="WordCount-Local"{print $1; exit}')
redir=$(curl -s -o /dev/null -w '%{redirect_url}' "$B/history/history/$APP/")
code=$(curl -s -o /tmp/p3-app.html -w '%{http_code}' -L "$B/history/history/$APP/jobs/")
links=$(grep -o "href=\"/history/history/$APP/[a-z]*/\"" /tmp/p3-app.html | sort -u | wc -l | tr -d ' ')
echo "    /history/history/$APP/ -> $redir ; jobs page $code ; prefixed nav links=$links"
[ "$code" = 200 ] && [ "$links" -ge 4 ] && pass "3.1 application UI works behind /history/" || fail "3.1 app UI"

# ---------------------------------------------------------------- 3.2
echo "### 3.2 Headlamp: admin-only, read-only"
API="$B/headlamp/clusters/main/api/v1"
anon=$(curl -s -o /dev/null -w '%{http_code}' "$API/namespaces/user-alice/pods")
TOKEN=$("$ROOT/scripts/headlamp-token.sh" 10m)
pods=$(curl -s -H "Authorization: Bearer $TOKEN" "$API/pods" | python3 -c '
import json, sys
for p in json.load(sys.stdin)["items"]:
    ns = p["metadata"]["namespace"]
    if ns.startswith("user-"):
        print(ns + "/" + p["metadata"]["name"])' | sort)
sec=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $TOKEN" "$API/namespaces/user-alice/secrets")
del=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE -H "Authorization: Bearer $TOKEN" "$API/namespaces/user-alice/pods/jupyter-alice")
echo "    without token: $anon ; secrets with token: $sec ; delete pod with token: $del"
echo "    pods in user-* namespaces seen by the admin:"; echo "$pods" | sed 's/^/      /'
[ "$anon" = 401 ] || [ "$anon" = 403 ] && pass "3.2 no token -> $anon" || fail "3.2 anonymous got $anon"
echo "$pods" | grep -q "user-alice/jupyter-alice" && echo "$pods" | grep -q "user-bob/jupyter-bob" \
  && pass "3.2 admin sees every user's Pods by namespace" || fail "3.2 pod list"
[ "$sec" = 403 ] && [ "$del" = 403 ] && pass "3.2 token cannot read Secrets or delete Pods (403)" || fail "3.2 sec=$sec del=$del"
ha=$($KC auth can-i list pods -A --as=system:serviceaccount:headlamp:headlamp)
[ "$ha" = no ] && pass "3.2 Headlamp's own ServiceAccount has no permissions" || fail "3.2 headlamp SA: $ha"

# ---------------------------------------------------------------- 3.3
echo "### 3.3 Portal links"
page=$(curl -s "$B/")
for l in '/history/' '/headlamp/' '/spark-ui/{u}/' '/hub/signup'; do
  echo "$page" | grep -qF "$l" && pass "3.3 portal links $l" || fail "3.3 portal missing $l"
done

# ---------------------------------------------------------------- 3.4
echo "### 3.4 deleting alice removes her event logs, keeps bob's"
A_IDS=$(hs_ids_of alice | awk '{print $1}'); B_IDS=$(hs_ids_of bob | awk '{print $1}')
echo "    delete alice: $(delete_user alice)"
for i in $(seq 1 30); do
  $KC -n spark-lab get pod silo-cleanup-alice -o jsonpath='{.status.phase}' 2>/dev/null | grep -q Succeeded && break; sleep 2
done
logs=$(silo_ls spark-logs/)
left=0; for id in $A_IDS; do echo "$logs" | grep -q "$id" && left=$((left + 1)); done
kept=0; for id in $B_IDS; do echo "$logs" | grep -q "$id" && kept=$((kept + 1)); done
users=$(silo_ls users/ | awk '{print $NF}' | tr '\n' ' ')
echo "    alice logs left: $left of $(echo "$A_IDS" | wc -w | tr -d ' ') ; bob logs kept: $kept of $(echo "$B_IDS" | wc -w | tr -d ' ') ; users/: $users"
[ "$left" = 0 ] && [ "$kept" -ge 1 ] && pass "3.4 alice's event logs deleted, bob's kept" || fail "3.4 event logs"
echo "$users" | grep -q "alice/" && fail "3.4 users/alice/ still present" || pass "3.4 users/alice/ deleted"
$KC get ns user-alice >/dev/null 2>&1 && ! $KC get ns user-alice -o jsonpath='{.status.phase}' | grep -q Terminating \
  && fail "3.4 user-alice still present" || pass "3.4 user-alice namespace deleted"
$KC -n spark-lab delete pod silo-cleanup-alice --ignore-not-found >/dev/null 2>&1

if [ "$CLEANUP" -eq 1 ]; then
  echo "### cleanup: delete bob"; echo "    delete bob: $(delete_user bob)"
fi
summary
