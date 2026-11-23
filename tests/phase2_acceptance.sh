#!/usr/bin/env bash
# Phase 2 acceptance (docs/spark_usermanagement_design.md §7 Phase 2).
# Creates/uses test users alice and bob; every Spark action runs inside their
# own Notebook Pods with their own spark-runner tokens.
#   tests/phase2_acceptance.sh            run all tests, keep alice/bob
#   tests/phase2_acceptance.sh --cleanup  afterwards delete alice and bob
set -u
source "$(dirname "$0")/lib.sh"
CLEANUP=0; [ "${1:-}" = "--cleanup" ] && CLEANUP=1

admin_login
echo "### setup: alice and bob running"
ensure_user_running alice alicepw12
ensure_user_running bob bobpw1234
$KC -n user-alice get pod jupyter-alice -o jsonpath='    alice pod ip={.status.podIP} sa={.spec.serviceAccountName}{"\n"}'

# ---------------------------------------------------------------- 2.1
echo "### 2.1 alice and bob run 02 (Client Mode) at the same time"
# Record the namespace of every Executor Pod seen while the notebooks run.
# (The subshell writes the file itself so that $! is the loop, not a pipe stage.)
( for i in $(seq 1 90); do $KC get pods -A -l spark-role=executor --no-headers 2>/dev/null \
      | awk '{print $1}'; sleep 1; done > /tmp/p2-exec-ns.raw ) &
W=$!
run_notebook alice 02_wordcount_executor.ipynb > /tmp/p2-alice-02.txt 2>&1 &
A=$!
run_notebook bob 02_wordcount_executor.ipynb > /tmp/p2-bob-02.txt 2>&1 &
Bp=$!
wait $A $Bp; kill $W 2>/dev/null; wait $W 2>/dev/null
sort -u /tmp/p2-exec-ns.raw > /tmp/p2-exec-ns.txt
for u in alice bob; do
  f=/tmp/p2-$u-02.txt
  ns=$(sed -n 's/^namespace  : //p' "$f")
  tasks=$(sed -n 's/^tasks ran on: //p' "$f")
  if [ "$ns" = "user-$u" ] && echo "$tasks" | grep -q "exec-1" && ! grep -q NOTEBOOK-ERROR "$f" \
     && grep -q "('spark', 3)" "$f"; then
    pass "2.1 $u: namespace=$ns tasks on $tasks"
  else
    fail "2.1 $u:"; cat "$f"
  fi
done
echo "    namespaces that had Executor Pods: $(tr '\n' ' ' < /tmp/p2-exec-ns.txt)"
[ "$(sort -u /tmp/p2-exec-ns.txt | tr '\n' ' ')" = "user-alice user-bob " ] \
  && pass "2.1 Executors only in user-alice and user-bob" || fail "2.1 executor namespaces"

# ---------------------------------------------------------------- 2.2
echo "### 2.2 two Client-Mode Drivers in the same Notebook Pod (alice)"
in_nb alice 'cd /tmp; for i in 1 2; do WORDCOUNT_EXECUTORS=1 python3 ~/work/examples/jobs/wordcount_executor.py > /tmp/drv$i.log 2>/tmp/drv$i.err & done; wait
for i in 1 2; do echo "driver $i: $(grep -h "Tasks ran on" /tmp/drv$i.log)"; done' > /tmp/p2-22.txt 2>&1
cat /tmp/p2-22.txt | sed 's/^/    /'
[ "$(grep -c "exec-1" /tmp/p2-22.txt)" = 2 ] && pass "2.2 both Drivers got their own Executor" || fail "2.2"

# ---------------------------------------------------------------- 2.3 + 2.5 (driver UI)
echo "### 2.3 alice: Cluster Mode with S3A (shared input -> users/alice/)"
in_nb alice '/opt/spark/bin/spark-submit --master k8s://https://kubernetes.default.svc:443 --deploy-mode cluster \
  --name wordcount-cluster \
  --conf spark.kubernetes.namespace=$SPARK_LAB_NAMESPACE \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-runner \
  --conf spark.kubernetes.container.image=$SPARK_LAB_RUNTIME_IMAGE \
  --conf spark.kubernetes.container.image.pullPolicy=Never \
  --conf spark.kubernetes.driverEnv.SILO_ENDPOINT=$SILO_ENDPOINT \
  --conf spark.kubernetes.driver.secretKeyRef.SILO_ACCESS_KEY=silo-shared-credentials:accessKey \
  --conf spark.kubernetes.driver.secretKeyRef.SILO_SECRET_KEY=silo-shared-credentials:secretKey \
  --conf spark.kubernetes.driverEnv.SILO_INPUT=s3a://spark-data/shared/wordcount/ \
  --conf spark.kubernetes.driverEnv.SILO_OUTPUT=s3a://spark-data/users/$SPARK_USER/cluster-wordcount/output \
  --conf spark.kubernetes.driverEnv.WORDCOUNT_HOLD_SECONDS=30 \
  --conf spark.executor.instances=2 --conf spark.executor.memory=1g \
  --conf spark.kubernetes.executor.request.cores=500m --conf spark.kubernetes.driver.request.cores=500m \
  --conf spark.kubernetes.submission.waitAppCompletion=false \
  local:///opt/spark-lab/examples/jobs/wordcount.py' > /tmp/p2-23-submit.txt 2>&1
for i in $(seq 1 60); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' "$B/spark-driver/alice/api/v1/applications")" = 200 ] && break; sleep 2
done
D=$($KC -n user-alice get pods -l spark-role=driver --sort-by=.metadata.creationTimestamp -o name | tail -1)
dsa=$($KC -n user-alice get "$D" -o jsonpath='{.spec.serviceAccountName}')
nex=$($KC -n user-alice get pods -l spark-role=executor --no-headers 2>/dev/null | wc -l | tr -d ' ')
redir=$(curl -s -o /dev/null -w '%{redirect_url}' "$B/spark-driver/alice/")
links=$(curl -sL "$B/spark-driver/alice/" | grep -o 'href="/spark-driver/alice/[a-z]*/"' | sort -u | tr '\n' ' ')
echo "    driver=$D sa=$dsa executors-now=$nex"
echo "    /spark-driver/alice/ -> $redir ; links: $links"
[ "$dsa" = spark-runner ] && [ "$nex" -ge 1 ] && pass "2.3 Driver + Executors in user-alice, Driver SA spark-runner" || fail "2.3"
case "$redir" in */spark-driver/alice/jobs/) pass "2.5 /spark-driver/alice/ redirect keeps prefix";; *) fail "2.5 driver redirect: $redir";; esac
for i in $(seq 1 60); do
  ph=$($KC -n user-alice get "$D" -o jsonpath='{.status.phase}'); [ "$ph" = Succeeded ] || [ "$ph" = Failed ] && break; sleep 2
done
out=$($KC -n spark-lab exec deploy/silo -- sh -c 'export MC_HOST_local="http://$MINIO_ROOT_USER:$MINIO_ROOT_PASSWORD@127.0.0.1:9000"; mcli cat local/spark-data/users/alice/cluster-wordcount/output/part-00000-*.json 2>/dev/null; mcli ls --recursive local/spark-data/users/alice/cluster-wordcount/output/' 2>&1)
echo "    driver phase=$ph"; echo "$out" | sed 's/^/    /' | head -8
[ "$ph" = Succeeded ] && echo "$out" | grep -q "_SUCCESS" && pass "2.3 S3A output written by Executors to users/alice/" || fail "2.3 S3A output"

# ---------------------------------------------------------------- 2.4
echo "### 2.4 alice submits into user-bob"
in_nb alice '/opt/spark/bin/spark-submit --master k8s://https://kubernetes.default.svc:443 --deploy-mode cluster \
  --name sneaky --conf spark.kubernetes.namespace=user-bob \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-runner \
  --conf spark.kubernetes.container.image=$SPARK_LAB_RUNTIME_IMAGE \
  --conf spark.kubernetes.container.image.pullPolicy=Never \
  local:///opt/spark-lab/examples/jobs/wordcount.py' > /tmp/p2-24.txt 2>&1
msg=$(grep -m1 -o 'pods is forbidden[^.]*' /tmp/p2-24.txt)
bobdrv=$($KC -n user-bob get pods -l spark-app-name=sneaky --no-headers 2>/dev/null | wc -l | tr -d ' ')
echo "    $msg"
[ -n "$msg" ] && [ "$bobdrv" = 0 ] && pass "2.4 rejected by RBAC (403), nothing created in user-bob" || { fail "2.4"; tail -5 /tmp/p2-24.txt; }


# ---------------------------------------------------------------- 2.5 (notebook UIs)
echo "### 2.5 /spark-ui/alice/ and /spark-ui/bob/ while sessions are alive"
for u in alice bob; do
  in_nb "$u" "cd /tmp; nohup timeout 45 python3 -c 'from pyspark.sql import SparkSession; import time; s = SparkSession.builder.master(\"local[1]\").appName(\"ui-$u\").getOrCreate(); s.sparkContext.parallelize(range(3)).count(); time.sleep(40)' >/tmp/ui.log 2>&1 &"
done
for u in alice bob; do
  for i in $(seq 1 30); do
    curl -s "$B/spark-ui/$u/api/v1/applications" | grep -q "ui-$u" && break; sleep 1
  done
  redir=$(curl -s -o /dev/null -w '%{redirect_url}' "$B/spark-ui/$u/")
  app=$(curl -s "$B/spark-ui/$u/api/v1/applications" | sed -n 's/.*"name" : "\([^"]*\)".*/\1/p' | head -1)
  links=$(curl -sL "$B/spark-ui/$u/" | grep -o "href=\"/spark-ui/$u/[a-z]*/\"" | sort -u | wc -l | tr -d ' ')
  echo "    /spark-ui/$u/ -> $redir ; app=$app ; prefixed nav links=$links"
  case "$redir" in
    */spark-ui/$u/jobs/) [ "$app" = "ui-$u" ] && [ "$links" -ge 4 ] && pass "2.5 /spark-ui/$u/ shows $u's application" || fail "2.5 $u app=$app links=$links";;
    *) fail "2.5 $u redirect $redir";;
  esac
done

# ---------------------------------------------------------------- 2.6
echo "### 2.6 03 S3A notebook, local and k8s modes (alice)"
sleep 45   # let the 2.5 sessions release port 4040 / CPU
for mode in local k8s; do
  run_notebook alice 03_wordcount_s3a.ipynb "EXECUTORS = \"$mode\"" > "/tmp/p2-26-$mode.txt" 2>&1
  f=/tmp/p2-26-$mode.txt
  readers=$(grep -o 'from Silo on [^ ]*' "$f" | sort -u | tr '\n' ' ')
  echo "    $mode: $readers | $(grep -m1 '^output:' "$f")"
  if grep -q "NOTEBOOK-ERROR" "$f" || ! grep -q "^rows: " "$f"; then
    fail "2.6 $mode"; cat "$f"
  elif [ "$mode" = k8s ] && ! echo "$readers" | grep -q exec; then
    fail "2.6 k8s: Silo was not read by Executor Pods"
  else
    pass "2.6 $mode: read shared/, wrote users/alice/"
  fi
done

# ---------------------------------------------------------------- Pod Security, end to end
echo "### PSA: Spark hostPath on Executors (alice, Client Mode)"
in_nb alice 'cd /tmp; timeout 120 python3 - > /tmp/hp.out 2> /tmp/hp.err <<PY
import os, socket, time
from pyspark.sql import SparkSession
s = (SparkSession.builder.appName("hostpath-attempt").master("k8s://https://kubernetes.default.svc:443")
     .config("spark.submit.deployMode", "client")
     .config("spark.kubernetes.namespace", os.environ["SPARK_LAB_NAMESPACE"])
     .config("spark.driver.host", os.environ["SPARK_LAB_DRIVER_HOST"]).config("spark.driver.bindAddress", "0.0.0.0")
     .config("spark.kubernetes.driver.pod.name", socket.gethostname())
     .config("spark.kubernetes.container.image", os.environ["SPARK_LAB_RUNTIME_IMAGE"])
     .config("spark.kubernetes.container.image.pullPolicy", "Never")
     .config("spark.executor.instances", "1").config("spark.kubernetes.executor.request.cores", "500m")
     .config("spark.kubernetes.executor.volumes.hostPath.h.mount.path", "/host")
     .config("spark.kubernetes.executor.volumes.hostPath.h.options.path", "/")
     .getOrCreate())
time.sleep(30)
print("registered executors:", s.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1, flush=True)
s.stop()
PY
cat /tmp/hp.out; grep -o "violates PodSecurity \"baseline:v1.36\": hostPath volumes" /tmp/hp.err | head -1
echo "rejected creation attempts: $(grep -c "violates PodSecurity" /tmp/hp.err)"' > /tmp/p2-psa.txt 2>&1
sed 's/^/    /' /tmp/p2-psa.txt
grep -q "registered executors: 0" /tmp/p2-psa.txt && grep -q "violates PodSecurity" /tmp/p2-psa.txt \
  && pass "PSA: Executor with hostPath rejected, no Executor started" || fail "PSA hostPath"

# ---------------------------------------------------------------- cleanup
$KC -n user-alice delete pod -l spark-role --ignore-not-found --wait=false >/dev/null 2>&1
if [ "$CLEANUP" -eq 1 ]; then
  echo "### cleanup: delete alice and bob"
  for u in alice bob; do echo "    delete $u: $(delete_user "$u")"; done
fi
summary
