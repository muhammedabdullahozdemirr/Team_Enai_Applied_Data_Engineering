
# producer rate vs pipeline throughput + lag


set -uo pipefail

TOTAL_EVENTS=10000
RATES=(50 100 250 500 1000)
RESULTS_FILE="bench/results.csv"

mkdir -p bench

# csv header
echo "rate_target,total_events,producer_duration_sec,producer_actual_rate,pipeline_duration_sec,pipeline_throughput,pipeline_lag_sec,es_indexed,dlq_count" > "$RESULTS_FILE"

# .env backup
cp .env .env.bench-backup

for rate in "${RATES[@]}"; do
    echo ""
    echo "=================================================="
    echo "BENCH RUN: rate=${rate}/s, total=${TOTAL_EVENTS}"
    echo "=================================================="

    # 1. Reset environment
    echo "[1/7] Ortam temizleniyor..."
    curl -s -X DELETE "http://localhost:9200/ecommerce-events" > /dev/null
    docker compose exec -T postgres psql -U ecom_user -d ecommerce -c "TRUNCATE ecommerce.dead_letter_events RESTART IDENTITY;" > /dev/null
    docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --delete --topic ecommerce-events > /dev/null 2>&1
    sleep 2
    docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --topic ecommerce-events --partitions 3 --replication-factor 1 > /dev/null
    sleep 3

    # 2. .env güncelle
    echo "[2/7] Rate güncelleniyor: ${rate}/s, total: ${TOTAL_EVENTS}"
    sed -i.bak "s/^EVENTS_PER_SECOND=.*/EVENTS_PER_SECOND=${rate}/" .env
    sed -i.bak "s/^TOTAL_EVENTS=.*/TOTAL_EVENTS=${TOTAL_EVENTS}/" .env
    rm -f .env.bak

    # 3. Producer'ı çalıştır, süreyi ölç
    echo "[3/7] Producer başlatılıyor..."
    T_start=$(date +%s)
    docker compose up producer 2>&1 | tail -5
    T_producer_end=$(date +%s)
    producer_duration=$((T_producer_end - T_start))
    if [ "$producer_duration" -eq 0 ]; then producer_duration=1; fi
    producer_actual_rate=$(awk "BEGIN {printf \"%.2f\", $TOTAL_EVENTS / $producer_duration}")

    echo "[3/7] Producer bitti: ${producer_duration}s, actual rate: ${producer_actual_rate}/s"

    # 4. ES'e drain'i bekle (count stabilize olana kadar)
    echo "[4/7] ES drain bekleniyor..."
    last_count=-1
    stable_count=0
    pipeline_end=$T_producer_end
    while true; do
        sleep 2
        es_count=$(curl -s "http://localhost:9200/ecommerce-events/_count" | python3 -c "import sys,json;print(json.load(sys.stdin).get('count',0))" 2>/dev/null || echo 0)
        dlq_count=$(docker compose exec -T postgres psql -U ecom_user -d ecommerce -tA -c "SELECT COUNT(*) FROM ecommerce.dead_letter_events;" 2>/dev/null | tr -d '[:space:]' || echo 0)
        total_processed=$((es_count + dlq_count))

        if [ "$total_processed" -eq "$last_count" ]; then
            stable_count=$((stable_count + 1))
            if [ "$stable_count" -ge 3 ]; then
                pipeline_end=$(date +%s)
                break
            fi
        else
            stable_count=0
        fi
        last_count=$total_processed

        # 60 sn timeout
        elapsed=$(( $(date +%s) - T_producer_end ))
        if [ "$elapsed" -gt 60 ]; then
            echo "[4/7] TIMEOUT: drain 60s'de bitmedi, mevcut sayılarla devam"
            pipeline_end=$(date +%s)
            break
        fi
    done

    pipeline_duration=$((pipeline_end - T_start))
    pipeline_lag=$((pipeline_end - T_producer_end))
    if [ "$pipeline_duration" -eq 0 ]; then pipeline_duration=1; fi
    pipeline_throughput=$(awk "BEGIN {printf \"%.2f\", $total_processed / $pipeline_duration}")

    echo "[5/7] Pipeline drain bitti: ${pipeline_duration}s, throughput: ${pipeline_throughput}/s, lag: ${pipeline_lag}s"
    echo "[6/7] Final: ES=${es_count}, DLQ=${dlq_count}, total=${total_processed}"

    # 7. CSV'ye yaz
    echo "${rate},${TOTAL_EVENTS},${producer_duration},${producer_actual_rate},${pipeline_duration},${pipeline_throughput},${pipeline_lag},${es_count},${dlq_count}" >> "$RESULTS_FILE"
    echo "[7/7] CSV güncellendi: $RESULTS_FILE"

    # NiFi/Kafka için sleep
    sleep 5
done

# .env restore
mv .env.bench-backup .env
echo ""
echo "=================================================="
echo "BENCH TAMAM. Sonuçlar: $RESULTS_FILE"
echo "=================================================="
cat "$RESULTS_FILE"
