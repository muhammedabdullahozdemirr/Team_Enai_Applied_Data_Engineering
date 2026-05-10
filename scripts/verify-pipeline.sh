
#pipeline doğrulama. nifi flowunu kurduktn sonra çalıştırın.

set -euo pipefail

echo "=================================================="
echo "Day 2 Pipeline Doğrulama"
echo "=================================================="

echo ""
echo "1. Servis durumları:"
docker compose ps --format "table {{.Name}}\t{{.Status}}"

echo ""
echo "2. Elasticsearch'te ecommerce-events index:"
ES_COUNT=$(curl -fsS "http://localhost:9200/ecommerce-events/_count" 2>/dev/null | python3 -c "import sys,json;print(json.load(sys.stdin).get('count', 'N/A'))" 2>/dev/null || echo "INDEX YOK")
echo "  Toplam doc: ${ES_COUNT}"

echo ""
echo "3. ES'ten örnek event:"
curl -fsS "http://localhost:9200/ecommerce-events/_search?size=1&pretty" 2>/dev/null | head -40 || echo "  (henüz veri yok)"

echo ""
echo "4. Dead letter events (Postgres):"
docker compose exec -T postgres psql -U ecom_user -d ecommerce -c \
    "SELECT error_type, COUNT(*) FROM ecommerce.dead_letter_events GROUP BY error_type ORDER BY COUNT(*) DESC;" 2>/dev/null \
    || echo "  (DLQ tablosu boş veya erişilemiyor)"

echo ""
echo "5. Sayım kontrolü:"
DLQ_COUNT=$(docker compose exec -T postgres psql -U ecom_user -d ecommerce -tA -c \
    "SELECT COUNT(*) FROM ecommerce.dead_letter_events;" 2>/dev/null | tr -d '[:space:]')
echo "  ES events:     ${ES_COUNT}"
echo "  PG dead-letter: ${DLQ_COUNT}"
if [ "${ES_COUNT}" != "N/A" ] && [ "${ES_COUNT}" != "INDEX YOK" ] && [ -n "${DLQ_COUNT}" ]; then
    TOTAL=$((ES_COUNT + DLQ_COUNT))
    echo "  Toplam:        ${TOTAL}  (beklenen ~30000)"
fi

echo ""
echo "Eğer ES count 0 ise, NiFi flow henüz başlamamış demektir."

