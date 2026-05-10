# postgre JDBC driverı indirir. nifi yın DBCPConnectionPool için lazım. 3 gün önce wpden anlattığım şey. 1kez çalıştırcaz bunu

set -euo pipefail

DRIVER_DIR="$(dirname "$0")"
DRIVER_VERSION="42.7.3"
DRIVER_FILE="postgresql-${DRIVER_VERSION}.jar"
DRIVER_URL="https://jdbc.postgresql.org/download/${DRIVER_FILE}"

if [ -f "${DRIVER_DIR}/${DRIVER_FILE}" ]; then
    echo "Driver zaten var: ${DRIVER_DIR}/${DRIVER_FILE}"
    exit 0
fi

echo "Postgres JDBC driver indiriliyor..."
curl -fL -o "${DRIVER_DIR}/${DRIVER_FILE}" "${DRIVER_URL}"
echo "Hazır: ${DRIVER_DIR}/${DRIVER_FILE}"
echo ""
echo "NiFi'yi yeniden başlat: docker compose restart nifi"
