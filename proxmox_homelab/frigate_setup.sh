# Create necessary directories
mkdir -p /opt/frigate/config /opt/mqtt/config



# Run mqtt and frigate
cd /opt/mqtt/
docker compose up -d
cd /opt/frigate/
docker compose up -d