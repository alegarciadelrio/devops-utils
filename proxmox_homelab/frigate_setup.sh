# Create necessary directories
cp -r frigate /opt
cp -r mqtt /opt

cd /opt/mqtt/
docker compose down
cd /opt/frigate/
docker compose down

# Run mqtt and frigate
cd /opt/mqtt/
docker compose up -d
cd /opt/frigate/
docker compose up -d