# Create necessary directories
cp -r frigate /opt
cp -r mqtt /opt

# Run mqtt and frigate
cd /opt/mqtt/
docker compose up -d
cd /opt/frigate/
docker compose up -d