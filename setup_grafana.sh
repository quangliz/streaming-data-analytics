#!/bin/bash

# Wait for Grafana to be ready
echo "Waiting for Grafana to be ready..."
until $(curl --output /dev/null --silent --head --fail http://localhost:3000); do
    printf '.'
    sleep 5
done
echo "Grafana is up!"

# Install MongoDB plugin
echo "Installing MongoDB plugin for Grafana..."
docker-compose exec -T grafana grafana-cli plugins install grafana-mongodb-datasource
docker-compose restart grafana

# Wait for Grafana to restart
echo "Waiting for Grafana to restart..."
sleep 10
until $(curl --output /dev/null --silent --head --fail http://localhost:3000); do
    printf '.'
    sleep 5
done
echo "Grafana is up again!"

# Create MongoDB data source
echo "Creating MongoDB data source..."
curl -X POST -H "Content-Type: application/json" -d '{
  "name": "MongoDB",
  "type": "grafana-mongodb-datasource",
  "access": "proxy",
  "url": "mongodb://admin:admin@mongodb:27017",
  "database": "user_analytics",
  "isDefault": true,
  "jsonData": {
    "tlsSkipVerify": true
  }
}' -u admin:admin http://localhost:3000/api/datasources

echo "Setup complete! You can now access Grafana at http://localhost:3000 (admin/admin)"
echo "Create dashboards to visualize the data from MongoDB collections:"
echo "- gender_distribution"
echo "- age_distribution"
echo "- country_distribution"
echo "- user_inflow"
echo "- avg_age_by_country" 