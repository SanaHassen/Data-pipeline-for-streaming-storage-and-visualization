#!/bin/sh



SUPERSET_CONTAINER="projet_fin_etude-superset-1"



# Wait for Superset to start
until docker exec $SUPERSET_CONTAINER /usr/local/bin/superset db upgrade; do
  >&2 echo "Superset is unavailable - sleeping"
  sleep 1
done



echo "Superset is adding the Admin User ";
echo "Superset is adding the Admin User ";
echo "Superset is adding the Admin User ";
echo "Superset is adding the Admin User ";
docker exec $SUPERSET_CONTAINER /usr/local/bin/superset fab create-admin --username admin --password admin --firstname admin --lastname user --email admin@superset.com;

echo "Superset is doing init ";
echo "Superset is doing init ";
echo "Superset is doing init ";
echo "Superset is doing init ";
docker exec $SUPERSET_CONTAINER /usr/local/bin/superset init;




