#!/bin/sh

# Loop until safe mode is OFF
while true; do
    output=$(docker exec projet_fin_etude-hdfs-namenode-1 /usr/local/hadoop/bin/hdfs dfsadmin -safemode get)
    
    if echo "$output" | grep -q "Safe mode is OFF"; then
        echo "Safe mode is OFF, continuing..."
        break
    else
        echo "Safe mode is ON, waiting..."
        sleep 30
    fi
done

# Add your further commands here
echo "Adding spark Writing permissions to hdfs" 
docker exec projet_fin_etude-hdfs-namenode-1 /usr/local/hadoop/bin/hdfs dfs -chown -R spark:spark /

echo "All Preparations are done" 
echo "Starting Spark Cluster .. " 