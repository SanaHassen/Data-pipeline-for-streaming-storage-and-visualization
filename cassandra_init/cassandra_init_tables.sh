#!/bin/sh

# Define variables
CASSANDRA_CONTAINER="projet_fin_etude-cassandra-1"
KEYSPACE="default"

# Function to create keyspace
create_keyspace() {
    docker exec $CASSANDRA_CONTAINER cqlsh -e "CREATE KEYSPACE IF NOT EXISTS $KEYSPACE WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
}

# Function to create tables
create_tables() {
  docker exec $CASSANDRA_CONTAINER cqlsh cassandra -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS default.orders (id TEXT PRIMARY KEY, order_id INT, order_line INT, order_date DATE, customer_id INT, product_id INT, quantity INT);"
  docker exec $CASSANDRA_CONTAINER cqlsh cassandra -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS default.bikeshops (id TEXT PRIMARY KEY, name TEXT, city TEXT, state TEXT, latitude DECIMAL, longitude DECIMAL);"
  docker exec $CASSANDRA_CONTAINER cqlsh cassandra -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS default.bikes (id TEXT PRIMARY KEY, model TEXT, category1 TEXT, category2 TEXT,frame TEXT, price DECIMAL);"
}

# Loop until successful connection
connected=false
while [ "$connected" != true ]; do
    docker exec $CASSANDRA_CONTAINER cqlsh -e "DESCRIBE KEYSPACES;"
    if [ $? -eq 0 ]; then
        connected=true
        echo "Connected to Cassandra"
    else
        echo "Failed to connect to Cassandra, retrying..."
        sleep 5
    fi
done

# Create keyspace and tables
create_keyspace
create_tables

echo "Keyspace and tables created successfully"



