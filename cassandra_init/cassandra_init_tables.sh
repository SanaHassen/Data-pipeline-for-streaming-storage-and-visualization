#!/bin/bash


echo 'aaa'
# # Define variables
# CASSANDRA_CONTAINER="projet_fin_etude-cassandra-1"
# KEYSPACE="default"

# # Function to create keyspace
# create_keyspace() {
#     docker exec $CASSANDRA_CONTAINER cqlsh -e "CREATE KEYSPACE IF NOT EXISTS $KEYSPACE WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
# }

# # Function to create tables
# create_tables() {
#     docker exec $CASSANDRA_CONTAINER cqlsh -e "USE $KEYSPACE; CREATE TABLE IF NOT EXISTS bikes (id UUID PRIMARY KEY, name text, price double);"
#     docker exec $CASSANDRA_CONTAINER cqlsh -e "USE $KEYSPACE; CREATE TABLE IF NOT EXISTS bikeshops (id UUID PRIMARY KEY, name text, location text);"
#     docker exec $CASSANDRA_CONTAINER cqlsh -e "USE $KEYSPACE; CREATE TABLE IF NOT EXISTS orders (id UUID PRIMARY KEY, bike_id UUID, bikeshop_id UUID, quantity int);"
# }

# # Loop until successful connection
# connected=false
# while [ "$connected" != true ]; do
#     docker exec $CASSANDRA_CONTAINER cqlsh -e "DESCRIBE KEYSPACES;"
#     if [ $? -eq 0 ]; then
#         connected=true
#         echo "Connected to Cassandra"
#     else
#         echo "Failed to connect to Cassandra, retrying..."
#         sleep 5
#     fi
# done

# # Create keyspace and tables
# create_keyspace
# create_tables

# echo "Keyspace and tables created successfully"
