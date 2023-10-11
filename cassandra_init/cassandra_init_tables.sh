#!/usr/bin/env bash

until printf "" 2>>/dev/null >>/dev/tcp/cassandra/9042; do 
    sleep 5;
    echo "Waiting for cassandra...";
done

echo "Creating keyspace and table..."
cqlsh cassandra -u cassandra -p cassandra -e "CREATE KEYSPACE IF NOT EXISTS db WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};"
cqlsh cassandra -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS db.orders (id TEXT PRIMARY KEY, line INT, order_date DATE, customer_id TEXT, product_id TEXT, quantity INT);"
cqlsh cassandra -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS db.bikeshops (id TEXT PRIMARY KEY, name TEXT, city TEXT, state TEXT, latitude DECIMAL, longitude DECIMAL);"
cqlsh cassandra -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS db.bikes (id TEXT PRIMARY KEY, model TEXT, category1 TEXT, category2 TEXT,frame TEXT, price DECIMAL);"