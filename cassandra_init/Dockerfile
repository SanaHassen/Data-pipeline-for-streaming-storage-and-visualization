# Use an image with Docker and necessary tools
FROM docker:latest

# Copy your script into the container
COPY cassandra_init_tables.sh /cassandra_init_tables.sh

# Make the script executable
RUN chmod +x /cassandra_init_tables.sh

# Run the script
CMD ["sh", "-c", "/cassandra_init_tables.sh"]
# CMD ["/cassandra_init_tables.sh"]