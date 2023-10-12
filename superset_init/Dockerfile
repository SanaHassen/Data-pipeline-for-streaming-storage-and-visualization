# Use an image with Docker and necessary tools
FROM docker:latest

# Copy your script into the container
COPY superset_init.sh /superset_init.sh

# Make the script executable
RUN chmod +x /superset_init.sh

# Run the script
CMD ["sh", "-c", "/superset_init.sh"]
