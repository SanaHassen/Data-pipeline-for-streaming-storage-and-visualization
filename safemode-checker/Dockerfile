# Use an image with Docker and necessary tools
FROM docker:latest

# Copy your script into the container
COPY check_safemode.sh /check_safemode.sh

# Make the script executable
RUN chmod +x /check_safemode.sh

# Run the script
CMD ["sh", "-c", "/check_safemode.sh"]
