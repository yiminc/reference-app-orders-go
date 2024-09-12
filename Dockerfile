# Use a scratch image since the binary is already statically compiled
FROM scratch AS oms-worker

# Copy the pre-built binary into the image
COPY ./oms /usr/local/bin/oms

# Run the worker process
ENTRYPOINT ["/usr/local/bin/oms", "worker"]

# ---------------------------------

# Use a scratch image for the API server
FROM scratch AS oms-api

# Expose ports for the API server
EXPOSE 8081
EXPOSE 8082
EXPOSE 8083
EXPOSE 8084

# Create a volume for data persistence
VOLUME /data
ENV DATA_DIR=/data

# Copy the pre-built binary into the image
COPY ./oms /usr/local/bin/oms

# Run the API process
ENTRYPOINT ["/usr/local/bin/oms", "api"]

# ---------------------------------

# Use a scratch image for the codec server
FROM scratch AS oms-codec-server

# Expose port for the codec server
EXPOSE 8089

# Copy the pre-built binary into the image
COPY ./oms /usr/local/bin/oms

# Run the codec server process
ENTRYPOINT ["/usr/local/bin/oms", "codec-server"]