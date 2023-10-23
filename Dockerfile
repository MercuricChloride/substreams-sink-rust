# Use the official Rust image as the base image
FROM rust:1.70 as builder

# Set the working directory in the Docker image
WORKDIR /usr/src/geo-substream-sink

# Copy main, entity, sink, and migration Cargo.toml and Cargo.lock files
COPY Cargo.toml Cargo.lock ./
COPY entity/Cargo.toml entity/
COPY sink/Cargo.toml sink/
COPY migration/Cargo.toml migration/

# Dummy build to cache dependencies
RUN mkdir -p src entity/src sink/src migration/src && \
    echo "fn main() {}" > src/main.rs && \
    echo "fn main() {}" > entity/src/main.rs && \
    echo "fn main() {}" > sink/src/main.rs && \
    echo "fn main() {}" > migration/src/main.rs && \
    cargo build --release && \
    rm -r src entity/src sink/src migration/src

RUN cargo install sea-orm-cli

# Now copy the entire project
COPY . .

# Build the project. This step will now only rebuild if something other than Cargo.toml and Cargo.lock has changed.
RUN cargo build --release

ARG SUBSTREAMS_ENDPOINT
ENV SUBSTREAMS_ENDPOINT=$SUBSTREAMS_ENDPOINT

ARG DATABASE_URL
ENV DATABASE_URL=$DATABASE_URL

ARG STREAMINGFAST_API_KEY
ENV STREAMINGFAST_API_KEY=$STREAMINGFAST_API_KEY

ARG PINAX_API_KEY
ENV PINAX_API_KEY=$PINAX_API_KEY

ARG MAX_CONNECTIONS
ENV MAX_CONNECTIONS=$MAX_CONNECTIONS

# Create a new stage with a smaller base image to reduce final image size
FROM debian:bullseye-slim

# Install libssl1.1
# Combine update, install, and cleanup in a single step for efficiency
RUN apt-get update && apt-get install -y libssl1.1 curl jq && rm -rf /var/lib/apt/lists/*

# Set the working directory in the Docker image
WORKDIR /usr/local/bin

# Copy the binary from the builder stage to the current stage
COPY --from=builder /usr/src/geo-substream-sink/target/release/geo-substream-sink .
COPY --from=builder /usr/local/cargo/bin/sea-orm-cli /usr/local/bin/

COPY substream.spkg . 
COPY entrypoint.sh .

ENTRYPOINT ["./entrypoint.sh"]
CMD ["deploy"]
