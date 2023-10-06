# Use the official Rust image as the base image
FROM rust:1.70 as builder

# Set the working directory in the Docker image
WORKDIR /usr/src/geo-substream-sink

# Copy the entire project to the working directory
COPY . .


# Build the project. This step will install all dependencies and compile the project
RUN cargo build --release

# Create a new stage with a smaller base image to reduce final image size
FROM debian:bullseye-slim


# Install libssl1.1
RUN apt-get update && apt-get install -y libssl1.1 && rm -rf /var/lib/apt/lists/*


# Set the working directory in the Docker image
WORKDIR /usr/local/bin

# Copy the binary from the builder stage to the current stage
COPY --from=builder /usr/src/geo-substream-sink/target/release/geo-substream-sink .

# Set the command to run when the Docker image starts
# If you need any environment variables or other setup, you'd add them before this
CMD ["./geo-substream-sink", "deploy-global"]