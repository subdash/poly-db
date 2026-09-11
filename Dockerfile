# Builder layer
FROM rust:1-bookworm AS builder

WORKDIR /build

COPY . .
# Binary should land at target/release/kvs-server. Use --locked option
# to ensure that the build fails instead of silently updating Cargo.lock
# if it's out of date with the manifests.
RUN cargo build --release --locked -p kvs-server

# Runtime layer
FROM debian:bookworm-slim

# Define the unprivileged group and user
ARG USERNAME=kvs
ARG USER_UID=10001
ARG USER_GID=10001

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID --no-create-home $USERNAME

# Create the working directory and copy the binary into the runtime layer
ARG WD=/var/lib/kvs
RUN mkdir -p $WD && chown $USER_UID:$USER_GID $WD
WORKDIR $WD
COPY --from=builder /build/target/release/kvs-server /usr/local/bin/kvs-server

# Expose port 3000 and run the binary as the unprivileged user.
EXPOSE 3000
USER $USERNAME
ENTRYPOINT ["/usr/local/bin/kvs-server"]

