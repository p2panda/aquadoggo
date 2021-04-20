FROM ekidd/rust-musl-builder as PLANNER
# Install cargo chef: https://github.com/LukeMathWalker/cargo-chef
# We only pay the installation cost once, 
# it will be cached from the second build onwards
RUN cargo install cargo-chef --version 0.1.19
# Add source code with right permissions
ADD --chown=rust:rust . ./
# Analyze the current project to determine the minimum subset of files (Cargo.lock
# and Cargo.toml manifests) required to build it and cache dependencies
RUN cargo chef prepare --recipe-path recipe.json

FROM ekidd/rust-musl-builder as CACHER

RUN cargo install cargo-chef --version 0.1.19
COPY --from=PLANNER /home/rust/src/recipe.json recipe.json
# Re-hydrate the minimum project skeleton identified by `cargo chef prepare` and
# build it to cache dependencies
RUN cargo chef cook --release --recipe-path /home/rust/src/recipe.json

FROM ekidd/rust-musl-builder:latest AS BUILDER

# Add source code with right permissions
ADD --chown=rust:rust . ./

# Copy over the cached dependencies
COPY --from=CACHER /home/rust/src/target target
COPY --from=CACHER $CARGO_HOME $CARGO_HOME

# Build our application (just builds aquadogo & aquadoggo_cli)
RUN cargo build --release

FROM alpine:latest
# Copy release into final alpine image
RUN apk --no-cache add ca-certificates
COPY --from=BUILDER \
            /home/rust/src/target/x86_64-unknown-linux-musl/release/aquadoggo \
            /usr/local/bin/
CMD /usr/local/bin/aquadoggo
