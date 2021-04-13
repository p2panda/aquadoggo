FROM ekidd/rust-musl-builder as PLANNER
# We only pay the installation cost once, 
# it will be cached from the second build onwards
# To ensure a reproducible build consider pinning 
# the cargo-chef version with `--version X.X.X`
RUN cargo install cargo-chef --version 0.1.19
# Add source code with right permissions
ADD --chown=rust:rust . ./
RUN cargo chef prepare --recipe-path recipe.json

FROM ekidd/rust-musl-builder as CACHER

RUN cargo install cargo-chef --version 0.1.19
COPY --from=PLANNER /home/rust/src/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path /home/rust/src/recipe.json

FROM ekidd/rust-musl-builder:latest AS BUILDER

# Add source code with right permissions
ADD --chown=rust:rust . ./

# Copy over the cached dependencies
COPY --from=CACHER /home/rust/src/target target
COPY --from=CACHER $CARGO_HOME $CARGO_HOME

# Build our application
RUN cargo build --release

# Now, we need to build our _real_ Docker container, copying in `aquadoggo`
FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=BUILDER \
            /home/rust/src/target/x86_64-unknown-linux-musl/release \
            /usr/local/bin/
CMD /usr/local/bin/aquadoggo
