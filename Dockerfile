FROM ekidd/rust-musl-builder:latest AS BUILDER

# Add source code with right permissions
ADD --chown=rust:rust . ./

# Build our application
RUN cargo build --release

# Now, we need to build our _real_ Docker container, copying in `aquadoggo`
FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=BUILDER \
            /home/rust/src/target/x86_64-unknown-linux-musl/release/aquadoggo \
            /usr/local/bin/
CMD /usr/local/bin/aquadoggo
