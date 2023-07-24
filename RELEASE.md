# Releasing aquadoggo

_This is an example for publising version `1.2.0`._

## Checks and preparations

1. Check that the CI has passed on the aquadoggo project's [Github page](https://github.com/p2panda/aquadoggo).
2. Make sure you are on the `main` branch.
3. Run the test suites and make sure all tests pass: `cargo test`.

## Changelog time!

4. Check the git history for any commits on main that have not been mentioned in the _Unreleased_ section of `CHANGELOG.md` but should be.
5. Add an entry in `CHANGELOG.md` for this new release and move over all the _Unreleased_ stuff. Follow the formatting given by previous entries.

## Tagging and versioning

6. Bump the package version in `Cargo.toml` by hand.
7. Commit the version changes with a commit message `1.2.0`.
8. Run `git tag v1.2.0` and push including your tags using `git push origin main --tags`.

## Publishing releases

9. Copy the changelog entry you authored into Github's [new release page](https://github.com/p2panda/aquadoggo/releases/new)'s description field. Title it with your version `v1.2.0`.
10. Run `cargo publish`.

## Publishing on DockerHub

11. The GitHub Action will automatically release this version on DockerHub based on the given Git Tag.
