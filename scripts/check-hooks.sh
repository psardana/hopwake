#!/usr/bin/env bash
# Checks every hook against the shared contract (docs/hooks.md): its own unit
# tests, which include the shared test vectors, then the wire check.
#
#   scripts/check-hooks.sh              all languages
#   scripts/check-hooks.sh java rust    some of them
set -uo pipefail
cd "$(dirname "$0")/.."

langs=("$@")
[ ${#langs[@]} -eq 0 ] && langs=(go java python dotnet rust node c)

go build -o bin/hookcheck ./cmd/hookcheck || exit 1
failed=()

run() { # name, then the command
  local name=$1; shift
  printf '── %s\n' "$name"
  if ! "$@"; then failed+=("$name"); fi
}

for l in "${langs[@]}"; do
  case $l in
    go)
      run "go: tests" go test ./pkg/hook/...
      run "go: wire" sh -c 'go build -o bin/conformance-go ./pkg/hook/conformance && bin/hookcheck -service conformance-go -- bin/conformance-go' ;;
    java)
      run "java: tests" mvn -q -B -f pkg/hook/java/pom.xml install
      run "java: tests on Spring Boot 4" mvn -q -B -f pkg/hook/java/pom.xml -Pboot4 -pl hopwake-hook,hopwake-spring-boot-starter verify
      # leave the Spring Boot 3 build in place for what depends on it
      mvn -q -B -f pkg/hook/java/pom.xml -DskipTests install >/dev/null
      run "java: wire" bin/hookcheck -service conformance-java -- java -cp pkg/hook/java/hopwake-hook/target/hopwake-hook-0.1.0-SNAPSHOT.jar:pkg/hook/java/conformance/target/conformance.jar dev.hopwake.hook.conformance.Main ;;
    python)
      run "python: tests" sh -c 'cd pkg/hook/python && python3 -m unittest discover -s tests -q'
      run "python: wire" bin/hookcheck -service conformance-python -- python3 pkg/hook/python/conformance.py ;;
    dotnet)
      run "dotnet: tests" dotnet test pkg/hook/dotnet/Hopwake.Hook.sln --nologo -v q
      run "dotnet: wire" sh -c 'dotnet build -c Release --nologo -v q pkg/hook/dotnet/conformance/Hopwake.Hook.Conformance >/dev/null && bin/hookcheck -service conformance-dotnet -- dotnet pkg/hook/dotnet/conformance/Hopwake.Hook.Conformance/bin/Release/net10.0/Hopwake.Hook.Conformance.dll' ;;
    rust)
      run "rust: tests" cargo test -q --manifest-path pkg/hook/rust/Cargo.toml
      run "rust: wire" sh -c 'cargo build -q --release --manifest-path pkg/hook/rust/Cargo.toml && bin/hookcheck -service conformance-rust -- pkg/hook/rust/target/release/conformance' ;;
    node)
      run "node: tests" node --experimental-strip-types --no-warnings --test pkg/hook/node/hopwake-hook.test.ts
      run "node: wire" bin/hookcheck -service conformance-node -- node --experimental-strip-types --no-warnings pkg/hook/node/conformance.ts ;;
    c)
      if [ -f pkg/hook/c/Makefile ]; then
        run "c: tests" make -s -C pkg/hook/c test
        run "c: through the tap" go test ./cmd/tap -run DeviceMarks -count=1
      fi ;;
    *) echo "unknown language: $l"; failed+=("$l") ;;
  esac
done

echo
if [ ${#failed[@]} -gt 0 ]; then
  printf 'FAILED: %s\n' "${failed[*]}"
  exit 1
fi
echo "every hook follows the contract"
