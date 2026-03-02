#!/usr/bin/env bash
set -euo pipefail

CURRENTDIR=$(dirname "$0")

pushd "$CURRENTDIR/.."

repository="${1:-venicedb}"
oss_release="${2:-$(git describe --tags --always 2>/dev/null || echo dev)}"
read -ra targets <<< "${3:-venice-controller venice-server venice-router venice-client venice-client-jupyter}"

# Validate targets against known set
known_targets="venice-controller venice-server venice-router venice-client venice-client-jupyter"
for target in "${targets[@]}"; do
  if [[ ! " $known_targets " =~ " $target " ]]; then
    echo "ERROR: Unknown target '$target'. Valid targets: $known_targets" >&2
    exit 1
  fi
done

# Check whether all required shadow jars for the selected targets already exist
need_shadowjar=false
for target in "${targets[@]}"; do
  case "$target" in
    venice-client)
      for jar in clients/venice-push-job/build/libs/venice-push-job-all.jar \
                 clients/venice-thin-client/build/libs/venice-thin-client-all.jar \
                 clients/venice-client/build/libs/venice-client-all.jar \
                 clients/venice-admin-tool/build/libs/venice-admin-tool-all.jar; do
        [ -f "$jar" ] || { need_shadowjar=true; break; }
      done
      ;;
    venice-client-jupyter)
      for jar in clients/venice-push-job/build/libs/venice-push-job-all.jar \
                 clients/venice-thin-client/build/libs/venice-thin-client-all.jar \
                 clients/venice-admin-tool/build/libs/venice-admin-tool-all.jar; do
        [ -f "$jar" ] || { need_shadowjar=true; break; }
      done
      ;;
    venice-server)
      [ -f "services/venice-server/build/libs/venice-server-all.jar" ] || need_shadowjar=true
      ;;
    venice-controller)
      [ -f "services/venice-controller/build/libs/venice-controller-all.jar" ] || need_shadowjar=true
      ;;
    venice-router)
      [ -f "services/venice-router/build/libs/venice-router-all.jar" ] || need_shadowjar=true
      ;;
  esac
  $need_shadowjar && break
done

if $need_shadowjar; then
  # Build only the shadow jars needed for the selected targets
  shadowjar_tasks=()
  for target in "${targets[@]}"; do
    case "$target" in
      venice-client)
        shadowjar_tasks+=(
          :clients:venice-push-job:shadowJar
          :clients:venice-thin-client:shadowJar
          :clients:venice-client:shadowJar
          :clients:venice-admin-tool:shadowJar
        )
        ;;
      venice-client-jupyter)
        shadowjar_tasks+=(
          :clients:venice-push-job:shadowJar
          :clients:venice-thin-client:shadowJar
          :clients:venice-admin-tool:shadowJar
        )
        ;;
      venice-server)    shadowjar_tasks+=(:services:venice-server:shadowJar) ;;
      venice-controller) shadowjar_tasks+=(:services:venice-controller:shadowJar) ;;
      venice-router)   shadowjar_tasks+=(:services:venice-router:shadowJar) ;;
    esac
  done
  # Deduplicate and run
  IFS=$'\n' shadowjar_tasks=($(printf "%s\n" "${shadowjar_tasks[@]}" | sort -u)); unset IFS
  ./gradlew --parallel "${shadowjar_tasks[@]}"
else
  echo "All required shadow jars for selected targets already exist, skipping ./gradlew shadowJar"
fi

cd docker

set -x
echo "Building docker images for repository $repository, version $oss_release"
echo "Targets: ${targets[*]}"

head_hash=$(git rev-parse --short HEAD)
version=$oss_release

# Copy artifacts only for requested targets
for target in "${targets[@]}"; do
  cp *py "$target/"
  case "$target" in
    venice-client)
      cp ../clients/venice-push-job/build/libs/venice-push-job-all.jar venice-client/
      cp ../clients/venice-thin-client/build/libs/venice-thin-client-all.jar venice-client/
      cp ../clients/venice-client/build/libs/venice-client-all.jar venice-client/
      cp ../clients/venice-admin-tool/build/libs/venice-admin-tool-all.jar venice-client/
      ;;
    venice-client-jupyter)
      cp ../clients/venice-push-job/build/libs/venice-push-job-all.jar venice-client-jupyter/
      cp ../clients/venice-thin-client/build/libs/venice-thin-client-all.jar venice-client-jupyter/
      cp ../clients/venice-admin-tool/build/libs/venice-admin-tool-all.jar venice-client-jupyter/
      ;;
    venice-server)
      cp ../services/venice-server/build/libs/venice-server-all.jar venice-server/
      ;;
    venice-controller)
      cp ../services/venice-controller/build/libs/venice-controller-all.jar venice-controller/
      ;;
    venice-router)
      cp ../services/venice-router/build/libs/venice-router-all.jar venice-router/
      ;;
  esac
done

# Define image descriptions
declare -A image_descriptions=(
  ["venice-controller"]="Venice Controller: responsible for managing administrative operations such as store creation, deletion, updates, and starting new pushes or versions."
  ["venice-server"]="Venice Server: Acts as a Venice storage node, handling data ingestion, storage, and serving from RocksDB."
  ["venice-router"]="Venice Router: responsible for routing requests from clients to the appropriate Venice Servers."
  ["venice-client"]="Venice Client: Includes tools for store administration (e.g., create, delete), data pushing (VPJ), and a CLI for querying store data."
  ["venice-client-jupyter"]="Venice Client Jupyter: Includes most of the same things as venice-client with the addition of a demo workflow using Spark and Jupyter."
)

# Build each target with labels — in parallel
pids=()
for target in "${targets[@]}"; do
    docker buildx build --load --platform linux/amd64 \
        --label "org.opencontainers.image.source=https://github.com/linkedin/venice" \
        --label "org.opencontainers.image.authors=VeniceDB" \
        --label "org.opencontainers.image.description=${image_descriptions[$target]}" \
        --label "org.opencontainers.image.licenses=BSD-2-Clause" \
        -t "$repository/$target:$version" -t "$repository/$target:latest-dev" $target &
    pids+=($!)
done

# Wait for all builds and fail if any failed
for pid in "${pids[@]}"; do
    wait "$pid" || { echo "Docker build failed (pid $pid)"; exit 1; }
done

# Clean up copied artifacts
for target in "${targets[@]}"; do
  rm -f "$target"/*.jar "$target"/*.py
done

popd
