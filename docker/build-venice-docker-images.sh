CURRENTDIR=$(dirname "$0")

pushd $CURRENTDIR/..

# Skip shadowJar if jars already exist (e.g., caller already ran assemble + shadowJar)
if ls services/venice-server/build/libs/venice-server-all.jar >/dev/null 2>&1; then
  echo "Shadow jars already exist, skipping ./gradlew shadowJar"
else
  ./gradlew shadowJar
fi

cd docker

repository="${1:-venicedb}"
oss_release="${2:-0.4.336}"
read -ra targets <<< "${3:-venice-controller venice-server venice-router venice-client venice-client-jupyter}"

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
