CURRENTDIR=$(dirname "$0")

pushd $CURRENTDIR/..
./gradlew shadowJar
cd docker

repository="${1:-venicedb}"
oss_release="${2:-0.4.336}"

set -x
echo "Building docker images for repository $repository, version $oss_release"

head_hash=$(git rev-parse --short HEAD)
version=$oss_release

cp *py venice-client/
cp ../clients/venice-push-job/build/libs/venice-push-job-all.jar venice-client/
cp ../clients/venice-thin-client/build/libs/venice-thin-client-all.jar venice-client/
cp ../clients/venice-client/build/libs/venice-client-all.jar venice-client/
cp ../clients/venice-admin-tool/build/libs/venice-admin-tool-all.jar venice-client/
cp *py venice-client-jupyter/
cp ../clients/venice-push-job/build/libs/venice-push-job-all.jar venice-client-jupyter/
cp ../clients/venice-thin-client/build/libs/venice-thin-client-all.jar venice-client-jupyter/
cp ../clients/venice-admin-tool/build/libs/venice-admin-tool-all.jar venice-client-jupyter/
cp *py venice-server/
cp ../services/venice-server/build/libs/venice-server-all.jar venice-server/
cp *py venice-controller/
cp ../services/venice-controller/build/libs/venice-controller-all.jar venice-controller/
cp *py venice-router/
cp ../services/venice-router/build/libs/venice-router-all.jar venice-router/

targets=(venice-controller venice-server venice-router venice-client venice-client-jupyter)
# Define image descriptions
declare -A image_descriptions=(
  ["venice-controller"]="Venice Controller: responsible for managing administrative operations such as store creation, deletion, updates, and starting new pushes or versions."
  ["venice-server"]="Venice Server: Acts as a Venice storage node, handling data ingestion, storage, and serving from RocksDB."
  ["venice-router"]="Venice Router: responsible for routing requests from clients to the appropriate Venice Servers."
  ["venice-client"]="Venice Client: Includes tools for store administration (e.g., create, delete), data pushing (VPJ), and a CLI for querying store data."
  ["venice-client-jupyter"]="Venice Client Jupyter: Includes most of the same things as venice-client with the addition of a demo workflow using Spark and Jupyter."
)

# Build each target with labels
for target in ${targets[@]}; do
    docker buildx build --load --platform linux/amd64 \
        --label "org.opencontainers.image.source=https://github.com/linkedin/venice" \
        --label "org.opencontainers.image.authors=VeniceDB" \
        --label "org.opencontainers.image.description=${image_descriptions[$target]}" \
        --label "org.opencontainers.image.licenses=BSD-2-Clause" \
        -t "$repository/$target:$version" -t "$repository/$target:latest-dev" $target
    docker buildx build --load --platform linux/amd64 -t "$repository/$target:$version" -t "$repository/$target:latest-dev" $target
done

rm -f venice-client/venice-push-job-all.jar
rm -f venice-client/venice-thin-client-all.jar
rm -f venice-client/venice-client-all.jar
rm -f venice-client/venice-admin-tool-all.jar
rm -f venice-client-jupyter/venice-push-job-all.jar
rm -f venice-client-jupyter/venice-thin-client-all.jar
rm -f venice-client-jupyter/venice-admin-tool-all.jar
rm -f venice-server/venice-server-all.jar
rm -f venice-controller/venice-controller-all.jar
rm -f venice-router/venice-router-all.jar
rm */*.py

popd
