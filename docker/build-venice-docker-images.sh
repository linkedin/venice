CURRENTDIR=$(dirname "$0")

pushd $CURRENTDIR/..
./gradlew shadowJar
cd docker

repository="${1:-venicedb}"
oss_release="${2:-0.4.17}"

set -x
echo "Building docker images for repository $repository, version $oss_release"

head_hash=$(git rev-parse --short HEAD)
version=$oss_release

cp *py venice-client/ 
cp ../clients/venice-push-job/build/libs/venice-push-job-all.jar venice-client/
cp ../clients/venice-thin-client/build/libs/venice-thin-client-all.jar venice-client/
cp ../clients/venice-admin-tool/build/libs/venice-admin-tool-all.jar venice-client/
cp *py venice-server/ 
cp ../services/venice-server/build/libs/venice-server-all.jar venice-server/
cp *py venice-controller/ 
cp ../services/venice-controller/build/libs/venice-controller-all.jar venice-controller/
cp *py venice-router/ 
cp ../services/venice-router/build/libs/venice-router-all.jar venice-router/

targets=(venice-controller venice-server venice-router venice-client)

for target in ${targets[@]}; do
    docker buildx build --load --platform linux/amd64 -t "$repository/$target:$version" -t "$repository/$target:latest-dev" $target
done

rm -f venice-client/venice-push-job-all.jar
rm -f venice-client/venice-thin-client-all.jar
rm -f venice-client/venice-admin-tool-all.jar
rm -f venice-server/venice-server-all.jar
rm -f venice-controller/venice-controller-all.jar
rm -f venice-router/venice-router-all.jar
rm */*.py

popd
