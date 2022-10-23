cd ../
./gradlew shadowJar
cd docker

head_hash=$(git rev-parse --short HEAD)

cp ../clients/venice-push-job/build/libs/venice-push-job-all.jar venice-client/
cp ../clients/venice-thin-client/build/libs/venice-thin-client-all.jar venice-client/
cp ../clients/venice-admin-tool/build/libs/venice-admin-tool-all.jar venice-client/
cp ../services/venice-server/build/libs/venice-server-all.jar venice-server/
cp ../services/venice-controller/build/libs/venice-controller-all.jar venice-controller/
cp ../services/venice-router/build/libs/venice-router-all.jar venice-router/

targets=(venice-controller venice-server venice-router venice-client)

for target in ${targets[@]}; do
    docker build -t "$target:$head_hash" $target
done

rm -f venice-client/venice-push-job-all.jar
rm -f venice-client/venice-thin-client-all.jar
rm -f venice-client/venice-admin-tool-all.jar
rm -f venice-server/venice-server-all.jar
rm -f venice-controller/venice-controller-all.jar
rm -f venice-router/venice-router-all.jar
