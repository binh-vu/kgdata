set -ex


echo "This script is used to build aarch64 binary as Github does not supported it yet"
echo "You need to run it in the root of the project"

if [ -d "$(pwd)/dist" ]; then
    rm -r $(pwd)/dist
fi

docker run --rm -w /project -v $(pwd):/project \
    -e EXTRA_PATH=/opt/python/cp310-cp310/bin \
    -e PYTHON_ROOT_DIR=/opt/python \
    -e CARGO_NET_GIT_FETCH_WITH_CLI=false \
    quay.io/pypa/manylinux2014_aarch64:latest \
    bash /project/.github/workflows/build.sh -t aarch64-unknown-linux-gnu
