#! /bin/bash

ROOT_DIR=$(pwd)
BUILD_DIR=$ROOT_DIR/build
BIN_DIR=$ROOT_DIR/bin
EXTERNAL_DIR=$ROOT_DIR/external

# Get the current username
current_user=$(whoami)

# Check if the username is "aatmanb"
if [ "$current_user" == "aatmanb" ]; then
    export GRPC_PATH="/nobackup2/aatmanb/cs739/kv_store/.local"
    echo "gRPC path set to $GRPC_PATH"
    source venv/bin/activate
else
    export GRPC_PATH="$HOME/.local"
    echo "gRPC path set to $GRPC_PATH"
fi

export SPDLOG_PATH="$EXTERNAL_DIR/spdlog"
export PATH="$GRPC_PATH/bin:$PATH"

# Check if directories exist, if not create it
if [ ! -d "$BUILD_DIR" ]; then 
    echo "Creating Build directory $BUILD_DIR"
    mkdir -p "$BUILD_DIR"
fi

if [ ! -d "$BIN_DIR" ]; then 
    echo "Creating Bin directory $BIN_DIR"
    mkdir -p "$BIN_DIR"
fi

cd $BUILD_DIR
cmake ..
make -j 4
cd $ROOT_DIR

# Generate python grpc code
python3 -m grpc_tools.protoc -I./protos --python_out=./src/client --pyi_out=./src/client --grpc_python_out=./src/client ./protos/hermes.proto
