# set(absl_DIR "/home/proteet/git_repos/hermes/grpc/third_party/abseil-cpp/CMake")
# set(CMAKE_PREFIX_PATH "/home/proteet/.local/lib")
# Find absl package
# find_
SET(GRPC_INSTALLATION_PATH "$ENV{HOME}/.local")
include_directories("${GRPC_INSTALLATION_PATH}/include")
link_directories("${GRPC_INSTALLATION_PATH}/lib")
SET(CMAKE_PREFIX_PATH GRPC_INSTALLATION_PATH)
find_package(absl CONFIG REQUIRED)
find_package(protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)


find_program(_PROTOBUF_PROTOC protoc
    HINTS "${GRPC_INSTALLATION_PATH}/bin"
)
find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin
    HINTS "${GRPC_INSTALLATION_PATH}/bin"
)

# Proto file
get_filename_component(hermes_proto "${CMAKE_SOURCE_DIR}/protos/hermes.proto" ABSOLUTE)
get_filename_component(hermes_proto_path "${hermes_proto}" PATH)

# Generated sources
set(hermes_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/hermes.pb.cc")
set(hermes_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/hermes.pb.h")
set(hermes_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/hermes.grpc.pb.cc")
set(hermes_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/hermes.grpc.pb.h")

add_custom_command(
      OUTPUT "${hermes_proto_srcs}" "${hermes_proto_hdrs}" "${hermes_grpc_srcs}" "${hermes_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${hermes_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${hermes_proto}"
      DEPENDS "${hermes_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

add_library(hermes_grpc_proto
  ${hermes_grpc_srcs}
  ${hermes_grpc_hdrs}
  ${hermes_proto_srcs}
  ${hermes_proto_hdrs})

target_link_libraries(hermes_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

add_executable(server server.cpp)
add_executable(client client.cpp)

# message("${absl::log_initialize}")
target_link_libraries(server 
    hermes_grpc_proto
    absl::flags absl::flags_parse 
    #  absl::log_initialize
    # absl::log_globals
    gRPC::grpc++
    protobuf    
)
target_link_libraries(client 
    hermes_grpc_proto
    absl::flags absl::flags_parse 
    gRPC::grpc++
    protobuf) 