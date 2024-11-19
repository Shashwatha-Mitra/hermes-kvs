cmake_minimum_required(VERSION 3.8)
project(Task1 C CXX)

# include_directories("/home/proteet/.local/include")
# link_directories("/home/proteet/.local/lib")
# find_package(grpc CONFIG REQUIRED)
find_package(spdlog REQUIRED HINTS "${CMAKE_SOURCE_DIR}/external/spdlog/build")

# add_subdirectory(grpc)
include_directories("${CMAKE_SOURCE_DIR}/external")

add_subdirectory(src)
# add_dependencies(src grpc)
