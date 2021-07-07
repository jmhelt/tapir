protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${CMAKE_SOURCE_DIR}/store/common/common-proto.proto tapir-proto.proto)

file(GLOB SourceFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_library(StoreTAPIR ${SourceFiles} ${PROTO_SRCS})
target_include_directories(StoreTAPIR PUBLIC ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR})
target_link_libraries(StoreTAPIR ReplicationIR Common)