add_subdirectory(common)
add_subdirectory(strongstore)
add_subdirectory(tapirstore)
add_subdirectory(benchmark/async)

file(GLOB SourceFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_executable(store ${SourceFiles})
target_include_directories(store PUBLIC ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR})
target_link_libraries(store StoreStrong StoreTAPIR StoreCommon StoreCommonBackend StoreCommonFrontend Common)