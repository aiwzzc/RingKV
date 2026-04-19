include_directories(${CMAKE_SOURCE_DIR})

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} net_SRCS)

add_library(AeroIO_net ${net_SRCS})

target_include_directories(AeroIO_net PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_SOURCE_DIR}/tools)

find_library(JEMALLOC_LIB jemalloc)
target_link_libraries(AeroIO_net ${JEMALLOC_LIB})

add_subdirectory(http)