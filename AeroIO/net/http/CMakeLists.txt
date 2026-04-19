aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} Http_SRC)

add_library(AeroIO_Http ${Http_SRC})

# 1. 添加 include 路径
target_include_directories(AeroIO_Http PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

find_library(JEMALLOC_LIB jemalloc)

# 2. 链接依赖库
target_link_libraries(AeroIO_Http PUBLIC AeroIO_net ${JEMALLOC_LIB})
