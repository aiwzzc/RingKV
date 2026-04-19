include_directories(${CMAKE_SOURCE_DIR})

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} Server_SRC)

find_package(PkgConfig REQUIRED)
pkg_check_modules(JEMALLOC jemalloc)

add_executable(RingKV ${Server_SRC})

# target_compile_options(RingKV PRIVATE -pg)
# target_link_options(RingKV PRIVATE -pg)

# target_compile_options(RingKV PRIVATE
#     -fsanitize=address
#     -fno-omit-frame-pointer
#     -g
#     -O0
# )

# target_link_options(RingKV PRIVATE
#     -fsanitize=address
# )

target_include_directories(RingKV PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/tools
)

target_link_libraries(RingKV AeroIO_net AeroIO_Http uring jemalloc hiredis ${JEMALLOC_LIBRARIES})