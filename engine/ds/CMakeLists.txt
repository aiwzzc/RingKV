aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} DS_SRC)

add_library(engine_ds ${DS_SRC})

target_include_directories(engine_ds
PUBLIC
    ${CMAKE_SOURCE_DIR}/AeroIO/base
    ${CMAKE_SOURCE_DIR}/third_party/unordered_dense/include
    ${CMAKE_SOURCE_DIR}/third_party/wyhash
)

target_link_libraries(engine_ds
PUBLIC
    aeroIo_base
)