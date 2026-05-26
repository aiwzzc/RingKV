aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} CORE_SRC)

add_library(engine_core ${CORE_SRC})

target_include_directories(engine_core
PUBLIC
    ${CMAKE_SOURCE_DIR}/AeroIO/base
    ${CMAKE_SOURCE_DIR}/engine
    ${CMAKE_SOURCE_DIR}/protocol
    ${CMAKE_SOURCE_DIR}/third_party/unordered_dense/include
)

target_link_libraries(engine_core 
PUBLIC
    engine_ds
    aeroIo_base
    rkv_protocol
)