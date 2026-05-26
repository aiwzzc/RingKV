aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} Server_SRC)

add_library(rkv_server ${Server_SRC})

target_include_directories(rkv_server
PUBLIC
    ${CMAKE_SOURCE_DIR}/AeroIO
    ${CMAKE_SOURCE_DIR}/engine
    ${CMAKE_SOURCE_DIR}/protocol
)

target_link_libraries(rkv_server
PUBLIC
    aeroIo_net
    engine_core
    engine_persist
    rkv_protocol
    hiredis
)