aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} Server_SRC)
list(REMOVE_ITEM Server_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/config.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/WorkerGroup.cc)

add_library(rkv_lifecycle config.cc WorkerGroup.cc)
target_include_directories(rkv_lifecycle PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(rkv_lifecycle PUBLIC Threads::Threads)

add_library(rkv_server ${Server_SRC} replication/Backlog.cc)

target_include_directories(rkv_server
PUBLIC
    ${CMAKE_SOURCE_DIR}/AeroIO
    ${CMAKE_SOURCE_DIR}/engine
    ${CMAKE_SOURCE_DIR}/protocol
)

target_link_libraries(rkv_server
PUBLIC
    rkv_lifecycle
    aeroIo_net
    engine_core
    rkv_protocol
)
