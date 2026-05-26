aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} net_SRC)

add_library(aeroIo_net ${net_SRC})

target_include_directories(aeroIo_net 
PUBLIC 
    ${CMAKE_SOURCE_DIR}/third_party
    ${CMAKE_SOURCE_DIR}/AeroIO/base
)

target_link_libraries(aeroIo_net 
PUBLIC
    aeroIo_base
    aeroIo_log
    uring
)