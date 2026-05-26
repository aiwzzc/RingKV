aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} RingKV_SRC)

add_executable(RingKV ${RingKV_SRC})

target_include_directories(RingKV 
PRIVATE
    ${CMAKE_SOURCE_DIR}/server
    ${CMAKE_SOURCE_DIR}/AeroIO/base
)

target_link_libraries(RingKV 
PUBLIC
    rkv_server
    aeroIo_log
)