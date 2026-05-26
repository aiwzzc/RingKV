aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} Persist_SRC)

add_library(engine_persist ${Persist_SRC})

target_include_directories(engine_persist
PUBLIC
    ${CMAKE_SOURCE_DIR}/engine
)

target_link_libraries(engine_persist
PUBLIC
    engine_ds
)