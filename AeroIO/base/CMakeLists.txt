aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} Base_SRC)

find_package(PkgConfig REQUIRED)
pkg_check_modules(JEMALLOC jemalloc)

add_library(aeroIo_base ${Base_SRC})

target_link_libraries(aeroIo_base
PUBLIC
    jemalloc 
    ${JEMALLOC_LIBRARIES}
)

add_subdirectory(Log)