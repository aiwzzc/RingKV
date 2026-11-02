add_executable(lifecycle_test lifecycle_test.cc)
target_link_libraries(lifecycle_test PRIVATE rkv_lifecycle)
add_test(NAME lifecycle COMMAND lifecycle_test)
set_tests_properties(lifecycle PROPERTIES TIMEOUT 20)

find_package(Python3 REQUIRED COMPONENTS Interpreter)
add_test(NAME config_cli COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/config_cli_test.py $<TARGET_FILE:RingKV>)
add_test(NAME server_lifecycle COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/server_lifecycle_test.py $<TARGET_FILE:RingKV>)
set_tests_properties(config_cli server_lifecycle PROPERTIES TIMEOUT 45)
set_tests_properties(server_lifecycle PROPERTIES SKIP_RETURN_CODE 77)
