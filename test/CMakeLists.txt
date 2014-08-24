cmake_minimum_required (VERSION 2.6)

project(cql_interface_tests)

FILE(GLOB test_files "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")
add_executable(${PROJECT_NAME} ${test_files} ${main_files})

include_directories (/usr/include  /usr/local/include/  ../..)

target_link_libraries(${PROJECT_NAME} cql_interface )

link_directories(/usr/local/lib /usr/lib)

add_test (${PROJECT_NAME} ${PROJECT_NAME}
                  --logger_config ${CMAKE_CURRENT_SOURCE_DIR}/test_log.cfg )

