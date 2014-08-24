cmake_minimum_required (VERSION 2.6)

project(cql_interface)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wno-strict-aliasing -std=c++11")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -ggdb")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")

IF("${DO_PROFILING}" EQUAL "1")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -pg")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -pg")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pg")
ENDIF("${DO_PROFILING}" EQUAL "1")

enable_testing()

FILE(GLOB cpp_files "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

link_directories(/usr/local/lib /usr/lib)

add_library(${PROJECT_NAME} STATIC ${cpp_files})

include_directories (/usr/include  /usr/local/include/ ..)

target_link_libraries(${PROJECT_NAME} 
                      boost_program_options.a
                      boost_system.a
                      log4cxx.a 
                      libcassandra_static.a 
                      libaprutil-1.a
                      libapr-1.a
                      libexpat.a
                      ssl
                      crypto
                      libuv.a
                      pthread)

install (PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/libcql_interface.a
         DESTINATION "/usr/local/lib")

FILE(GLOB header_files "${CMAKE_CURRENT_SOURCE_DIR}/*.h")
install (FILES ${header_files}
         DESTINATION "/usr/local/include/cql-interface")

add_subdirectory (test)
