cmake_minimum_required(VERSION 3.0.0)
project(rmr VERSION 0.0.0)

include(CTest)
enable_testing()

add_executable(rmr src/main.c)
target_link_libraries(rmr ev)
target_link_libraries(rmr ${CMAKE_SOURCE_DIR}/src/hiredis/libhiredis.a)

set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
include(CPack)
