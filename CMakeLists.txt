cmake_minimum_required(VERSION 3.0.0)
project(rmr VERSION 0.0.0)

include(CTest)
enable_testing()

#//add_library(rmr src/rmr.c)

add_library(rmr MODULE
    src/rmr.c src/module.c
)
target_compile_options(rmr PUBLIC "-fPIC")


target_link_libraries(rmr uv)
target_link_libraries(rmr pthread)
target_link_libraries(rmr ${CMAKE_SOURCE_DIR}/src/hiredis/libhiredis.a)
target_link_libraries(rmr -Bsymbolic -Bsymbolic-functions)



set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
include(CPack)
