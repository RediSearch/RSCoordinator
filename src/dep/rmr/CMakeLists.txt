FILE(GLOB RMR_SRC
    *.c
    redise_parser/*.c)

INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/src)
ADD_LIBRARY(rmr OBJECT ${RMR_SRC})
set(ENABLE_SSL ON CACHE BOOL "Build hiredis with ssl")
ADD_SUBDIRECTORY(hiredis)
