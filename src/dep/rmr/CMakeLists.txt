FILE(GLOB RMR_SRC
    *.c
    redise_parser/*.c)

INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/src)

ADD_LIBRARY(rmr OBJECT
    ${RMR_SRC}
    ${CMAKE_CURRENT_SOURCE_DIR}/../triemap/triemap.c)
ADD_SUBDIRECTORY(hiredis)