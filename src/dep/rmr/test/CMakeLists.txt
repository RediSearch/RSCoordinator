# Get the actual test names
INCLUDE_DIRECTORIES(../)
FUNCTION(RMRTEST name)
    ADD_EXECUTABLE("${name}" ${name}.c init-rm.c)

    TARGET_LINK_LIBRARIES("${name}" testdeps m)
    ADD_TEST(NAME "${name}" COMMAND "${name}")
    SET_TARGET_PROPERTIES("${name}" PROPERTIES COMPILE_FLAGS "-fvisibility=default")
    TARGET_COMPILE_DEFINITIONS("${name}" PRIVATE REDISMODULE_MAIN)
ENDFUNCTION()

FILE(GLOB TEST_SOURCES "test_*.c")

FOREACH(n ${TEST_SOURCES})
    GET_FILENAME_COMPONENT(test_name ${n} NAME_WE)
    MESSAGE("${n} => ${test_name}")
    RMRTEST("${test_name}")
ENDFOREACH()