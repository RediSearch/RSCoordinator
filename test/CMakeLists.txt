INCLUDE_DIRECTORIES("${PROJECT_SOURCE_DIR}/src")
INCLUDE_DIRECTORIES("${PROJECT_SOURCE_DIR}/src/dep")

ADD_EXECUTABLE(test_searchcluster test_searchcluster.c)
TARGET_LINK_LIBRARIES(test_searchcluster testdeps m)
SET_TARGET_PROPERTIES(test_searchcluster PROPERTIES COMPILE_FLAGS "-fvisibility=default")
TARGET_COMPILE_DEFINITIONS(test_searchcluster PRIVATE REDISMODULE_MAIN) 

ADD_EXECUTABLE(test_distagg test_distagg.cpp)
TARGET_LINK_LIBRARIES(test_distagg testdeps m redismock dl)
SET_TARGET_PROPERTIES(test_distagg PROPERTIES COMPILE_FLAGS "-fvisibility=default")
TARGET_COMPILE_DEFINITIONS(test_distagg PRIVATE REDISMODULE_MAIN) 

ADD_TEST(NAME test_searchcluster COMMAND test_searchcluster)
ADD_TEST(name test_distagg COMMAND test_distagg)
