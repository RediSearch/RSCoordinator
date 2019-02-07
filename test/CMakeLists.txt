INCLUDE_DIRECTORIES("${PROJECT_SOURCE_DIR}/src")
INCLUDE_DIRECTORIES("${PROJECT_SOURCE_DIR}/src/dep")
ADD_EXECUTABLE(test_searchcluster test_searchcluster.c)
ADD_EXECUTABLE(test_distagg test_distagg.cpp)
TARGET_LINK_LIBRARIES(test_searchcluster testdeps m)
TARGET_LINK_LIBRARIES(test_distagg testdeps m)

ADD_TEST(NAME test_searchcluster COMMAND test_searchcluster)
ADD_TEST(name test_distagg COMMAND test_distagg)
