INCLUDE_DIRECTORIES("${PROJECT_SOURCE_DIR}/src")
INCLUDE_DIRECTORIES("${PROJECT_SOURCE_DIR}/src/dep")
ADD_EXECUTABLE(test_searchcluster test_searchcluster.c)
TARGET_LINK_LIBRARIES(test_searchcluster testdeps m)

ADD_TEST(NAME test_searchcluster COMMAND test_searchcluster)