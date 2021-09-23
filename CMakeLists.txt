CMAKE_MINIMUM_REQUIRED(VERSION 3.10.0)
PROJECT(RSCoordinator)

INCLUDE(src/dep/RediSearch/cmake/redisearch_cflags.cmake)

OPTION(RS_ENTERPRISE_BUILD "build for enterprise cluster" OFF)

if(NOT DEFINED binroot)
	get_filename_component(binroot ${CMAKE_CURRENT_BINARY_DIR} ABSOLUTE)
endif()
message("RSCoordinator binroot: " ${binroot})

if (NOT DEFINED VECSIM_ARCH)
set(VECSIM_ARCH "native")
endif()
message("RSCoordinator/VecSim VECSIM_ARCH: " ${VECSIM_ARCH})

SET(RS_BUILD_COORDINATOR 1)

ADD_DEFINITIONS(-DREDISMODULE_EXPERIMENTAL_API)
ADD_DEFINITIONS(-D_GNU_SOURCE)
ADD_DEFINITIONS(-DREDIS_MODULE_TARGET)
ADD_DEFINITIONS(-UNDEBUG)
ADD_DEFINITIONS(-DRS_COORDINATOR)

# Also remove /D NDEBUG to avoid MSVC warnings about conflicting defines.
FOREACH (flags_var_to_scrub
    CMAKE_CXX_FLAGS_RELEASE
    CMAKE_CXX_FLAGS_RELWITHDEBINFO
    CMAKE_CXX_FLAGS_MINSIZEREL
    CMAKE_C_FLAGS_RELEASE
    CMAKE_C_FLAGS_RELWITHDEBINFO
    CMAKE_C_FLAGS_MINSIZEREL)
  STRING(REGEX REPLACE "(^| )[/-]D *NDEBUG($| )" " "
    "${flags_var_to_scrub}" "${${flags_var_to_scrub}}")
ENDFOREACH()

set(RS_VECSIM_SO_FLAGS "-lstdc++")

SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${RS_C_FLAGS}")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${RS_CXX_FLAGS}")
SET(CMAKE_LINKER "${CMAKE_C_COMPILER}")

# Specific to coordinator
SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fvisibility=hidden")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fvisibility=hidden")

SET(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Bsymbolic -Bsymbolic-functions ${RS_VECSIM_SO_FLAGS}")
SET(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -Bsymbolic -Bsymbolic-functions  ${RS_VECSIM_SO_FLAGS}")

SET(RS_DIR ${PROJECT_SOURCE_DIR}/src/dep/RediSearch)
SET(RS_RUN_TESTS OFF CACHE BOOL "" FORCE) # Don't run RS' own tests
SET(RS_ENABLE_MOCK ON CACHE BOOL "" FORCE) # But build mock so we can link our own tests
SET(RS_BUILD_STATIC ON CACHE BOOL "" FORCE)
ENABLE_TESTING() # Enable our own tests

FIND_PATH(LIBUV_INCLUDE_DIR uv.h
    HINTS ${LIBUV_ROOT}
    PATH_SUFFIXES include)

FIND_LIBRARY(LIBUV_LIBRARIES
    NAMES libuv.a uv
    HINTS ${LIBUV_ROOT}
    PATH_SUFFIXES lib)

INCLUDE_DIRECTORIES(${RS_DIR}/src)
INCLUDE_DIRECTORIES(${LIBUV_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${RS_DIR}/deps)
INCLUDE_DIRECTORIES(${RS_DIR}/deps)
INCLUDE_DIRECTORIES(${RS_DIR}/deps/VectorSimilarity/src)

ADD_SUBDIRECTORY(src/dep/RediSearch)
ADD_SUBDIRECTORY(src/dep/rmr)
ADD_SUBDIRECTORY(src/dep/rmutil)
ADD_SUBDIRECTORY(src)

include (ExternalProject)

ExternalProject_Get_Property (VectorSimilarity install_dir)
set(VectorSimilarity_bindir ${install_dir})

add_library (vecsim STATIC IMPORTED)
set_target_properties (vecsim PROPERTIES IMPORTED_LOCATION ${VectorSimilarity_bindir}/libVectorSimilarity.a)

add_library (vecsim-spaces STATIC IMPORTED)
set_target_properties (vecsim-spaces PROPERTIES IMPORTED_LOCATION ${VectorSimilarity_bindir}/libVectorSimilaritySpaces.a)

SET(FINAL_OBJECTS
    ${COORDINATOR_SRC}
    $<TARGET_OBJECTS:coordinator-core>
    $<TARGET_OBJECTS:coordinator-rmutil>
    $<TARGET_OBJECTS:rmr>)

MACRO(MODULE_TARGET SUFFIX)
    SET(_moduleName module-${SUFFIX})
    ADD_LIBRARY(${_moduleName} SHARED ${FINAL_OBJECTS} src/build-info/info-${SUFFIX}.c)
    TARGET_LINK_LIBRARIES(${_moduleName} redisearch-${SUFFIX} ${LIBUV_LIBRARIES} hiredis hiredis_ssl vecsim vecsim-spaces)

    IF (${SUFFIX} STREQUAL "oss")
        SET(_moduleDefs "RS_CLUSTER_OSS")
    ELSEIF (${SUFFIX} STREQUAL "enterprise")
        SET(_moduleDefs "RS_CLUSTER_ENTERPRISE")
    ENDIF()
    target_compile_definitions(${_moduleName} PRIVATE ${_moduleDefs})
    
    SET_TARGET_PROPERTIES(${_moduleName} PROPERTIES LINKER_LANGUAGE CXX)
    ADD_LDFLAGS(${_moduleName} "-Wl,--enable-new-dtags -Wl,-rpath,\$ORIGIN")

    SET_TARGET_PROPERTIES(${_moduleName} PROPERTIES PREFIX "")
    SET_TARGET_PROPERTIES(${_moduleName} PROPERTIES SUFFIX ".so")

    IF (NOT APPLE)
        ADD_CUSTOM_COMMAND(TARGET ${_moduleName} POST_BUILD
            COMMAND objcopy --only-keep-debug $<TARGET_FILE:${_moduleName}> $<TARGET_FILE:${_moduleName}>.debug
            COMMAND objcopy --add-gnu-debuglink $<TARGET_FILE:${_moduleName}>.debug $<TARGET_FILE:${_moduleName}>
            COMMAND ${CMAKE_STRIP} -g $<TARGET_FILE:${_moduleName}>)
    ENDIF()
	
	add_dependencies(${_moduleName} VectorSimilarity)
ENDMACRO()

# IF (RS_ENTERPRISE_BUILD)
#   MODULE_TARGET(enterprise)
# ELSE()
#   MODULE_TARGET(oss)
# ENDIF()

MODULE_TARGET(oss)
MODULE_TARGET(enterprise)

add_library(testdeps STATIC ${FINAL_OBJECTS})
target_link_libraries(testdeps ${LIBUV_LIBRARIES} redisearch-oss redismock hiredis hiredis_ssl vecsim vecsim-spaces dl)
# Add RMR's tests, and our own tests
add_subdirectory(src/dep/rmr/test)
add_subdirectory(test)
add_subdirectory(src/dep/RediSearch/tests/ctests/ext-example)

set_target_properties(redismock PROPERTIES COMPILE_FLAGS "-fvisibility=default")
