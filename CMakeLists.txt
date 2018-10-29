cmake_minimum_required(VERSION 3.9.6)
project(hdf5tools CXX)

set(CMAKE_CXX_STANDARD 98)

# find package HDF5
# FindHDF5.cmake is essentially broken and does not allow
# to properly override the search path by setting HDF5_ROOT.
# This is a work-around to allow setting a custom HDF5
# installation location to search.
message("\nSearching for HDF5")

# Staticly linking against HDF5... This is a pain. Switching off and setting rpath instead...
#SET(CMAKE_FIND_LIBRARY_SUFFIXES ".a")

# Allow macOS shared (dynamic) libraries to link with undefined symbols
if (APPLE)
    message("Allowing undefined symbols in macOS shared library linking")
    set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -undefined dynamic_lookup")
endif(APPLE)

# Set up RPATH handling correctly so that both built and installed targets have the appropriate
# settings - see https://cmake.org/Wiki/CMake_RPATH_handling
# Use, i.e. don't skip the full RPATH for the build tree
SET(CMAKE_SKIP_BUILD_RPATH  FALSE)
# When building, don't use the install RPATH already (but later on when installing)
SET(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
# Add the automatically determined parts of the RPATH which point to directories outside the
# build tree to the install RPATH
SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
# The RPATH to be used when installing, but only if it's not a system directory
LIST(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES "${CMAKE_INSTALL_PREFIX}/lib" isSystemDir)
IF("${isSystemDir}" STREQUAL "-1")
    SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
ENDIF("${isSystemDir}" STREQUAL "-1")


IF (HDF5_ROOT)
    message(STATUS "HDF5_ROOT set: " ${HDF5_ROOT})
    set( ENV{HDF5_ROOT} ${HDF5_ROOT} )
    find_path( HDF5_INCLUDE_DIRS
            NAMES hdf5.h hdf5_hl.h
            PATHS ${HDF5_ROOT}/include
            NO_DEFAULT_PATH )
    find_library( HDF5_LIBRARIES
            NAMES hdf5
            PATHS ${HDF5_ROOT}/lib
            NO_DEFAULT_PATH )
    find_library( HDF5HL_LIBRARIES
            NAMES hdf5_hl
            PATHS ${HDF5_ROOT}/lib
            NO_DEFAULT_PATH )
    if (HDF5HL_LIBRARIES)
        set(HDF5_FOUND true)
    else (HDF5HL_LIBRARIES)
        set(HDF5_FOUND false)
    endif (HDF5HL_LIBRARIES)
ELSE(HDF5_ROOT)
    # If no HDF5_ROOT variable has been set then we just
    # use the built-in FindHDF5.cmake...
    find_package(HDF5 1.10.2
            COMPONENTS C HL)
ENDIF(HDF5_ROOT)
message(STATUS "Found HDF5 libs: " ${HDF5_LIBRARIES})

include_directories(${HDF5_INCLUDE_DIRS})
add_executable(h5vds-replace-paths main.cpp)
target_link_libraries(h5vds-replace-paths ${HDF5_LIBRARIES})
install(TARGETS h5vds-replace-paths RUNTIME DESTINATION bin)
