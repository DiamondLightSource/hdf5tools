cmake_minimum_required(VERSION 3.12)
project(vdstools CXX)

set(CMAKE_CXX_STANDARD 98)

# find package HDF5
# FindHDF5.cmake is essentially broken and does not allow
# to properly override the search path by setting HDF5_ROOT.
# This is a work-around to allow setting a custom HDF5
# installation location to search.
message("\nSearching for HDF5")

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

include_directories(${HDF5_INCLUDE_DIRS})
add_executable(vdstool main.cpp)
target_link_libraries(vdstool ${HDF5_LIBRARIES} )
