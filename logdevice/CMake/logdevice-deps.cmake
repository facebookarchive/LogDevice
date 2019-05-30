# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Python version can be chosen by specifying e.g.:
set(LD_PYTHON_VERSION 3.5 CACHE STRING "Python version")
find_package(PythonInterp ${LD_PYTHON_VERSION} REQUIRED)
find_package(PythonLibs ${LD_PYTHON_VERSION} REQUIRED)
if(thriftpy3)
  find_package(Cython 0.29 REQUIRED)
endif()
set(_boost_py_component1
	    python${PYTHON_VERSION_MAJOR})
set(_boost_py_component2
	    python-py${PYTHON_VERSION_MAJOR}${PYTHON_VERSION_MINOR})

find_package(Boost 1.55.0 MODULE
  COMPONENTS
    context
    chrono
    date_time
    filesystem
    program_options
    regex
    system
    thread
    ${_boost_py_component1}
)

if(NOT Boost_FOUND)
  message(STATUS "Boost Python Component ${_boost_py_component1} not found")
  find_package(Boost 1.55.0 MODULE
    COMPONENTS
      context
      chrono
      date_time
      filesystem
      program_options
      regex
      system
      thread
      ${_boost_py_component2}
  )
  if(NOT Boost_FOUND)
    message(FATAL_ERROR "Boost Python Component ${_boost_py_component2} is also not found, terminating. At least one is required. ${Boost_ERROR_REASON}")
  else()
    message(STATUS "Boost Python Component ${_boost_py_component2} found")
  endif()
else()
  message(STATUS "Boost Python Component ${_boost_py_component1} found")
endif()

set(CMAKE_THREAD_PREFER_PTHREAD ON)
find_package(Libunwind REQUIRED)
find_package(JeMalloc REQUIRED)
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Zstd REQUIRED)
find_package(LibLZMA REQUIRED)
find_package(Libsodium REQUIRED)
find_package(LibEvent REQUIRED)
find_package(LibDl REQUIRED)
find_package(DoubleConversion REQUIRED)
find_package(Zookeeper REQUIRED)
find_package(BZip2 REQUIRED)
find_package(ZLIB REQUIRED)
find_package(Gflags REQUIRED)
find_package(Glog REQUIRED)
find_package(LZ4 REQUIRED)
find_package(Sqlite REQUIRED)
find_package(Snappy REQUIRED)
find_package(LibIberty REQUIRED)
include_directories(${LOGDEVICE_STAGING_DIR}/usr/local/include)
include_directories(${JEMALLOC_INCLUDE_DIR})
include_directories(${LIBSODIUM_INCLUDE_DIR})
include_directories(${FOLLY_INCLUDE_DIR})
include_directories(${LIBUNWIND_INCLUDE_DIR})
include_directories(${Boost_INCLUDE_DIRS})
include_directories(${JEMALLOC_INCLUDE_DIR})
include_directories(${OPENSSL_INCLUDE_DIR})
include_directories(${ZSTD_INCLUDE_DIR})
include_directories(${LIBEVENT_INCLUDE_DIR})
include_directories(${LIBDL_INCLUDE_DIRS})
include_directories(${DOUBLE_CONVERSION_INCLUDE_DIR})
include_directories(${Zookeeper_INCLUDE_DIR})
include_directories(${LIBGLOG_INCLUDE_DIR})
include_directories(${LZ4_INCLUDE_DIR})
include_directories(${SQLITE_INCLUDE_DIR})
include_directories(${BZIP2_INCLUDE_DIR})
include_directories(${ZLIB_INCLUDE_DIR})
include_directories(${SNAPPY_INCLUDE_DIR})
include_directories(${ROCKSDB_INCLUDE_DIRS})
include_directories(${LIBLZMA_INCLUDE_DIRS})
include_directories(${LIBGFLAGS_INCLUDE_DIR})
include_directories(${FBTHRIFT_INCLUDE_DIR})

# Figure out where to install the Python library
# Some packages (e.g. OpenCV) also install to dist-packages (Debian)
# instead of site-packages. We will use the default as well.
execute_process(COMMAND ${PYTHON_EXECUTABLE} -c "from distutils.sysconfig \
                      import get_python_lib; \
                      print(get_python_lib(prefix='', plat_specific=True))"
                  OUTPUT_VARIABLE _python_dist_path
                  OUTPUT_STRIP_TRAILING_WHITESPACE)
# Debian systems will return dist-packages here
# Other platforms rely on site-packages
set(_python_major_minor "${PYTHON_VERSION_MAJOR}.${PYTHON_VERSION_MINOR}")
if("${_python_dist_path}" MATCHES "site-packages")
    set(_python_dist_path "python${_python_major_minor}/site-packages")
else() #debian based assumed, install to the dist-packages.
    set(_python_dist_path "python${_python_major_minor}/dist-packages")
endif()

set(PYTHON_MODULE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}/lib/${_python_dist_path}")
