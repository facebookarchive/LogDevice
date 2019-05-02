# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

function(auto_sources RETURN_VALUE PATTERN SOURCE_SUBDIRS)
  if ("${SOURCE_SUBDIRS}" STREQUAL "RECURSE")
    SET(PATH ".")
    if (${ARGC} EQUAL 4)
      list(GET ARGV 3 PATH)
    endif ()
  endif()

  if ("${SOURCE_SUBDIRS}" STREQUAL "RECURSE")
    unset(${RETURN_VALUE})
    file(GLOB SUBDIR_FILES "${PATH}/${PATTERN}")
    list(APPEND ${RETURN_VALUE} ${SUBDIR_FILES})

    file(GLOB subdirs RELATIVE ${PATH} "${PATH}/*")

    foreach(DIR ${subdirs})
      if (IS_DIRECTORY ${PATH}/${DIR})
        if (NOT "${DIR}" STREQUAL "CMakeFiles")
          file(GLOB_RECURSE SUBDIR_FILES "${PATH}/${DIR}/${PATTERN}")
          list(APPEND ${RETURN_VALUE} ${SUBDIR_FILES})
        endif()
      endif()
    endforeach()
  else()
    file(GLOB ${RETURN_VALUE} "${PATTERN}")

    foreach (PATH ${SOURCE_SUBDIRS})
      file(GLOB SUBDIR_FILES "${PATH}/${PATTERN}")
      list(APPEND ${RETURN_VALUE} ${SUBDIR_FILES})
    endforeach()
  endif ()

  set(${RETURN_VALUE} ${${RETURN_VALUE}} PARENT_SCOPE)
endfunction(auto_sources)

# Remove all files matching a set of patterns, and,
# optionally, not matching a second set of patterns,
# from a set of lists.
#
# Example:
# This will remove all files in the CPP_SOURCES list
# matching "/test/" or "Test.cpp$", but not matching
# "BobTest.cpp$".
# REMOVE_MATCHES_FROM_LISTS(CPP_SOURCES MATCHES "/test/" "Test.cpp$" IGNORE_MATCHES "BobTest.cpp$")
#
# Parameters:
#
# [...]:
# The names of the lists to remove matches from.
#
# [MATCHES ...]:
# The matches to remove from the lists.
#
# [IGNORE_MATCHES ...]:
# The matches not to remove, even if they match
# the main set of matches to remove.
function(REMOVE_MATCHES_FROM_LISTS)
  set(LISTS_TO_SEARCH)
  set(MATCHES_TO_REMOVE)
  set(MATCHES_TO_IGNORE)
  set(argumentState 0)
  foreach (arg ${ARGN})
    if ("x${arg}" STREQUAL "xMATCHES")
      set(argumentState 1)
    elseif ("x${arg}" STREQUAL "xIGNORE_MATCHES")
      set(argumentState 2)
    elseif (argumentState EQUAL 0)
      list(APPEND LISTS_TO_SEARCH ${arg})
    elseif (argumentState EQUAL 1)
      list(APPEND MATCHES_TO_REMOVE ${arg})
    elseif (argumentState EQUAL 2)
      list(APPEND MATCHES_TO_IGNORE ${arg})
    else()
      message(FATAL_ERROR "Unknown argument state!")
    endif()
  endforeach()

  foreach (theList ${LISTS_TO_SEARCH})
    foreach (entry ${${theList}})
      foreach (match ${MATCHES_TO_REMOVE})
        if (${entry} MATCHES ${match})
          set(SHOULD_IGNORE OFF)
          foreach (ign ${MATCHES_TO_IGNORE})
            if (${entry} MATCHES ${ign})
              set(SHOULD_IGNORE ON)
              break()
            endif()
          endforeach()

          if (NOT SHOULD_IGNORE)
            list(REMOVE_ITEM ${theList} ${entry})
          endif()
        endif()
      endforeach()
    endforeach()
    set(${theList} ${${theList}} PARENT_SCOPE)
  endforeach()
endfunction()

# Debug included sources
#
# Output a list of SOURCES for each target in the current directory, allows
# debug of build issues related to the unexpected inclusion of sources for
# example through globbing
function(target_sources_debug)
  get_property(alltargets
               DIRECTORY .
               PROPERTY BUILDSYSTEM_TARGETS)
  foreach(target ${alltargets})
        get_target_property(source_files
                        ${target}
                        SOURCES)
    file(APPEND target_sources_debug.txt "${target}:\n")
    foreach(source_file ${source_files})
      file(APPEND target_sources_debug.txt "    ${source_file}\n")
    endforeach()
  endforeach()
endfunction()

# Create a thrift library, macro prefixed with ld_ so that it can be migrated to ThriftLibrary at
# some point in the future.

macro(ld_thrift_py3_library file_name services options file_path output_path include_prefix)

  # Parse optional arguments
  set(_py3_namespace)
  set(_dependencies)
  set(_cython_includes
    "-I${CMAKE_BINARY_DIR}/fbthrift-prefix/src/fbthrift-build/thrift/lib/py3/cybld"
  )

  set(_nextarg)
  foreach(_arg ${ARGN})
    if ("${_arg}" STREQUAL "DEPENDS")
      set(_nextarg "DEPENDS")
    elseif ("${_arg}" STREQUAL "CYTHON_INCLUDES")
      set(_nextarg "CYTHON_INCLUDES")
    elseif ("${_arg}" STREQUAL "PY3_NAMESPACE")
      set(_nextarg "PY3_NAMESPACE")
    else()
      if("${_nextarg}" STREQUAL "DEPENDS")
        list(APPEND _dependencies ${_arg})
      elseif("${_nextarg}" STREQUAL "CYTHON_INCLUDES")
        list(APPEND _cython_includes "-I${_arg}")
      elseif("${_nextarg}" STREQUAL "PY3_NAMESPACE")
        set(_py3_namespace "${_arg}/")
        set(_nextarg)
      else()
        message(FATAL_ERROR "Unexpected parameter '${_arg}' in "
          "ld_thrift_py3_library call")
      endif()
    endif()
  endforeach()

  thrift_library(
    "${file_name}"
    "${services}"
    "cpp2"
    "${options}"
    "${file_path}"
    "${output_path}"
    "${include_prefix}"
    THRIFT_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/.."
  )

  if(thriftpy3)
    set_target_properties(
      ${file_name}-cpp2-obj
      PROPERTIES POSITION_INDEPENDENT_CODE True
    )

    thrift_generate(
      "${file_name}"
      "${services}"
      "py3"
      "${options}"
      "${file_path}"
      "${output_path}"
      "${include_prefix}"
      THRIFT_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/.."
    )

    # TODO: Why not read the py3_namespace from the thrift source file?

    # Creation of this file in ThriftLibrary does not account for name space;
    # I think use of thrift generate for py3 might need to be deprecated.
    file(WRITE "${output_path}/gen-py3/${_py3_namespace}${file_name}/__init__.py")

    add_dependencies(${file_name}-py3-target ${file_name}-cpp2-target)

    set(_cython_modules "types")
    if(NOT "${services}" STREQUAL "")
      list(APPEND _cython_modules "clients")
    endif()

    foreach(_src ${_cython_modules})
      set(_pyx "gen-py3/${_py3_namespace}${file_name}/${_src}.pyx")
      set(_cxx "gen-py3/${_py3_namespace}${file_name}/${_src}.cpp")
      string(REPLACE "/" "-" _module_name "${_py3_namespace}${file_name}/${_src}-py3")
      message(STATUS "Create Cython module ${_module_name} from ${_pyx}")

      set(_generated_module_sources "${output_path}/${_cxx}")
      if(NOT ${_src} STREQUAL "types")
        list(APPEND _generated_module_sources
          "${output_path}/gen-py3/${file_name}/${_src}_wrapper.cpp"
        )
        set_source_files_properties(
          "${output_path}/gen-py3/${file_name}/${_src}_wrapper.cpp"
          PROPERTIES GENERATED TRUE
        )
      endif()

      add_custom_command(OUTPUT "${output_path}/${_cxx}"
        COMMAND ${CYTHON_EXE} --fast-fail -3 --cplus ${_pyx} -o ${_cxx}
          ${_cython_includes}
        COMMENT "Generating ${_cxx} using Cython"
        WORKING_DIRECTORY "${output_path}"
      )

      python_add_module(${_module_name} ${_generated_module_sources})
      if(${BUILD_SUBMODULES})
        target_link_libraries(${_module_name}
          PRIVATE
          "${file_name}-cpp2"
          thriftcpp2_shared
          folly_pic
        )
      else()
        target_link_libraries(${_module_name}
          PRIVATE
          "${file_name}-cpp2"
          FBThrift::thriftcpp2_shared
          Folly::folly_pic
        )
      endif()
      target_include_directories(${_module_name} PUBLIC ${PYTHON_INCLUDE_DIRS})
      set_target_properties(${_module_name}
        PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY
          "${output_path}/gen-py3/${_py3_namespace}${file_name}"
        LIBRARY_OUTPUT_NAME ${_src})
    endforeach()
  endif() # thriftpy3
endmacro()
