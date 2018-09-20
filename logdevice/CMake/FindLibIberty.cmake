# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

FIND_LIBRARY(IBERTY_LIBRARIES
        NAMES iberty_pic iberty)

IF (IBERTY_LIBRARIES)

   # show which libiberty was found only if not quiet
   MESSAGE( STATUS "Found libiberty: ${IBERTY_LIBRARIES}")

   SET(IBERTY_FOUND TRUE)

ELSE (IBERTY_LIBRARIES)

   IF ( IBERTY_FIND_REQUIRED)
      MESSAGE(FATAL_ERROR "Could not find libiberty. Try to install binutil-devel?")
   ELSE()
      MESSAGE(STATUS "Could not find libiberty; downloading binutils and building PIC libiberty.")
   ENDIF (IBERTY_FIND_REQUIRED)

ENDIF (IBERTY_LIBRARIES)

