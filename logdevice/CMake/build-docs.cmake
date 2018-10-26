# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_custom_target(settings.md
	COMMAND logdeviced --markdown-settings > settings.md
	WORKING_DIRECTORY ${LOGDEVICE_DIR}/../docs
	DEPENDS logdeviced)

add_custom_target(ldquery.md
	COMMAND markdown-ldquery > ldquery.md
	WORKING_DIRECTORY ${LOGDEVICE_DIR}/../docs
	DEPENDS markdown-ldquery)

add_custom_target(client-api-doxy
	COMMAND rm -rf website/static/api && doxygen logdevice/Doxyfile
	WORKING_DIRECTORY ${LOGDEVICE_DIR}/..)

add_custom_target(docs)
add_dependencies(docs settings.md ldquery.md client-api-doxy)
