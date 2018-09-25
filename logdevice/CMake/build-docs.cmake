add_custom_target(settings.md
	COMMAND logdeviced --markdown-settings > settings.md
	WORKING_DIRECTORY ${LOGDEVICE_DIR}/../docs
	DEPENDS logdeviced)

add_custom_target(ldquery.md
	COMMAND markdown-ldquery > ldquery.md
	WORKING_DIRECTORY ${LOGDEVICE_DIR}/../docs
	DEPENDS markdown-ldquery)

add_custom_target(docs)
add_dependencies(docs settings.md ldquery.md)
