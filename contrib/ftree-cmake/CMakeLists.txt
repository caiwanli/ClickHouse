set(FTREE_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/ftree")
set(FTREE_SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/ftree")
set(FTREE_SRC
    ${FTREE_SRC_DIR}/ast.cc
    ${FTREE_SRC_DIR}/filter.tab.cc
    ${FTREE_SRC_DIR}/filter.yy.cc
    ${FTREE_SRC_DIR}/FilterTree.cc
    ${FTREE_SRC_DIR}/parser.cc)
add_library(ftree ${FTREE_SRC})
target_include_directories(ftree SYSTEM PUBLIC "${FTREE_INCLUDE_DIR}" PRIVATE "${FTREE_SRC_DIR}")