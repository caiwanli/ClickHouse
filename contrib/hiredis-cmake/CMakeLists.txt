set(HIREDIS_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/hiredis")
set(HIREDIS_SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/hiredis")
set(HIREDIS_SRC
    ${HIREDIS_SRC_DIR}/alloc.c
    ${HIREDIS_SRC_DIR}/async.c
    ${HIREDIS_SRC_DIR}/dict.c
    ${HIREDIS_SRC_DIR}/hiredis.c
    ${HIREDIS_SRC_DIR}/net.c
    ${HIREDIS_SRC_DIR}/read.c
    ${HIREDIS_SRC_DIR}/sds.c
    ${HIREDIS_SRC_DIR}/sockcompat.c
    ${HIREDIS_SRC_DIR}/RedisClient.cpp)
add_library(hiredis ${HIREDIS_SRC})
target_include_directories(hiredis SYSTEM PUBLIC "${HIREDIS_INCLUDE_DIR}" PRIVATE "${HIREDIS_SRC_DIR}")