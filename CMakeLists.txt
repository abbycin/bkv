cmake_minimum_required(VERSION 3.25 FATAL_ERROR)
project(bkv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if (${CMAKE_CXX_COMPILER_ID} STREQUAL "GNU")
        set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -Wall -Wextra")
endif()

add_executable(bkv
        utils.h
        meta_types.h
        cache.h
        meta.h
        bptree.h
        bkv.h
        bkv.cc
)