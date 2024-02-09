cmake_minimum_required(VERSION 3.18..3.20)

project(MiniDatabase)

# find_package(boost REQUIRED)

add_executable(MyApp src/block_handle.cpp src/block_info.cpp src/buffer_manager.cpp src/catalog_manager.cpp src/file_handle.cpp 
               src/file_info.cpp src/index_manager.cpp src/interpreter.cpp src/main.cpp src/minidb_api.cpp src/record_manager.cpp src/sql_statement.cpp)

target_sources(MyApp PRIVATE src/block_handle.h src/block_info.h src/buffer_manager.h src/catalog_manager.h src/commons.h src/exceptions.h
               src/file_handle.h src/file_info.h src/index_manager.h src/interpreter.h src/minidb_api.h src/record_manager.h src/sql_statement.h)   

# target_link_libraries(MyApp PUBLIC boost)


find_package(Boost REQUIRED COMPONENTS filesystem regex serialization) # Add 'serialization' here
include_directories(${Boost_INCLUDE_DIRS})
target_link_libraries(MyApp ${Boost_LIBRARIES})
target_link_libraries(MyApp readline)
