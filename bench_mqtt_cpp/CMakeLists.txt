cmake_minimum_required(VERSION 3.10)
project(mqtt_cpp_bench LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(CheckIncludeFile)

find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
  pkg_check_modules(MOSQUITTO QUIET libmosquitto)
endif()

if(MOSQUITTO_FOUND)
  message(STATUS "Found libmosquitto via pkg-config")
else()
  find_path(MOSQUITTO_INCLUDE_DIR
    NAMES mosquitto.h
  )
  find_library(MOSQUITTO_LIBRARY
    NAMES mosquitto
  )

  if(NOT MOSQUITTO_INCLUDE_DIR OR NOT MOSQUITTO_LIBRARY)
    message(FATAL_ERROR
      "libmosquitto not found.\n"
      "Ubuntu/Debian: sudo apt install mosquitto mosquitto-clients libmosquitto-dev\n"
      "macOS/Homebrew: brew install mosquitto\n"
      "If installed to a custom prefix, set -DCMAKE_PREFIX_PATH=/path/to/prefix or\n"
      "MOSQUITTO_INCLUDE_DIR / MOSQUITTO_LIBRARY accordingly.")
  endif()
endif()

add_executable(bench_mqtt_echo_ack
  src/bench_mqtt_echo_ack.cpp
)

add_executable(bench_mqtt_pub_rtt
  src/bench_mqtt_pub_rtt.cpp
)

if(MOSQUITTO_FOUND)
  target_link_libraries(bench_mqtt_echo_ack PRIVATE PkgConfig::MOSQUITTO)
  target_link_libraries(bench_mqtt_pub_rtt PRIVATE PkgConfig::MOSQUITTO)
else()
  target_include_directories(bench_mqtt_echo_ack PRIVATE "${MOSQUITTO_INCLUDE_DIR}")
  target_include_directories(bench_mqtt_pub_rtt PRIVATE "${MOSQUITTO_INCLUDE_DIR}")

  target_link_libraries(bench_mqtt_echo_ack PRIVATE "${MOSQUITTO_LIBRARY}")
  target_link_libraries(bench_mqtt_pub_rtt PRIVATE "${MOSQUITTO_LIBRARY}")
endif()

target_include_directories(bench_mqtt_echo_ack PRIVATE
  "${CMAKE_CURRENT_SOURCE_DIR}/src"
)

target_include_directories(bench_mqtt_pub_rtt PRIVATE
  "${CMAKE_CURRENT_SOURCE_DIR}/src"
)

