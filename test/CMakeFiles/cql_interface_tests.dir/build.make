# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 2.8

#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:

# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list

# Suppress display of executed commands.
$(VERBOSE).SILENT:

# A target that is always out of date.
cmake_force:
.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/charles/cql-interface/cql-interface

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/charles/cql-interface/cql-interface

# Include any dependencies generated for this target.
include test/CMakeFiles/cql_interface_tests.dir/depend.make

# Include the progress variables for this target.
include test/CMakeFiles/cql_interface_tests.dir/progress.make

# Include the compile flags for this target's objects.
include test/CMakeFiles/cql_interface_tests.dir/flags.make

test/CMakeFiles/cql_interface_tests.dir/TestRefId.cpp.o: test/CMakeFiles/cql_interface_tests.dir/flags.make
test/CMakeFiles/cql_interface_tests.dir/TestRefId.cpp.o: test/TestRefId.cpp
	$(CMAKE_COMMAND) -E cmake_progress_report /home/charles/cql-interface/cql-interface/CMakeFiles $(CMAKE_PROGRESS_1)
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Building CXX object test/CMakeFiles/cql_interface_tests.dir/TestRefId.cpp.o"
	cd /home/charles/cql-interface/cql-interface/test && /usr/bin/c++   $(CXX_DEFINES) $(CXX_FLAGS) -o CMakeFiles/cql_interface_tests.dir/TestRefId.cpp.o -c /home/charles/cql-interface/cql-interface/test/TestRefId.cpp

test/CMakeFiles/cql_interface_tests.dir/TestRefId.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/cql_interface_tests.dir/TestRefId.cpp.i"
	cd /home/charles/cql-interface/cql-interface/test && /usr/bin/c++  $(CXX_DEFINES) $(CXX_FLAGS) -E /home/charles/cql-interface/cql-interface/test/TestRefId.cpp > CMakeFiles/cql_interface_tests.dir/TestRefId.cpp.i

test/CMakeFiles/cql_interface_tests.dir/TestRefId.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/cql_interface_tests.dir/TestRefId.cpp.s"
	cd /home/charles/cql-interface/cql-interface/test && /usr/bin/c++  $(CXX_DEFINES) $(CXX_FLAGS) -S /home/charles/cql-interface/cql-interface/test/TestRefId.cpp -o CMakeFiles/cql_interface_tests.dir/TestRefId.cpp.s

test/CMakeFiles/cql_interface_tests.dir/TestRefId.cpp.o.requires:
.PHONY : test/CMakeFiles/cql_interface_tests.dir/TestRefId.cpp.o.requires

test/CMakeFiles/cql_interface_tests.dir/TestRefId.cpp.o.provides: test/CMakeFiles/cql_interface_tests.dir/TestRefId.cpp.o.requires
	$(MAKE) -f test/CMakeFiles/cql_interface_tests.dir/build.make test/CMakeFiles/cql_interface_tests.dir/TestRefId.cpp.o.provides.build
.PHONY : test/CMakeFiles/cql_interface_tests.dir/TestRefId.cpp.o.provides

test/CMakeFiles/cql_interface_tests.dir/TestRefId.cpp.o.provides.build: test/CMakeFiles/cql_interface_tests.dir/TestRefId.cpp.o

test/CMakeFiles/cql_interface_tests.dir/TestRunner.cpp.o: test/CMakeFiles/cql_interface_tests.dir/flags.make
test/CMakeFiles/cql_interface_tests.dir/TestRunner.cpp.o: test/TestRunner.cpp
	$(CMAKE_COMMAND) -E cmake_progress_report /home/charles/cql-interface/cql-interface/CMakeFiles $(CMAKE_PROGRESS_2)
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Building CXX object test/CMakeFiles/cql_interface_tests.dir/TestRunner.cpp.o"
	cd /home/charles/cql-interface/cql-interface/test && /usr/bin/c++   $(CXX_DEFINES) $(CXX_FLAGS) -o CMakeFiles/cql_interface_tests.dir/TestRunner.cpp.o -c /home/charles/cql-interface/cql-interface/test/TestRunner.cpp

test/CMakeFiles/cql_interface_tests.dir/TestRunner.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/cql_interface_tests.dir/TestRunner.cpp.i"
	cd /home/charles/cql-interface/cql-interface/test && /usr/bin/c++  $(CXX_DEFINES) $(CXX_FLAGS) -E /home/charles/cql-interface/cql-interface/test/TestRunner.cpp > CMakeFiles/cql_interface_tests.dir/TestRunner.cpp.i

test/CMakeFiles/cql_interface_tests.dir/TestRunner.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/cql_interface_tests.dir/TestRunner.cpp.s"
	cd /home/charles/cql-interface/cql-interface/test && /usr/bin/c++  $(CXX_DEFINES) $(CXX_FLAGS) -S /home/charles/cql-interface/cql-interface/test/TestRunner.cpp -o CMakeFiles/cql_interface_tests.dir/TestRunner.cpp.s

test/CMakeFiles/cql_interface_tests.dir/TestRunner.cpp.o.requires:
.PHONY : test/CMakeFiles/cql_interface_tests.dir/TestRunner.cpp.o.requires

test/CMakeFiles/cql_interface_tests.dir/TestRunner.cpp.o.provides: test/CMakeFiles/cql_interface_tests.dir/TestRunner.cpp.o.requires
	$(MAKE) -f test/CMakeFiles/cql_interface_tests.dir/build.make test/CMakeFiles/cql_interface_tests.dir/TestRunner.cpp.o.provides.build
.PHONY : test/CMakeFiles/cql_interface_tests.dir/TestRunner.cpp.o.provides

test/CMakeFiles/cql_interface_tests.dir/TestRunner.cpp.o.provides.build: test/CMakeFiles/cql_interface_tests.dir/TestRunner.cpp.o

test/CMakeFiles/cql_interface_tests.dir/TestCassandra.cpp.o: test/CMakeFiles/cql_interface_tests.dir/flags.make
test/CMakeFiles/cql_interface_tests.dir/TestCassandra.cpp.o: test/TestCassandra.cpp
	$(CMAKE_COMMAND) -E cmake_progress_report /home/charles/cql-interface/cql-interface/CMakeFiles $(CMAKE_PROGRESS_3)
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Building CXX object test/CMakeFiles/cql_interface_tests.dir/TestCassandra.cpp.o"
	cd /home/charles/cql-interface/cql-interface/test && /usr/bin/c++   $(CXX_DEFINES) $(CXX_FLAGS) -o CMakeFiles/cql_interface_tests.dir/TestCassandra.cpp.o -c /home/charles/cql-interface/cql-interface/test/TestCassandra.cpp

test/CMakeFiles/cql_interface_tests.dir/TestCassandra.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/cql_interface_tests.dir/TestCassandra.cpp.i"
	cd /home/charles/cql-interface/cql-interface/test && /usr/bin/c++  $(CXX_DEFINES) $(CXX_FLAGS) -E /home/charles/cql-interface/cql-interface/test/TestCassandra.cpp > CMakeFiles/cql_interface_tests.dir/TestCassandra.cpp.i

test/CMakeFiles/cql_interface_tests.dir/TestCassandra.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/cql_interface_tests.dir/TestCassandra.cpp.s"
	cd /home/charles/cql-interface/cql-interface/test && /usr/bin/c++  $(CXX_DEFINES) $(CXX_FLAGS) -S /home/charles/cql-interface/cql-interface/test/TestCassandra.cpp -o CMakeFiles/cql_interface_tests.dir/TestCassandra.cpp.s

test/CMakeFiles/cql_interface_tests.dir/TestCassandra.cpp.o.requires:
.PHONY : test/CMakeFiles/cql_interface_tests.dir/TestCassandra.cpp.o.requires

test/CMakeFiles/cql_interface_tests.dir/TestCassandra.cpp.o.provides: test/CMakeFiles/cql_interface_tests.dir/TestCassandra.cpp.o.requires
	$(MAKE) -f test/CMakeFiles/cql_interface_tests.dir/build.make test/CMakeFiles/cql_interface_tests.dir/TestCassandra.cpp.o.provides.build
.PHONY : test/CMakeFiles/cql_interface_tests.dir/TestCassandra.cpp.o.provides

test/CMakeFiles/cql_interface_tests.dir/TestCassandra.cpp.o.provides.build: test/CMakeFiles/cql_interface_tests.dir/TestCassandra.cpp.o

# Object files for target cql_interface_tests
cql_interface_tests_OBJECTS = \
"CMakeFiles/cql_interface_tests.dir/TestRefId.cpp.o" \
"CMakeFiles/cql_interface_tests.dir/TestRunner.cpp.o" \
"CMakeFiles/cql_interface_tests.dir/TestCassandra.cpp.o"

# External object files for target cql_interface_tests
cql_interface_tests_EXTERNAL_OBJECTS =

test/cql_interface_tests: test/CMakeFiles/cql_interface_tests.dir/TestRefId.cpp.o
test/cql_interface_tests: test/CMakeFiles/cql_interface_tests.dir/TestRunner.cpp.o
test/cql_interface_tests: test/CMakeFiles/cql_interface_tests.dir/TestCassandra.cpp.o
test/cql_interface_tests: test/CMakeFiles/cql_interface_tests.dir/build.make
test/cql_interface_tests: test/CMakeFiles/cql_interface_tests.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --red --bold "Linking CXX executable cql_interface_tests"
	cd /home/charles/cql-interface/cql-interface/test && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/cql_interface_tests.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
test/CMakeFiles/cql_interface_tests.dir/build: test/cql_interface_tests
.PHONY : test/CMakeFiles/cql_interface_tests.dir/build

test/CMakeFiles/cql_interface_tests.dir/requires: test/CMakeFiles/cql_interface_tests.dir/TestRefId.cpp.o.requires
test/CMakeFiles/cql_interface_tests.dir/requires: test/CMakeFiles/cql_interface_tests.dir/TestRunner.cpp.o.requires
test/CMakeFiles/cql_interface_tests.dir/requires: test/CMakeFiles/cql_interface_tests.dir/TestCassandra.cpp.o.requires
.PHONY : test/CMakeFiles/cql_interface_tests.dir/requires

test/CMakeFiles/cql_interface_tests.dir/clean:
	cd /home/charles/cql-interface/cql-interface/test && $(CMAKE_COMMAND) -P CMakeFiles/cql_interface_tests.dir/cmake_clean.cmake
.PHONY : test/CMakeFiles/cql_interface_tests.dir/clean

test/CMakeFiles/cql_interface_tests.dir/depend:
	cd /home/charles/cql-interface/cql-interface && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/charles/cql-interface/cql-interface /home/charles/cql-interface/cql-interface/test /home/charles/cql-interface/cql-interface /home/charles/cql-interface/cql-interface/test /home/charles/cql-interface/cql-interface/test/CMakeFiles/cql_interface_tests.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : test/CMakeFiles/cql_interface_tests.dir/depend

