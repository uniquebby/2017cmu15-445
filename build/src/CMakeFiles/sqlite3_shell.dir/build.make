# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.14

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


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
CMAKE_SOURCE_DIR = /home/bs/learning/15445/cmu_15445_2018

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/bs/learning/15445/cmu_15445_2018/build

# Include any dependencies generated for this target.
include src/CMakeFiles/sqlite3_shell.dir/depend.make

# Include the progress variables for this target.
include src/CMakeFiles/sqlite3_shell.dir/progress.make

# Include the compile flags for this target's objects.
include src/CMakeFiles/sqlite3_shell.dir/flags.make

src/CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.o: src/CMakeFiles/sqlite3_shell.dir/flags.make
src/CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.o: ../src/sqlite/shell.c
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/bs/learning/15445/cmu_15445_2018/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building C object src/CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.o"
	cd /home/bs/learning/15445/cmu_15445_2018/build/src && /usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -o CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.o   -c /home/bs/learning/15445/cmu_15445_2018/src/sqlite/shell.c

src/CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing C source to CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.i"
	cd /home/bs/learning/15445/cmu_15445_2018/build/src && /usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -E /home/bs/learning/15445/cmu_15445_2018/src/sqlite/shell.c > CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.i

src/CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling C source to assembly CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.s"
	cd /home/bs/learning/15445/cmu_15445_2018/build/src && /usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -S /home/bs/learning/15445/cmu_15445_2018/src/sqlite/shell.c -o CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.s

# Object files for target sqlite3_shell
sqlite3_shell_OBJECTS = \
"CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.o"

# External object files for target sqlite3_shell
sqlite3_shell_EXTERNAL_OBJECTS =

bin/sqlite3: src/CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.o
bin/sqlite3: src/CMakeFiles/sqlite3_shell.dir/build.make
bin/sqlite3: lib/libsqlite3.so
bin/sqlite3: src/CMakeFiles/sqlite3_shell.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/bs/learning/15445/cmu_15445_2018/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking C executable ../bin/sqlite3"
	cd /home/bs/learning/15445/cmu_15445_2018/build/src && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/sqlite3_shell.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
src/CMakeFiles/sqlite3_shell.dir/build: bin/sqlite3

.PHONY : src/CMakeFiles/sqlite3_shell.dir/build

src/CMakeFiles/sqlite3_shell.dir/clean:
	cd /home/bs/learning/15445/cmu_15445_2018/build/src && $(CMAKE_COMMAND) -P CMakeFiles/sqlite3_shell.dir/cmake_clean.cmake
.PHONY : src/CMakeFiles/sqlite3_shell.dir/clean

src/CMakeFiles/sqlite3_shell.dir/depend:
	cd /home/bs/learning/15445/cmu_15445_2018/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/bs/learning/15445/cmu_15445_2018 /home/bs/learning/15445/cmu_15445_2018/src /home/bs/learning/15445/cmu_15445_2018/build /home/bs/learning/15445/cmu_15445_2018/build/src /home/bs/learning/15445/cmu_15445_2018/build/src/CMakeFiles/sqlite3_shell.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : src/CMakeFiles/sqlite3_shell.dir/depend

