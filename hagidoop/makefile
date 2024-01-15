# Makefile for Hagidoop

# Directories
SRC_DIR := src
BIN_DIR := bin
CONFIG_DIR := config
DATA_DIR := data
DOC_DIR := doc
SCRIPTS_DIR := scripts
APPLICATION_DIR := $(SRC_DIR)/application
CONFIG_SRC_DIR := $(SRC_DIR)/config
DAEMON_DIR := $(SRC_DIR)/daemon
HDFS_DIR := $(SRC_DIR)/hdfs
INTERFACE_DIR := $(SRC_DIR)/interface
IO_DIR := $(SRC_DIR)/io

# Java Compiler
JAVAC := javac
JAVAC_FLAGS := -d $(BIN_DIR) -cp $(BIN_DIR)

# Java Source Files
APPLICATION_SRCS := $(wildcard $(APPLICATION_DIR)/*.java)
CONFIG_SRCS := $(wildcard $(CONFIG_SRC_DIR)/*.java)
DAEMON_SRCS := $(wildcard $(DAEMON_DIR)/*.java)
HDFS_SRCS := $(wildcard $(HDFS_DIR)/*.java)
INTERFACE_SRCS := $(wildcard $(INTERFACE_DIR)/*.java)
IO_SRCS := $(wildcard $(IO_DIR)/*.java)

# Compiled Java Classes
APPLICATION_CLASSES := $(patsubst $(APPLICATION_DIR)/%.java,$(BIN_DIR)/%.class,$(APPLICATION_SRCS))
CONFIG_CLASSES := $(patsubst $(CONFIG_SRC_DIR)/%.java,$(BIN_DIR)/%.class,$(CONFIG_SRCS))
DAEMON_CLASSES := $(patsubst $(DAEMON_DIR)/%.java,$(BIN_DIR)/%.class,$(DAEMON_SRCS))
HDFS_CLASSES := $(patsubst $(HDFS_DIR)/%.java,$(BIN_DIR)/%.class,$(HDFS_SRCS))
INTERFACE_CLASSES := $(patsubst $(INTERFACE_DIR)/%.java,$(BIN_DIR)/%.class,$(INTERFACE_SRCS))
IO_CLASSES := $(patsubst $(IO_DIR)/%.java,$(BIN_DIR)/%.class,$(IO_SRCS))

# Targets
all: $(APPLICATION_CLASSES) $(CONFIG_CLASSES) $(DAEMON_CLASSES) $(HDFS_CLASSES) $(INTERFACE_CLASSES) $(IO_CLASSES)

# Compile Application Java Classes
$(BIN_DIR)/%.class: $(APPLICATION_DIR)/%.java
	$(JAVAC) $(JAVAC_FLAGS) $<

# Compile Config Java Classes
$(BIN_DIR)/%.class: $(CONFIG_SRC_DIR)/%.java
	$(JAVAC) $(JAVAC_FLAGS) $<

# Compile Daemon Java Classes
$(BIN_DIR)/%.class: $(DAEMON_DIR)/%.java
	$(JAVAC) $(JAVAC_FLAGS) $<

# Compile HDFS Java Classes
$(BIN_DIR)/%.class: $(HDFS_DIR)/%.java
	$(JAVAC) $(JAVAC_FLAGS) $<

# Compile Interface Java Classes
$(BIN_DIR)/%.class: $(INTERFACE_DIR)/%.java
	$(JAVAC) $(JAVAC_FLAGS) $<

# Compile IO Java Classes
$(BIN_DIR)/%.class: $(IO_DIR)/%.java
	$(JAVAC) $(JAVAC_FLAGS) $<

# Clean compiled classes
clean:
	rm -rf $(BIN_DIR)/*
