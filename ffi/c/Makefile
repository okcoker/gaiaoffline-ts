# Makefile for C CSV Parser Library

CC = gcc
CFLAGS = -O3 -Wall -fPIC
LDFLAGS = -shared -lz

# Detect OS
UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S),Darwin)
    # macOS
    LIB_NAME = libgaia_csv_parser.dylib
    LDFLAGS += -dynamiclib
else ifeq ($(UNAME_S),Linux)
    # Linux
    LIB_NAME = libgaia_csv_parser.so
else
    # Windows (MSYS/MinGW)
    LIB_NAME = gaia_csv_parser.dll
endif

TARGET = $(LIB_NAME)
SRC = gaia_csv_parser.c

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(SRC)
	@echo "Building C CSV parser library..."
	$(CC) $(CFLAGS) $(SRC) -o $(TARGET) $(LDFLAGS)
	@echo "Built $(TARGET)"

clean:
	rm -f $(TARGET)
