

CFLAGS 	:= -Wall -Wextra -Wno-unused-parameter -g -std=c99 -I/opt/homebrew/include 
SOURCES := avro.c
OBJECTS := avro.o
LIBS 	:= -lavro -lsqlite3 -L/opt/homebrew/lib
TARGET 	:= avro.dylib

all: $(TARGET)


$(TARGET): $(OBJECTS)
	$(CC) $(CFLAGS) $(CPPFLAGS) -shared -dyanmiclib -o $@ $< $(LDFLAGS) $(LIBS)


%.o: %.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<

test: $(TARGET)
	/opt/homebrew/bin/python3 -m unittest discover

format:
	clang-format -i $(SOURCES)

clean:
	$(RM) -rf $(TARGET) $(OBJECTS) avro.dylib.dSYM/


.PHONY: format clean
