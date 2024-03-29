

CFLAGS 	:= -Wall -Wextra -Wno-unused-parameter -g -std=c99
SOURCES := avro.c
OBJECTS := avro.o
LIBS 	:= -lavro -lsqlite3
TARGET 	:= avro.dylib

all: $(TARGET)


$(TARGET): $(OBJECTS)
	$(CC) $(CFLAGS) $(CPPFLAGS) -shared -dyanmiclib -o $@ $< $(LDFLAGS) $(LIBS)


%.o: %.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<

format:
	clang-format -i $(SOURCES)

clean:
	$(RM) $(TARGET) $(OBJECTS)


.PHONY: format clean
