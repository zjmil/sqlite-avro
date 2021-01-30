

CFLAGS 	= -Wall -Wextra -g -std=c99
SOURCES = avro.c
OBJECTS = avro.o
LIBS 	= -lavro -lsqlite3
TARGET 	= avro.dylib

all: $(TARGET)


$(TARGET): $(OBJECTS)
	$(CC) $(CFLAGS) $(CPPFLAGS) -shared -dyanmiclib -o $@ $< $(LDFLAGS) $(LIBS)


%.o: %.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<


clean:
	$(RM) $(TARGET) $(OBJECTS)


.PHONY: clean
