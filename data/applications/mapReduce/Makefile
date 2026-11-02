CXX = g++
CXXFLAGS = -Wall -ggdb3 -std=c++14 -pthread

TARGET = HW4_MapReduce
SOURCES = HW4_MapReduce.cpp
OBJECTS = $(SOURCES:.cpp=.o)

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CXX) $(CXXFLAGS) $(OBJECTS) -o $@

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(TARGET) $(OBJECTS)

