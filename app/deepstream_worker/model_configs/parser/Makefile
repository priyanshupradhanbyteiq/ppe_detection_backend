################################################################################
# DeepStream Custom YOLOv11 Parser
################################################################################

CUDA_VER ?= 12.6

DS_PATH := /opt/nvidia/deepstream/deepstream-7.1

CXX := g++

TARGET := libnvdsinfer_custom_yolov11.so
SRC := nvdsinfer_custom_yolov11.cpp

CFLAGS := -Wall \
           -std=c++17 \
           -O2 \
           -fPIC

INCLUDES := \
    -I$(DS_PATH)/sources/includes \
    -I/usr/local/cuda/include

LIBS := \
    -L/usr/local/cuda/lib64 \
    -lcudart

all:
	$(CXX) $(CFLAGS) $(INCLUDES) -shared $(SRC) -o $(TARGET) $(LIBS)

clean:
	rm -f $(TARGET)