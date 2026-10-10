#ifndef __NVDSINFER_CUSTOM_YOLOV11_H__
#define __NVDSINFER_CUSTOM_YOLOV11_H__

#include <vector>
#include <string>

#include "nvdsinfer_custom_impl.h"

// Network input dimensions
#define YOLO_INPUT_WIDTH 640
#define YOLO_INPUT_HEIGHT 640

// PPE model has 10 classes
#define NUM_CLASSES 10

// YOLO 640x640 prediction locations
#define NUM_BOXES 8400

// 4 bbox values + 10 class scores
//
// Model output:
// [1, 14, 8400]
#define NUM_OUTPUT_ATTRS 14


extern "C" bool NvDsInferParseYoloV11(
    std::vector<NvDsInferLayerInfo> const& outputLayersInfo,
    NvDsInferNetworkInfo const& networkInfo,
    NvDsInferParseDetectionParams const& detectionParams,
    std::vector<NvDsInferObjectDetectionInfo>& objectList
);


#endif