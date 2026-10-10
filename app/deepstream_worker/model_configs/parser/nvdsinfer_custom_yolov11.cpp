/* =========================================================================
 * nvdsinfer_custom_yolov11.cpp  (v2 - self-verifying)
 *
 * Output layout is assumed to be YOLOv8/v11-style:
 *      [NUM_OUTPUT_ATTRS][NUM_BOXES]  ==  [18][8400]
 *      rows 0..3   -> box coords (format UNCONFIRMED, see below)
 *      rows 4..17  -> per-class scores (14 classes, sigmoid state UNCONFIRMED)
 *
 * Three things could NOT be verified without the actual .onnx/.engine file
 * and are handled as runtime-checked config below instead of blind guesses:
 *
 *   1. BOX_FORMAT       -> CXCYWH vs XYXY
 *   2. APPLY_SIGMOID     -> raw logits vs already-sigmoided scores
 *   3. OUTPUT_LAYOUT     -> [ATTRS][BOXES] vs [BOXES][ATTRS]
 *
 * HOW TO RESOLVE THEM PROPERLY (2 minutes, do this once):
 *   Open your .onnx in https://netron.app, click the final output node,
 *   and check:
 *     - Its shape: [1,18,8400] confirms ATTRS-major; [1,8400,18] confirms
 *       BOXES-major. The code below auto-detects this from inferDims,
 *       so you don't need to hardcode it - just check the log line it
 *       prints on first inference to confirm it detected correctly.
 *     - Trace the last op before output: if it's Sigmoid -> scores are
 *       pre-sigmoided (APPLY_SIGMOID_TO_CLASS_SCORES = false). If the last
 *       op on the class branch is Conv/Reshape/Concat with no Sigmoid ->
 *       raw logits (set it to true).
 *     - Trace the box branch: if you see Sub/Add forming (center - wh/2,
 *       center + wh/2) before the final Concat -> output is already XYXY.
 *       If it ends right after the Mul (stride * offset) without that
 *       Sub/Add pair -> output is CXCYWH.
 *   Until you've checked, this file defaults to CXCYWH + pre-sigmoided
 *   (the Ultralytics default export path) but LOGS raw value ranges on
 *   the first frame so you can sanity-check against the notes below.
 * ========================================================================= */

#include "nvdsinfer_custom_yolov8.h"

#include <algorithm>
#include <atomic>
#include <mutex>
#include <cmath>
#include <cstring>
#include <iostream>
#include <limits>

/* -------------------------------------------------------------------------
 * Config knobs - VERIFY THESE IN NETRON (see header comment above)
 * ---------------------------------------------------------------------- */

enum class BoxFormat { CXCYWH, XYXY };

// Default assumption: Ultralytics standard export (center-form boxes,
// class scores already through sigmoid). Flip if Netron/logs disagree.
static const BoxFormat kBoxFormat = BoxFormat::CXCYWH;
#define APPLY_SIGMOID_TO_CLASS_SCORES false

// IoU threshold for NMS.
static const float kNmsIouThreshold = 0.45f;

// Cap on proposals fed into NMS (post confidence-threshold, pre-NMS),
// to bound the O(n^2) NMS cost. 300 is generous for 14 classes.
static const int kMaxProposalsForNms = 300;

/* -------------------------------------------------------------------------
 * Helper structs
 * ---------------------------------------------------------------------- */

struct BBoxInfo
{
    float x1, y1, x2, y2;
    float score;
    int   classId;
};

/* -------------------------------------------------------------------------
 * Sigmoid / IoU utilities
 * ---------------------------------------------------------------------- */

static inline float sigmoid(float x)
{
    return 1.0f / (1.0f + std::exp(-x));
}

static inline float clamp(float val, float lo, float hi)
{
    return std::max(lo, std::min(val, hi));
}

static float computeIoU(const BBoxInfo& a, const BBoxInfo& b)
{
    const float interX1 = std::max(a.x1, b.x1);
    const float interY1 = std::max(a.y1, b.y1);
    const float interX2 = std::min(a.x2, b.x2);
    const float interY2 = std::min(a.y2, b.y2);

    const float interW = std::max(0.0f, interX2 - interX1);
    const float interH = std::max(0.0f, interY2 - interY1);
    const float interArea = interW * interH;

    const float areaA = std::max(0.0f, a.x2 - a.x1) * std::max(0.0f, a.y2 - a.y1);
    const float areaB = std::max(0.0f, b.x2 - b.x1) * std::max(0.0f, b.y2 - b.y1);

    const float unionArea = areaA + areaB - interArea;
    if (unionArea <= 0.0f) return 0.0f;

    return interArea / unionArea;
}

/* -------------------------------------------------------------------------
 * NMS implementation (class-wise, greedy, highest score first)
 * ---------------------------------------------------------------------- */

static void nonMaximumSuppression(std::vector<BBoxInfo>& binfo, float iouThreshold)
{
    std::stable_sort(binfo.begin(), binfo.end(),
                      [](const BBoxInfo& a, const BBoxInfo& b) {
                          return a.score > b.score;
                      });

    std::vector<BBoxInfo> result;
    result.reserve(binfo.size());
    std::vector<bool> suppressed(binfo.size(), false);

    for (size_t i = 0; i < binfo.size(); ++i)
    {
        if (suppressed[i]) continue;
        result.push_back(binfo[i]);

        for (size_t j = i + 1; j < binfo.size(); ++j)
        {
            if (suppressed[j]) continue;
            if (computeIoU(binfo[i], binfo[j]) > iouThreshold)
                suppressed[j] = true;
        }
    }

    binfo = std::move(result);
}

static std::vector<BBoxInfo> nmsAllClasses(std::vector<BBoxInfo>& detections, float iouThreshold)
{
    std::vector<std::vector<BBoxInfo>> perClass(NUM_CLASSES);

    for (auto& det : detections)
        if (det.classId >= 0 && det.classId < NUM_CLASSES)
            perClass[det.classId].push_back(det);

    std::vector<BBoxInfo> merged;
    for (int c = 0; c < NUM_CLASSES; ++c)
    {
        if (perClass[c].empty()) continue;
        nonMaximumSuppression(perClass[c], iouThreshold);
        merged.insert(merged.end(), perClass[c].begin(), perClass[c].end());
    }
    return merged;
}

/* -------------------------------------------------------------------------
 * Box decoding
 * ---------------------------------------------------------------------- */

// Handles both CXCYWH and XYXY source formats; always emits clipped
// corner coords in network-pixel space. Invalid (non-positive area)
// boxes are rejected rather than passed through.
static inline bool decodeBox(
    float a, float b, float c, float d,
    BoxFormat fmt,
    const NvDsInferNetworkInfo& networkInfo,
    float& x1, float& y1, float& x2, float& y2)
{
    if (fmt == BoxFormat::CXCYWH)
    {
        const float cx = a, cy = b, w = c, h = d;
        if (w <= 0.0f || h <= 0.0f) return false;
        x1 = cx - w * 0.5f;
        y1 = cy - h * 0.5f;
        x2 = cx + w * 0.5f;
        y2 = cy + h * 0.5f;
    }
    else // XYXY
    {
        x1 = a; y1 = b; x2 = c; y2 = d;
        if (x2 <= x1 || y2 <= y1) return false;
    }

    x1 = clamp(x1, 0.0f, static_cast<float>(networkInfo.width - 1));
    y1 = clamp(y1, 0.0f, static_cast<float>(networkInfo.height - 1));
    x2 = clamp(x2, 0.0f, static_cast<float>(networkInfo.width - 1));
    y2 = clamp(y2, 0.0f, static_cast<float>(networkInfo.height - 1));

    // After clamping, re-check for degenerate boxes (e.g. fully outside frame).
    if (x2 <= x1 || y2 <= y1) return false;

    return true;
}

/* -------------------------------------------------------------------------
 * Confidence calculation
 * ---------------------------------------------------------------------- */

// attrStride: distance (in elements) between consecutive attributes for
// the SAME box, given the detected memory layout.
// boxStride: distance between consecutive box indices for the SAME attribute.
static inline void getBestClass(
    const float* output,
    int boxIdx,
    int attrStride,
    int boxStride,
    int& bestClassId,
    float& bestScore)
{
    bestClassId = -1;
    bestScore   = -std::numeric_limits<float>::infinity();

    for (int c = 0; c < NUM_CLASSES; ++c)
    {
        const int attrIdx = 4 + c;
        const float raw = output[attrIdx * attrStride + boxIdx * boxStride];

#if APPLY_SIGMOID_TO_CLASS_SCORES
        const float score = sigmoid(raw);
#else
        const float score = raw;
#endif
        if (score > bestScore)
        {
            bestScore   = score;
            bestClassId = c;
        }
    }
}

/* -------------------------------------------------------------------------
 * Convert to NvDsInferObjectDetectionInfo
 * ---------------------------------------------------------------------- */

static void addBBoxProposal(
    const BBoxInfo& b,
    std::vector<NvDsInferObjectDetectionInfo>& objectList)
{
    NvDsInferObjectDetectionInfo obj;
    obj.classId = b.classId;
    obj.detectionConfidence = b.score;
    obj.left   = b.x1;
    obj.top    = b.y1;
    obj.width  = b.x2 - b.x1;
    obj.height = b.y2 - b.y1;
    objectList.push_back(obj);
}

/* -------------------------------------------------------------------------
 * Output layer discovery + layout detection
 * ---------------------------------------------------------------------- */

struct LayoutInfo
{
    const float* data = nullptr;
    int numBoxes = 0;
    int numAttrs = 0;
    int attrStride = 0; // elements between attrs for a fixed box
    int boxStride  = 0; // elements between boxes for a fixed attr
    bool valid = false;
};

// Finds the layer that plausibly holds detections and figures out
// whether it's [ATTRS][BOXES] or [BOXES][ATTRS] from its own dims,
// rather than assuming index 0 or a fixed layout.
static LayoutInfo resolveOutputLayout(std::vector<NvDsInferLayerInfo> const& outputLayersInfo)
{
    LayoutInfo info;

    for (auto const& layer : outputLayersInfo)
    {
        if (layer.buffer == nullptr) continue;

        const auto& dims = layer.inferDims;
        // Expect 2 meaningful dims (batch dim usually stripped by nvinfer
        // for per-layer inferDims): d[0] x d[1].
        if (dims.numDims < 2) continue;

        int d0 = dims.d[0];
        int d1 = dims.d[1];

        bool attrsMajor = (d0 == NUM_OUTPUT_ATTRS && d1 == NUM_BOXES);
        bool boxesMajor = (d0 == NUM_BOXES && d1 == NUM_OUTPUT_ATTRS);

        if (!attrsMajor && !boxesMajor) continue; // not the layer we want

        info.data = reinterpret_cast<const float*>(layer.buffer);
        info.numAttrs = NUM_OUTPUT_ATTRS;
        info.numBoxes = NUM_BOXES;

        if (attrsMajor)
        {
            // memory: attr0[box0..N], attr1[box0..N], ...
            info.attrStride = NUM_BOXES; // step to next attr row
            info.boxStride  = 1;         // step to next box within a row
            static std::once_flag layoutLogged;
            std::call_once(layoutLogged, [&] {
                std::cout << "[NvDsInferParseYoloV11] Detected layer '" << layer.layerName
                          << "' as ATTRS-major [" << d0 << "][" << d1 << "]\n";
            });
        }
        else
        {
            // memory: box0[attr0..17], box1[attr0..17], ...
            info.attrStride = 1;
            info.boxStride  = NUM_OUTPUT_ATTRS;
            static std::once_flag layoutLogged;
            std::call_once(layoutLogged, [&] {
                std::cout << "[NvDsInferParseYoloV11] Detected layer '" << layer.layerName
                          << "' as BOXES-major [" << d0 << "][" << d1 << "]\n";
            });
        }

        info.valid = true;
        return info;
    }

    return info; // .valid == false
}

/* -------------------------------------------------------------------------
 * NvDsInferParseYoloV11()
 * ---------------------------------------------------------------------- */

extern "C" bool NvDsInferParseYoloV11(
    std::vector<NvDsInferLayerInfo> const& outputLayersInfo,
    NvDsInferNetworkInfo const& networkInfo,
    NvDsInferParseDetectionParams const& detectionParams,
    std::vector<NvDsInferObjectDetectionInfo>& objectList)
{
    static std::atomic<bool> loggedOnce{false};

    if (outputLayersInfo.empty())
    {
        std::cerr << "ERROR: NvDsInferParseYoloV11 -- no output layers found." << std::endl;
        return false;
    }

    LayoutInfo layout = resolveOutputLayout(outputLayersInfo);
    if (!layout.valid)
    {
        std::cerr << "ERROR: NvDsInferParseYoloV11 -- could not find an output layer "
                  << "matching " << NUM_OUTPUT_ATTRS << "x" << NUM_BOXES
                  << " (in either order). Check NUM_CLASSES/NUM_OUTPUT_ATTRS/NUM_BOXES "
                  << "against your actual model dims." << std::endl;
        for (auto const& layer : outputLayersInfo)
        {
            std::cerr << "  layer '" << layer.layerName << "' dims: [";
            for (int i = 0; i < layer.inferDims.numDims; ++i)
                std::cerr << layer.inferDims.d[i] << (i + 1 < layer.inferDims.numDims ? "," : "");
            std::cerr << "]" << std::endl;
        }
        return false;
    }

    const float* output = layout.data;

    if (!loggedOnce.exchange(true))
    {
        // One-time diagnostics to help confirm box format / sigmoid state
        // without needing to reopen Netron.
        float rawMin =  std::numeric_limits<float>::infinity();
        float rawMax = -std::numeric_limits<float>::infinity();
        float boxCoordMax = -std::numeric_limits<float>::infinity();

        for (int i = 0; i < layout.numBoxes; ++i)
        {
            for (int c = 0; c < NUM_CLASSES; ++c)
            {
                float v = output[(4 + c) * layout.attrStride + i * layout.boxStride];
                rawMin = std::min(rawMin, v);
                rawMax = std::max(rawMax, v);
            }
            for (int a = 0; a < 4; ++a)
            {
                float v = output[a * layout.attrStride + i * layout.boxStride];
                boxCoordMax = std::max(boxCoordMax, v);
            }
        }

        std::cout << "[NvDsInferParseYoloV11] network=" << networkInfo.width << "x"
                  << networkInfo.height
                  << " boxes=" << layout.numBoxes << " attrs=" << layout.numAttrs
                  << std::endl;
        std::cout << "[NvDsInferParseYoloV11] class-score raw range: [" << rawMin
                  << ", " << rawMax << "]"
                  << (rawMin >= -0.01f && rawMax <= 1.01f
                          ? "  -> looks ALREADY SIGMOIDED (0..1), current setting OK"
                          : "  -> looks like RAW LOGITS, set APPLY_SIGMOID_TO_CLASS_SCORES=true")
                  << std::endl;
        std::cout << "[NvDsInferParseYoloV11] max box coord value: " << boxCoordMax
                  << " (network dim is " << networkInfo.width << ") -- if this is well "
                  << "under network width even for large objects, or values look like "
                  << "x2/y2 > x1/y1 pairs, double check BOX_FORMAT against Netron."
                  << std::endl;
    }

    std::vector<BBoxInfo> proposals;
    proposals.reserve(layout.numBoxes);

    for (int i = 0; i < layout.numBoxes; ++i)
    {
        int   bestClassId = -1;
        float bestScore   = -std::numeric_limits<float>::infinity();
        getBestClass(output, i, layout.attrStride, layout.boxStride, bestClassId, bestScore);

        if (bestClassId < 0) continue;

        float threshold = 0.25f;
        if (detectionParams.perClassPreclusterThreshold.size() > static_cast<size_t>(bestClassId))
            threshold = detectionParams.perClassPreclusterThreshold[bestClassId];

        if (bestScore < threshold) continue;

        const float a0 = output[0 * layout.attrStride + i * layout.boxStride];
        const float a1 = output[1 * layout.attrStride + i * layout.boxStride];
        const float a2 = output[2 * layout.attrStride + i * layout.boxStride];
        const float a3 = output[3 * layout.attrStride + i * layout.boxStride];

        BBoxInfo b;
        if (!decodeBox(a0, a1, a2, a3, kBoxFormat, networkInfo, b.x1, b.y1, b.x2, b.y2))
            continue; // degenerate/invalid box, skip

        b.score   = bestScore;
        b.classId = bestClassId;
        proposals.push_back(b);
    }

    // Bound NMS cost: keep only the top-K proposals by score before the
    // O(n^2) pass, rather than running NMS on every surviving box.
    if (static_cast<int>(proposals.size()) > kMaxProposalsForNms)
    {
        std::partial_sort(proposals.begin(), proposals.begin() + kMaxProposalsForNms,
                           proposals.end(),
                           [](const BBoxInfo& x, const BBoxInfo& y) { return x.score > y.score; });
        proposals.resize(kMaxProposalsForNms);
    }

    std::vector<BBoxInfo> finalDetections = nmsAllClasses(proposals, kNmsIouThreshold);

    objectList.reserve(objectList.size() + finalDetections.size());
    for (const auto& b : finalDetections)
        addBBoxProposal(b, objectList);

    return true;
}

CHECK_CUSTOM_PARSE_FUNC_PROTOTYPE(NvDsInferParseYoloV11);

