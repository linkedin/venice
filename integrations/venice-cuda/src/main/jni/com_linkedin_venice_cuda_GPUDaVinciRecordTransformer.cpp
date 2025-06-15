#include <jni.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <iostream>
#include "com_linkedin_venice_cuda_GPUDaVinciRecordTransformer.h"

// Forward declarations for CUDA functions
extern "C" {
    struct GPUContext;
    struct GPUHashMap;

    GPUContext* gpu_initialize(int device_id);
    void gpu_shutdown(GPUContext* ctx);
    GPUHashMap* gpu_create_hashmap(GPUContext* ctx, uint32_t num_buckets,
                                   uint32_t max_entries, uint32_t data_pool_size);
    void gpu_destroy_hashmap(GPUHashMap* map);
    bool gpu_insert_batch(GPUContext* ctx, GPUHashMap* map,
                         const uint8_t* keys, const uint32_t* key_sizes,
                         const uint8_t* values, const uint32_t* value_sizes,
                         uint32_t num_inserts);
}

// Global context management
std::unordered_map<jlong, GPUContext*> g_contexts;
std::unordered_map<jlong, GPUHashMap*> g_hashmaps;
std::mutex g_mutex;

// Helper function to convert jbyteArray to vector
std::vector<uint8_t> jbyteArrayToVector(JNIEnv* env, jbyteArray arr) {
    jsize len = env->GetArrayLength(arr);
    std::vector<uint8_t> vec(len);
    env->GetByteArrayRegion(arr, 0, len, reinterpret_cast<jbyte*>(vec.data()));
    return vec;
}

// Helper function to convert vector to jbyteArray
jbyteArray vectorToJbyteArray(JNIEnv* env, const std::vector<uint8_t>& vec) {
    jbyteArray arr = env->NewByteArray(vec.size());
    if (arr != nullptr) {
        env->SetByteArrayRegion(arr, 0, vec.size(),
                               reinterpret_cast<const jbyte*>(vec.data()));
    }
    return arr;
}

/*
 * Class:     com_linkedin_davinci_client_gpu_GPUDaVinciRecordTransformer
 * Method:    nativeInitializeGPU
 * Signature: (I)J
 */
JNIEXPORT jlong JNICALL Java_com_linkedin_venice_cuda_GPUDaVinciRecordTransformer_nativeInitializeGPU
  (JNIEnv *env, jobject obj, jint deviceId) {
    GPUContext* ctx = gpu_initialize(deviceId);
    if (ctx == nullptr) {
        return 0;
    }

    jlong handle = reinterpret_cast<jlong>(ctx);
    {
        std::lock_guard<std::mutex> lock(g_mutex);
        g_contexts[handle] = ctx;
    }

    return handle;
}

/*
 * Class:     com_linkedin_davinci_client_gpu_GPUDaVinciRecordTransformer
 * Method:    nativeShutdownGPU
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_linkedin_venice_cuda_GPUDaVinciRecordTransformer_nativeShutdownGPU
  (JNIEnv *env, jobject obj, jlong gpuContext) {
    std::lock_guard<std::mutex> lock(g_mutex);

    auto it = g_contexts.find(gpuContext);
    if (it != g_contexts.end()) {
        gpu_shutdown(it->second);
        g_contexts.erase(it);
    }
}

/*
 * Class:     com_linkedin_davinci_client_gpu_GPUDaVinciRecordTransformer
 * Method:    nativeCreateHashMap
 * Signature: (JI)J
 */
JNIEXPORT jlong JNICALL Java_com_linkedin_venice_cuda_GPUDaVinciRecordTransformer_nativeCreateHashMap
  (JNIEnv *env, jobject obj, jlong gpuContext, jint initialCapacity) {
    std::lock_guard<std::mutex> lock(g_mutex);

    auto it = g_contexts.find(gpuContext);
    if (it == g_contexts.end()) {
        return 0;
    }

    // Calculate appropriate sizes based on initial capacity
    uint32_t num_buckets = initialCapacity;  // Could use prime number
    uint32_t max_entries = initialCapacity * 2;  // Allow for growth
    uint32_t avg_entry_size = 256;  // Assume average key+value size
    uint32_t data_pool_size = max_entries * avg_entry_size;

    GPUHashMap* map = gpu_create_hashmap(it->second, num_buckets, max_entries, data_pool_size);
    if (map == nullptr) {
        return 0;
    }

    jlong handle = reinterpret_cast<jlong>(map);
    g_hashmaps[handle] = map;

    return handle;
}

/*
 * Class:     com_linkedin_davinci_client_gpu_GPUDaVinciRecordTransformer
 * Method:    nativeDestroyHashMap
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_linkedin_venice_cuda_GPUDaVinciRecordTransformer_nativeDestroyHashMap
  (JNIEnv *env, jobject obj, jlong gpuHashMapPtr) {
    std::lock_guard<std::mutex> lock(g_mutex);

    auto it = g_hashmaps.find(gpuHashMapPtr);
    if (it != g_hashmaps.end()) {
        gpu_destroy_hashmap(it->second);
        g_hashmaps.erase(it);
    }
}

/*
 * Class:     com_linkedin_davinci_client_gpu_GPUDaVinciRecordTransformer
 * Method:    nativePut
 * Signature: (J[B[B)Z
 */
JNIEXPORT jboolean Java_com_linkedin_venice_cuda_GPUDaVinciRecordTransformer_nativePut
  (JNIEnv *env, jobject obj, jlong gpuHashMapPtr, jbyteArray key, jbyteArray value) {
    std::lock_guard<std::mutex> lock(g_mutex);

    auto map_it = g_hashmaps.find(gpuHashMapPtr);
    if (map_it == g_hashmaps.end()) {
        return JNI_FALSE;
    }

    // Find associated context
    GPUContext* ctx = nullptr;
    for (const auto& ctx_pair : g_contexts) {
        ctx = ctx_pair.second;
        break;  // Use first available context (improve this in production)
    }

    if (ctx == nullptr) {
        return JNI_FALSE;
    }

    // Convert Java arrays to vectors
    std::vector<uint8_t> key_vec = jbyteArrayToVector(env, key);
    std::vector<uint8_t> value_vec = jbyteArrayToVector(env, value);

    uint32_t key_size = key_vec.size();
    uint32_t value_size = value_vec.size();

    // Call batch insert with single item
    bool success = gpu_insert_batch(ctx, map_it->second,
                                   key_vec.data(), &key_size,
                                   value_vec.data(), &value_size,
                                   1);

    return success ? JNI_TRUE : JNI_FALSE;
}

/*
 * Class:     com_linkedin_davinci_client_gpu_GPUDaVinciRecordTransformer
 * Method:    nativeGet
 * Signature: (J[B)[B
 */
JNIEXPORT jbyteArray Java_com_linkedin_venice_cuda_GPUDaVinciRecordTransformer_nativeGet
  (JNIEnv *env, jobject obj, jlong gpuHashMapPtr, jbyteArray key) {
    // For MVP, we'll implement a simple CPU-based lookup
    // In production, you'd implement gpu_lookup_batch similar to gpu_insert_batch

    // This is a placeholder that returns null
    // Real implementation would search the GPU hashmap
    return nullptr;
}

/*
 * Class:     com_linkedin_davinci_client_gpu_GPUDaVinciRecordTransformer
 * Method:    nativeRemove
 * Signature: (J[B)Z
 */
JNIEXPORT jboolean Java_com_linkedin_venice_cuda_GPUDaVinciRecordTransformer_nativeRemove
  (JNIEnv *env, jobject obj, jlong gpuHashMapPtr, jbyteArray key) {
    // For MVP, removal is not implemented
    // In production, you'd mark entries as deleted
    return JNI_FALSE;
}

/*
 * Class:     com_linkedin_davinci_client_gpu_GPUDaVinciRecordTransformer
 * Method:    nativeClear
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_linkedin_venice_cuda_GPUDaVinciRecordTransformer_nativeClear
  (JNIEnv *env, jobject obj, jlong gpuHashMapPtr) {
    // For MVP, we destroy and recreate
    // In production, you'd reset the hashmap state
    std::lock_guard<std::mutex> lock(g_mutex);

    auto it = g_hashmaps.find(gpuHashMapPtr);
    if (it != g_hashmaps.end()) {
        // Just reset counters for now
        // Real implementation would clear all entries
    }
}