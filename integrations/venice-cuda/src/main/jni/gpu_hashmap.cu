#include <cuda_runtime.h>
#include <device_launch_parameters.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <atomic>

// GPU HashMap structures
struct GPUHashEntry {
    uint64_t key_hash;
    uint32_t key_size;
    uint32_t value_size;
    uint32_t key_offset;    // Offset in data pool
    uint32_t value_offset;  // Offset in data pool
    int32_t next;          // Index of next entry in chain (-1 if end)
    uint32_t flags;        // 0 = empty, 1 = occupied, 2 = deleted
};

struct GPUHashMap {
    GPUHashEntry* entries;
    uint8_t* data_pool;      // Pool for storing actual key/value data
    int32_t* bucket_heads;   // Head indices for each bucket
    uint32_t num_buckets;
    uint32_t max_entries;
    uint32_t data_pool_size;
    uint32_t* entry_count;   // Atomic counter for entries
    uint32_t* data_pool_used; // Atomic counter for data pool usage
    uint32_t* next_entry_idx; // Atomic counter for next available entry slot
};

struct GPUContext {
    int device_id;
    cudaStream_t stream;
    bool initialized;
};

// MurmurHash3 for GPU
__device__ uint64_t gpu_murmur3_64(const uint8_t* key, uint32_t len) {
    const uint64_t m = 0xc6a4a7935bd1e995ULL;
    const int r = 47;
    uint64_t h = 0x8445d61a4e774912ULL ^ (len * m);
    
    const uint64_t* data = (const uint64_t*)key;
    const uint64_t* end = data + (len / 8);
    
    while (data != end) {
        uint64_t k = *data++;
        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
    }
    
    const uint8_t* data2 = (const uint8_t*)data;
    switch (len & 7) {
        case 7: h ^= ((uint64_t)data2[6]) << 48;
        case 6: h ^= ((uint64_t)data2[5]) << 40;
        case 5: h ^= ((uint64_t)data2[4]) << 32;
        case 4: h ^= ((uint64_t)data2[3]) << 24;
        case 3: h ^= ((uint64_t)data2[2]) << 16;
        case 2: h ^= ((uint64_t)data2[1]) << 8;
        case 1: h ^= ((uint64_t)data2[0]);
                h *= m;
    }
    
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    
    return h;
}

// CUDA kernel for inserting into hashmap
__global__ void gpu_insert_kernel(GPUHashMap* map, const uint8_t* keys, const uint32_t* key_sizes,
                                  const uint8_t* values, const uint32_t* value_sizes,
                                  uint32_t* key_offsets, uint32_t* value_offsets,
                                  bool* results, uint32_t num_inserts) {
    uint32_t tid = blockIdx.x * blockDim.x + threadIdx.x;
    if (tid >= num_inserts) return;
    
    // Calculate offsets for this thread's key and value
    uint32_t key_offset = key_offsets[tid];
    uint32_t value_offset = value_offsets[tid];
    uint32_t key_size = key_sizes[tid];
    uint32_t value_size = value_sizes[tid];
    
    const uint8_t* key = keys + key_offset;
    const uint8_t* value = values + value_offset;
    
    // Hash the key
    uint64_t hash = gpu_murmur3_64(key, key_size);
    uint32_t bucket = hash % map->num_buckets;
    
    // Allocate space in data pool
    uint32_t data_offset = atomicAdd(map->data_pool_used, key_size + value_size);
    if (data_offset + key_size + value_size > map->data_pool_size) {
        results[tid] = false;
        return;
    }
    
    // Copy key and value to data pool
    memcpy(map->data_pool + data_offset, key, key_size);
    memcpy(map->data_pool + data_offset + key_size, value, value_size);
    
    // Allocate entry
    uint32_t entry_idx = atomicAdd(map->next_entry_idx, 1);
    if (entry_idx >= map->max_entries) {
        results[tid] = false;
        return;
    }
    
    // Fill entry
    GPUHashEntry* entry = &map->entries[entry_idx];
    entry->key_hash = hash;
    entry->key_size = key_size;
    entry->value_size = value_size;
    entry->key_offset = data_offset;
    entry->value_offset = data_offset + key_size;
    entry->flags = 1; // occupied
    
    // Insert into bucket chain
    int32_t old_head = atomicExch(&map->bucket_heads[bucket], entry_idx);
    entry->next = old_head;
    
    atomicAdd(map->entry_count, 1);
    results[tid] = true;
}

// CUDA kernel for lookup
__global__ void gpu_lookup_kernel(GPUHashMap* map, const uint8_t* keys, const uint32_t* key_sizes,
                                  uint32_t* key_offsets, uint8_t* values, uint32_t* value_sizes,
                                  uint32_t* value_offsets, bool* found, uint32_t num_lookups) {
    uint32_t tid = blockIdx.x * blockDim.x + threadIdx.x;
    if (tid >= num_lookups) return;
    
    uint32_t key_offset = key_offsets[tid];
    uint32_t key_size = key_sizes[tid];
    const uint8_t* key = keys + key_offset;
    
    // Hash the key
    uint64_t hash = gpu_murmur3_64(key, key_size);
    uint32_t bucket = hash % map->num_buckets;
    
    // Search in bucket chain
    int32_t current = map->bucket_heads[bucket];
    found[tid] = false;
    
    while (current != -1) {
        GPUHashEntry* entry = &map->entries[current];
        
        if (entry->flags == 1 && entry->key_hash == hash && entry->key_size == key_size) {
            // Compare actual keys
            bool match = true;
            for (uint32_t i = 0; i < key_size; i++) {
                if (map->data_pool[entry->key_offset + i] != key[i]) {
                    match = false;
                    break;
                }
            }
            
            if (match) {
                // Found! Copy value
                uint32_t value_offset = value_offsets[tid];
                memcpy(values + value_offset, map->data_pool + entry->value_offset, entry->value_size);
                value_sizes[tid] = entry->value_size;
                found[tid] = true;
                return;
            }
        }
        
        current = entry->next;
    }
}

// Host-side functions
extern "C" {

GPUContext* gpu_initialize(int device_id) {
    GPUContext* ctx = new GPUContext();
    ctx->device_id = device_id;
    ctx->initialized = false;
    
    cudaError_t err = cudaSetDevice(device_id);
    if (err != cudaSuccess) {
        delete ctx;
        return nullptr;
    }
    
    err = cudaStreamCreate(&ctx->stream);
    if (err != cudaSuccess) {
        delete ctx;
        return nullptr;
    }
    
    ctx->initialized = true;
    return ctx;
}

void gpu_shutdown(GPUContext* ctx) {
    if (ctx && ctx->initialized) {
        cudaStreamDestroy(ctx->stream);
        delete ctx;
    }
}

GPUHashMap* gpu_create_hashmap(GPUContext* ctx, uint32_t num_buckets, uint32_t max_entries, uint32_t data_pool_size) {
    if (!ctx || !ctx->initialized) return nullptr;
    
    cudaSetDevice(ctx->device_id);
    
    GPUHashMap* h_map = new GPUHashMap();
    h_map->num_buckets = num_buckets;
    h_map->max_entries = max_entries;
    h_map->data_pool_size = data_pool_size;
    
    // Allocate GPU memory
    cudaMalloc(&h_map->entries, sizeof(GPUHashEntry) * max_entries);
    cudaMalloc(&h_map->data_pool, data_pool_size);
    cudaMalloc(&h_map->bucket_heads, sizeof(int32_t) * num_buckets);
    cudaMalloc(&h_map->entry_count, sizeof(uint32_t));
    cudaMalloc(&h_map->data_pool_used, sizeof(uint32_t));
    cudaMalloc(&h_map->next_entry_idx, sizeof(uint32_t));
    
    // Initialize
    cudaMemset(h_map->entries, 0, sizeof(GPUHashEntry) * max_entries);
    cudaMemset(h_map->bucket_heads, -1, sizeof(int32_t) * num_buckets);
    cudaMemset(h_map->entry_count, 0, sizeof(uint32_t));
    cudaMemset(h_map->data_pool_used, 0, sizeof(uint32_t));
    cudaMemset(h_map->next_entry_idx, 0, sizeof(uint32_t));
    
    // Allocate device copy of hashmap struct
    GPUHashMap* d_map;
    cudaMalloc(&d_map, sizeof(GPUHashMap));
    cudaMemcpy(d_map, h_map, sizeof(GPUHashMap), cudaMemcpyHostToDevice);
    
    // Store device pointer in host struct for later use
    h_map->entries = (GPUHashEntry*)d_map;
    
    return h_map;
}

void gpu_destroy_hashmap(GPUHashMap* map) {
    if (!map) return;
    
    GPUHashMap* d_map = (GPUHashMap*)map->entries;
    GPUHashMap h_map;
    cudaMemcpy(&h_map, d_map, sizeof(GPUHashMap), cudaMemcpyDeviceToHost);
    
    cudaFree(h_map.entries);
    cudaFree(h_map.data_pool);
    cudaFree(h_map.bucket_heads);
    cudaFree(h_map.entry_count);
    cudaFree(h_map.data_pool_used);
    cudaFree(h_map.next_entry_idx);
    cudaFree(d_map);
    
    delete map;
}

bool gpu_insert_batch(GPUContext* ctx, GPUHashMap* map, 
                     const uint8_t* keys, const uint32_t* key_sizes,
                     const uint8_t* values, const uint32_t* value_sizes,
                     uint32_t num_inserts) {
    if (!ctx || !map || !keys || !values || num_inserts == 0) return false;
    
    cudaSetDevice(ctx->device_id);
    
    // Calculate offsets
    uint32_t* h_key_offsets = new uint32_t[num_inserts];
    uint32_t* h_value_offsets = new uint32_t[num_inserts];
    uint32_t key_total = 0, value_total = 0;
    
    for (uint32_t i = 0; i < num_inserts; i++) {
        h_key_offsets[i] = key_total;
        h_value_offsets[i] = value_total;
        key_total += key_sizes[i];
        value_total += value_sizes[i];
    }
    
    // Allocate device memory
    uint8_t *d_keys, *d_values;
    uint32_t *d_key_sizes, *d_value_sizes, *d_key_offsets, *d_value_offsets;
    bool *d_results;
    
    cudaMalloc(&d_keys, key_total);
    cudaMalloc(&d_values, value_total);
    cudaMalloc(&d_key_sizes, sizeof(uint32_t) * num_inserts);
    cudaMalloc(&d_value_sizes, sizeof(uint32_t) * num_inserts);
    cudaMalloc(&d_key_offsets, sizeof(uint32_t) * num_inserts);
    cudaMalloc(&d_value_offsets, sizeof(uint32_t) * num_inserts);
    cudaMalloc(&d_results, sizeof(bool) * num_inserts);
    
    // Copy data to device
    cudaMemcpy(d_keys, keys, key_total, cudaMemcpyHostToDevice);
    cudaMemcpy(d_values, values, value_total, cudaMemcpyHostToDevice);
    cudaMemcpy(d_key_sizes, key_sizes, sizeof(uint32_t) * num_inserts, cudaMemcpyHostToDevice);
    cudaMemcpy(d_value_sizes, value_sizes, sizeof(uint32_t) * num_inserts, cudaMemcpyHostToDevice);
    cudaMemcpy(d_key_offsets, h_key_offsets, sizeof(uint32_t) * num_inserts, cudaMemcpyHostToDevice);
    cudaMemcpy(d_value_offsets, h_value_offsets, sizeof(uint32_t) * num_inserts, cudaMemcpyHostToDevice);
    
    // Launch kernel
    uint32_t threads_per_block = 256;
    uint32_t blocks = (num_inserts + threads_per_block - 1) / threads_per_block;
    
    GPUHashMap* d_map = (GPUHashMap*)map->entries;
    gpu_insert_kernel<<<blocks, threads_per_block, 0, ctx->stream>>>(
        d_map, d_keys, d_key_sizes, d_values, d_value_sizes,
        d_key_offsets, d_value_offsets, d_results, num_inserts
    );
    
    // Wait for completion
    cudaStreamSynchronize(ctx->stream);
    
    // Check results
    bool* h_results = new bool[num_inserts];
    cudaMemcpy(h_results, d_results, sizeof(bool) * num_inserts, cudaMemcpyDeviceToHost);
    
    bool all_success = true;
    for (uint32_t i = 0; i < num_inserts; i++) {
        if (!h_results[i]) {
            all_success = false;
            break;
        }
    }
    
    // Cleanup
    delete[] h_key_offsets;
    delete[] h_value_offsets;
    delete[] h_results;
    
    cudaFree(d_keys);
    cudaFree(d_values);
    cudaFree(d_key_sizes);
    cudaFree(d_value_sizes);
    cudaFree(d_key_offsets);
    cudaFree(d_value_offsets);
    cudaFree(d_results);
    
    return all_success;
}

} // extern "C"
