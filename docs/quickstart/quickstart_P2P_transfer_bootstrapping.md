---
layout: default
title: Venice P2P Transfer Bootstrapping Quickstart
parent: Quickstart
permalink: /docs/quickstart/quickstart_P2P_transfer_bootstrapping
---


# Venice P2P Transfer Bootstrapping Architecture

**High-Performance Peer-to-Peer Data Bootstrap**

*Accelerating Venice Node Deployment Through Direct Peer-to-Peer RocksDB Snapshot Transfer*

---

## 🚨 Problem & Solution

### The Problem

**Bootstrap Bottlenecks**
- **Kafka-Only Recovery:** All nodes bootstrap exclusively from Kafka brokers
- **Resource Intensive:** Time-consuming process during cluster recovery, inefficient for consuming messages from the PubSub system under high-update workloads
- **Scalability Limits:** Broker capacity becomes recovery bottleneck

**Real-World Impact**
- Extended MTTR (Mean Time to Restore or Repair) during outages
- Cascading broker overload
- Slower cluster expansion
- Increased operational overhead

### 💡 The Solution

**Direct P2P Transfer**
- **Peer-to-Peer:** Direct node-to-node data transfer
- **RocksDB Snapshots:** Consistent point-in-time data copies
- **Intelligent Fallback:** Automatic Kafka Ingestion recovery on failure
- **Low Risk:** Low Deployment Risk in DaVinci client

**Key Benefits**
- Faster node recovery and scaling
- Reduced Kafka broker load


---

## 🏗️ System Architecture Flow

### Venice Blob Transfer Complete Flow

```
                           Venice Blob Transfer Complete Flow

Step 1: Peer Discovery
┌─────────────┐  Discovery Request   ┌─────────────┐  Query Helix/ZK     ┌─────────────┐
│ Client Node │ ────────────────────>│ Discovery   │ ──────────────────> │ Metadata    │
│ (Needs Data)│                      │ Service     │                     │ Repository  │
└─────────────┘                      └─────────────┘                     └─────────────┘
       ^                                     │                                   │
       │                                     │ Return Host List                  │
       │                              ┌─────────────┐                            │
       └──────────────────────────────│ Host List:  │ <──────────────────────────┘
                                      │ [host1,     │
                                      │  host2,     │
                                      │  host3...]  │
                                      └─────────────┘

Step 2: Sequential Host Attempts
┌─────────────┐  T1:Try Host 1  ┌─────────────┐ 
│ Client Node │ ──────────────> │ Server A    │ 
│             │ <── FAIL ────── │ (No Data/   │                 
└─────────────┘                 │  Busy)      │                
                                └─────────────┘                 
                 T2:Try Host 2  ┌─────────────┐  
                ──────────────> │ Server B    │ 
                <── FAIL ────── │ Table Format│
                                │  not Match  │              
                                └─────────────┘       

                 T3:Try Host 3  ┌─────────────┐  
                ──────────────> │ Server C    │ 
                                │ (Not busy,  │               
                                │Format Match)│              
                                └─────────────┘     
                                         
Step 3: Start Transfer                   
┌────────────────┐                       
│ Client Node    │ 
│                │
│  Receives:     │ <── 1.1 File: file1.sst ────────── ┌─────────────┐
│  1. Files      │ <── 1.2 File: file2.sst ────────── │ Server C    │
│                │ <── 1.3 File: MANIFEST_00 ──────── │             │
│  2. Metadata   │ <── 2. Metadata ────────────────── └─────────────┘
│  3. COMPLETE   │ <── 3. COMPLETE Flag ───────────── 
└────────────────┘

Step 4: Client Processing after recevie COMPLETE flag

┌─────────────┐  Validate Files     ┌─────────────┐    Atomic Rename     ┌─────────────┐
│ Client Node │ ──────────────────> │ Temp Folder │   ─────────────────> │ Partition   │
│             │  (MD5)              │/store/temp_ │    (temp→partition)  │ Folder      │
└─────────────┘                     | p1/         |                      │ /store/p1/  │
                                    └─────────────┘                      └─────────────┘

Step 5: Kafka Ingestion Fallback (Always Happens)

┌─────────────┐  Resume/Fill Gap    ┌─────────────┐
│ Client Node │ ──────────────────> │ Kafka       │
│             │  From snapshot      │ Ingestion   │
│             │  offset to latest   │             │
└─────────────┘                     └─────────────┘

```

**Step 1: Peer Discovery**
- **Venice Server:** Query Helix CustomizedViewRepository for COMPLETED nodes
- **DaVinci Application:** Query DaVinci push job report for ready-to-serve nodes

**Step 2: P2P Transfer in Client Side**
```
Connectivity Check ────> Peer Selection ────> Sequential Request
      ↓                         ↓                      ↓
Parallel connection       Filter & shuffle      Try peers until
check with caching      connectable hosts      success or exhaust
```

**Step 3: Data Transfer**
```
Snapshot Creation ────> File Streaming ────>  Metadata Sync
       ↓                       ↓                  ↓
Server creates         Chunked transfer      Offset + Version State
RocksDB snapshot      with MD5 validation     after files complete
```
**Step 4: Completion**
- **Success Path:** After validating all files, atomically rename the temp directory to the partition directory, then initiate Kafka ingestion to synchronize any remaining offset gap.
- **Fallback Path:** If any error occurs, clean up the temp directory and retry with the next peer; if all peers fail, back to Kafka bootstrap from the beginning.

### Key Components

- **DefaultIngestionBackend** - Bootstrap orchestration entry point
- **NettyP2PBlobTransferManager** - P2P transfer coordinator
- **BlobSnapshotManager** - Server-side snapshot lifecycle
- **NettyFileTransferClient** - High-performance client


## 📥 Client-Side Process

### Process Flow

```
1. Discover Peers → 2. Check Connectivity → 3. Request Data → 4. Fallback to Kafka
```

### 🔍 Step 1: Peer Discovery

**Venice Server:**
- Query Helix CustomizedViewRepository
- Find COMPLETED nodes
- Filter by store/version/partition

**DaVinci Application:**
- Query DaVinci push job report
- Find ready-to-serve nodes
- Extract available peer list

### 🔗 Step 2: Connectivity Checking
Smart Caching Strategy due to large peer sets:

```
┌─────────────┐   ┌─────────────┐   ┌─────────────────────┐   ┌─────────────┐
│ Purge Stale │ → │ Filter Bad  │ → │ Check Hosts in      │ → │ Update Cache│
│ Records     │   │ Hosts       │   │Parallel Connectivity│   │ Results     │
└─────────────┘   └─────────────┘   └─────────────────────┘   └─────────────┘                          
```

- **Parallel Testing:** Multiple host connections simultaneously
- **Smart Caching:** Remember good/bad hosts with timestamps
- **Timeout Management:** 1-minute connectivity check limit

### 📦 Step 3: Sequential Data Request

```
For Each Peer (Shuffled List):
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│   Send Request  │ → │  Receive Data   │ → │  Receive        │ → │  Receive        │ → │   Validate      │ → │  Rename Temp    │
│    HTTP GET     │   │  File Chunks    │   │  Metadata       │   │ COMPLETE_FLAG   │   │      MD5        │   │ to Partition Dir│
└─────────────────┘   └─────────────────┘   └─────────────────┘   └─────────────────┘   └─────────────────┘   └─────────────────┘
```


### 🚨 Error Handling Details in different Scenarios

**Connection Failures (VenicePeersConnectionException)**
- Network timeout
- SSL handshake failure
- Host unreachable
- **Action:** Try next peer

**File Not Found (VeniceBlobTransferFileNotFoundException)**
- Snapshot missing
- Stale snapshot
- Server too busy, reject with 429 errors
- **Action:** Try next peer

**Transfer Errors (Data Integrity Issues)**
- Checksum mismatch
- File size mismatch
- Network interruption: Server initiates deployment or shuts down unexpectedly
- **Action:** Cleanup + next peer

### 🧹 Comprehensive Cleanup Process

```
┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│ Force Flush │ → │ Close File  │ → │ Delete      │ → │ Reset       │
│ File Channel│   │ Handles     │   │ Partial Dir │   │ Handler     │
└─────────────┘   └─────────────┘   └─────────────┘   └─────────────┘
```

- **Zero Partial State:** Complete cleanup ensures no corruption
- **Handler Reset:** Ready for next peer attempt

### 🔄 Step 4: Kafka Fallback Strategy

**Two-Phase Strategy:**
- **Phase 1 - Blob Transfer:** Rapid bulk data transfer via P2P
- **Phase 2 - Kafka Fill-Gap:** Even if blob transfer fails, deployment continues with Kafka ingestion

**Zero-Risk Design:** After a successful blob transfer, Kafka ingestion always follows to synchronize any data between the snapshot offset and the latest offset, guaranteeing full deployment.

---

## 📤 Server-Side Process

### Process Flow

```
1. Request Reception & Validation → 2. Prepare Snapshot & Metadata → 3. File Transfer & Chunking → 4. Metadata Transfer & Completion
```

### ☑️ Step 1: Request Reception & Validation

Incoming Request Pipeline:
```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ HTTP GET    │    │ Parse URI   │    │ Validate    │    │ Check       │
│ Request     │ →  │ Extract     │ →  │ Table       │ →  │ Concurrency │
│             │    │ Parameters  │    │ Format      │    │ Limits      │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

- **URI Parsing:** /storeName/version/partition/tableFormat
- **Format Check:** PLAIN_TABLE vs BLOCK_BASED_TABLE match verification
- **Concurrency Control:** Global limit enforcement; reject with 429 if over limit

### 📊 Step 2: Prepare Snapshot & Metadata

**Snapshot Lifecycle:**
- Check staleness (Configable TTL)
- Verify concurrent user limits
- Create a new snapshot if the existing one is stale or does not exist
- Prepare partition metadata

**Metadata Preparation:**
- Serialization of StoreVersionState (enables synchronization of hybrid store configuration parameters)
- OffsetRecord encapsulation (captures the snapshot’s offset for accurate state synchronization)
- JSON metadata response

### 📦 Step 3: File Transfer & Chunking Strategy (Server/Client Single File Transfer Process Details)

**Server-Side Adaptive Chunking Algorithm:**



```
Step 1: File Preparation
┌─────────────────────────┐   ┌─────────────────────────┐   ┌─────────────────────────┐   ┌─────────────────────────┐   ┌─────────────────────────┐
│   Open File for         │   │   Calculate File        │   │   Generate MD5          │   │   Prepare HTTP          │   │   Send Response         │
│     Reading             │   │      Length             │   │     Checksum            │   │    Response             │   │      Headers            │
│                         │ → │                         │ → │                         │ → │                         │ → │                         │
│ Open file in            │   │ Get file size for       │   │ Generate checksum       │   │ Set content headers     │   │ Send headers to         │
│ read-only mode          │   │ response headers        │   │ for validation          │   │ and file metadata       │   │ client first            │
└─────────────────────────┘   └─────────────────────────┘   └─────────────────────────┘   └─────────────────────────┘   └─────────────────────────┘

Step 2: Adaptive Chunking Strategy
┌─────────────────────────┐   ┌─────────────────────────┐   ┌─────────────────────────┐   
│   Calculate Optimal     │   │   Create Chunked        │   │   Wrap for HTTP         │   
│     Chunk Size          │   │    File Handler         │   │     Streaming           │  
│ 16KB - 2MB              │ → │                         │ → │                         │ 
│ Determine best chunk    │   │ Set up memory-efficient │   │ Prepare for HTTP        │ 
│ size based on file      │   │ streaming mechanism     │   │ chunked transfer        │ 
└─────────────────────────┘   └─────────────────────────┘   └─────────────────────────┘   

Step 3: Efficient File Streaming
┌─────────────────────────┐   ┌─────────────────────────┐   ┌─────────────────────────┐   ┌─────────────────────────┐
│   Stream File Data      │   │   Netty Chunked         │   │   Monitor Transfer      │   │   Complete Transfer     │
│     in Chunks           │   │    Write Handler        │   │      Progress           │   │                         │
│                         │ → │                         │ → │                         │ → │                         │
│ ctx.writeAndFlush()     │   │ Memory-efficient        │   │ Log success/failure     │   │ Ready for next file     │
│ non-blocking write      │   │ file streaming          │   │ per file                │   │ or metadata             │
└─────────────────────────┘   └─────────────────────────┘   └─────────────────────────┘   └─────────────────────────┘

```

**Client-Side High-Performance Reception:**




```
Step 1: File Setup
┌─────────────────────────┐   ┌─────────────────────────┐   ┌─────────────────────────┐
│   Parse Headers         │   │   Create Temp Dir       │   │   Create Empty File     │
│                         │   │                         │   │                         │
│ • Extract filename      │ → │ • Make /store/temp_p0/  │ → │ • Create empty file     │
│ • Extract file size     │   │ • Delete if exists      │   │ • Open FileChannel      │
│ • Extract MD5 hash      │   │                         │   │   in WRITE mode         │
└─────────────────────────┘   └─────────────────────────┘   └─────────────────────────┘

Step 2: Write Data Chunk for each chunk arrived repeatedly
┌─────────────────────────┐   ┌─────────────────────────┐   ┌─────────────────────────┐
│   Receive Network       │   │   Convert to Channel    │   │   Write to File         │
│      Data Chunk         │   │                         │   │                         │
│                         │ → │ • ByteBuf → Stream      │ → │ • transferFrom() call   │
│ • HttpContent arrives   │   │ • Stream → Channel      │   │                         │
│ • Extract ByteBuf       │   │                         │   │ • Update file position  │
└─────────────────────────┘   └─────────────────────────┘   └─────────────────────────┘


Step 3: Complete File
┌─────────────────────────┐   ┌─────────────────────────┐   ┌─────────────────────────┐
│   Last Chunk Received   │   │   Flush and Close       │   │   Async Validation      │
│                         │   │                         │   │                         │
│ • Detect end of file    │ → │ • Force data to disk    │ → │ • Submit MD5 check      │
│ • Validate file size    │   │ • Close FileChannel     │   │ • Reset handler state   │
│                         │   │                         │   │ • Ready for next file   │
└─────────────────────────┘   └─────────────────────────┘   └─────────────────────────┘

```
```
Data Flow Transformation at Convert to Channel

Network → ByteBuf → ByteBufInputStream → ReadableByteChannel → FileChannel
   │         │              │                    │               │
   │         │              │                    │               └─ Disk File
   │         │              │                    └─ NIO Channel (efficient)
   │         │              └─ Java Stream (bridge)
   │         └─ Netty Buffer
   └─ Raw network packets

```

### 📊 Step 4: Metadata Transfer & Completion Protocol

**Critical Ordering for Data Consistency:**
```
┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│ 1. Prepare  │   │ 2. Transfer │   │ 3. Send     │   │ 4. Send     │
│ RocksDB     │ → │ All Files   │ → │ Metadata    │→  │ COMPLETE    │
│ Snapshot    │   │ (with MD5)  │   │ (Offset +   │   │ Signal      │
│             │   │             │   │ SVS)        │   │             │
└─────────────┘   └─────────────┘   └─────────────┘   └─────────────┘
```

**Why This Ordering is Critical:**

❌ **Wrong Order (Metadata → Files → Complete):**
- Client updates offset records immediately
- If file transfer fails, offset state is corrupted
- Need offset/SVS rollback mechanisms
- Increased error handling complexity and risk

✅ **Correct Order (Files → Metadata → Complete):**
- Files transferred and validated first
- Metadata only sent after successful file transfer
- Atomic state update on COMPLETE signal
- Less risk consistency guarantee

**Metadata Consistency Protocol:**
- **Metadata:** OffsetRecord + StoreVersionState captured before snapshot creation time
- **JSON Serialization:** Structured metadata transfer with size validation

---
## 🌐 Traffic Control

### 🚥 Global Traffic Shaping

**Shared Rate Limiting:** Single GlobalChannelTrafficShapingHandler instance controls bandwidth across ALL blob transfer channels globally

### Traffic Management Strategy

```
Global Control Architecture:
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│   Single    │   │  Monitor    │   │  Enforce    │
│ Controller  │ → │ All Channels│ → │ Bandwidth   │
│  Instance   │   │ Globally    │   │  Limits     │
└─────────────┘   └─────────────┘   └─────────────┘
```

- **Write Limit:** Global bytes/sec across all channels
- **Read Limit:** Global bytes/sec across all channels
- **Check Interval:** 1 second monitoring cycle

### Adaptive Throttling System

**Dynamic Rate Adjustment:**
- Increment/Decrement: 20%
- Range: 20% - 200% of base rate
- Separate read/write throttlers
- Idle threshold handling

---

## 🎛️ Configuration & Operations

### Configurations

📥 **Receiver/Client Feature Enablement Control**

*Venice Server:*
- Store Level: `blobTransferInServerEnabled`
- Application Level: `blob.transfer.receiver.server.policy`

*DaVinci Application:*
- Store Level: `blobTransferEnabled`

📤 **Sender/Server Feature Enablement Control**

*All Applications:*
- Application Level: `blob.transfer.manager.enabled`

### Performance Tuning Parameters

🪜 **Thresholds**
- **Offset Lag:** Skip blob transfer if not lagged enough
- **Snapshot TTL:** Maintain snapshot freshness
- **Snapshot Cleanup Interval:** Maintain disk storage

🏎️ **Bandwidth Limits**
- **Client Read:** Client side read bytes per sec
- **Service Write:** Server write bytes per sec
- **Adaptive Range:** 20% - 200% of base rate
- **Max Concurrent Snapshot User:** Control server concurrent serve requests load

⏰ **Timeouts**
- **Transfer Max:** Server side max timeout for transferring files
- **Receive Max:** Client side max timeout for receiving files

---

## 📋 Summary

### Venice Blob Transfer: Key Features & Benefits

**🚀 Performance Excellence**
- Intelligent peer discovery and selection
- Fast failover with comprehensive error handling
- High-performance Netty streaming architecture
- Efficient file operations and adaptive chunking

**🔒 Rock-Solid Reliability**
- Consistent RocksDB snapshots with point-in-time guarantees
- Comprehensive error handling and automatic cleanup
- Data integrity validation with MD5 and size checks
- Automatic Kafka fallback for 100% data coverage

**🛡️ Security & Control**
- End-to-end SSL/TLS encryption
- Certificate-based ACL validation
- Global traffic rate limiting and adaptive throttling
- Comprehensive timeout and connection management

**🔧 Operational Excellence**
- Flexible multi-level configuration
- Automated snapshot lifecycle management
- Graceful degradation and low risk deployment

**Result: Faster node recovery, reduced infrastructure load, improved cluster scalability with enterprise-grade reliability**