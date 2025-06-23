# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - 2024-12-24

### 🚀 Major Features

- **Sequential Pipeline Architecture**: Complete redesign from parallel to sequential processing
  - Kafka Producer runs first (60 seconds)
  - Consumer processes all data after producer completes
  - Eliminates race conditions and improves reliability

- **Dual Consumer Strategy**: Robust fallback mechanism
  - **Primary**: Spark consumer for optimized Parquet output
  - **Fallback**: Simple Kafka consumer for JSON/CSV output
  - Automatic fallback if Spark encounters issues

- **Multi-Format Output**: Enhanced data accessibility
  - JSON format for raw data inspection
  - CSV format for spreadsheet applications
  - Parquet format for analytics workflows

### 🔧 Technical Improvements

- **Simplified Spark Configuration**: Resolved networking issues
  - Switched from cluster mode (`spark://localhost:7077`) to local mode (`local[2]`)
  - Added proper driver binding configuration
  - Removed complex WSL IP dependencies

- **Enhanced Error Handling**:
  - Comprehensive exception handling with detailed logging
  - Process timeout management (2-minute consumer timeout)
  - Graceful degradation when components fail

- **Improved Logging**:
  - Color-coded success/failure indicators (✅/❌)
  - Step-by-step progress tracking
  - Detailed error messages with troubleshooting hints

### 🛠️ Configuration Updates

- **Docker Compose**: Updated Spark to version 3.4.1
  - Fixed version compatibility issues between PySpark and Docker Spark
  - Updated Kafka packages to match Spark version
  - Added proper networking configuration

- **Dependencies**: Streamlined package requirements
  - Added pandas for CSV export functionality
  - Kept core dependencies minimal for easier installation

### 📁 Output Structure

```
output/
├── users_data.json      # Raw JSON data from Kafka
├── users_data.csv       # Structured CSV for analysis
└── users_spark_data.parquet  # Optimized Parquet (if Spark works)
```

### 🐛 Bug Fixes

- Fixed PySpark version compatibility (3.4.1 throughout stack)
- Resolved Spark driver binding issues in WSL/Windows environments
- Fixed indentation and syntax errors in consumer logic
- Corrected schema field types (nullable = True for flexibility)

### 📖 Documentation

- **Comprehensive README**: Complete rewrite with:
  - Clear architecture diagrams
  - Step-by-step setup instructions
  - Troubleshooting guide
  - Performance tuning recommendations

- **API Documentation**: Detailed data schema specification
- **Monitoring Guide**: Instructions for using Kafka Control Center and Spark UI

## [1.0.0] - 2024-12-23

### Initial Release

- Basic Kafka producer for streaming user data
- Spark streaming consumer for real-time processing
- Docker Compose setup with Kafka, Zookeeper, and Spark
- S3 integration for data lake storage
- Parallel processing architecture

### Features

- Real-time data streaming from RandomUser API
- Apache Kafka message broker
- Apache Spark distributed processing
- AWS S3 data lake integration
- Confluent Control Center monitoring

### Known Issues

- Version compatibility problems between PySpark and Docker Spark
- Network binding issues in WSL environments
- Race conditions in parallel processing
- Limited error handling and recovery

---

## Migration Guide: v1.0 → v2.0

### Breaking Changes

1. **Execution Model**: Changed from parallel to sequential execution
   ```bash
   # Old way (v1.0)
   # Producer and consumer ran simultaneously
   
   # New way (v2.0)
   # Producer completes first, then consumer processes all data
   python realtime_pipeline.py
   ```

2. **Output Location**: Data now saved locally by default
   ```bash
   # Old: S3 only (required AWS credentials)
   # New: Local files with optional S3 (if credentials provided)
   ```

3. **Spark Configuration**: Simplified networking
   ```python
   # Old: .master('spark://localhost:7077')
   # New: .master('local[2]')
   ```

### Upgrade Steps

1. **Update Docker Services**:
   ```bash
   docker-compose down
   docker-compose pull
   docker-compose up -d
   ```

2. **Install New Dependencies**:
   ```bash
   pip install pandas  # New requirement for CSV export
   ```

3. **Remove Environment Variables** (optional):
   ```bash
   # AWS credentials now optional for local development
   # unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
   ```

4. **Update Usage Pattern**:
   ```bash
   # Single command now handles entire pipeline
   python realtime_pipeline.py
   
   # Check output directory
   ls ./output/
   ```

### Compatibility

- **Python**: 3.8+ (unchanged)
- **Docker**: 20.10+ (unchanged)
- **Spark**: 3.4.1 (updated from 3.3.0)
- **Kafka**: Compatible with existing topics and data 