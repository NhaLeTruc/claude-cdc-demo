# CDC Demo Architecture

## System Overview

This project demonstrates Change Data Capture (CDC) implementations for multiple storage systems, showcasing real-time data replication and change tracking.

## High-Level Architecture

```mermaid
graph TB
    subgraph "Source Databases"
        PG[PostgreSQL<br/>Logical Replication]
        MY[MySQL<br/>Binlog]
    end

    subgraph "CDC Layer"
        DBZ[Debezium<br/>Connect]
        KAFKA[Apache Kafka]
    end

    subgraph "Processing"
        PARSER[Event Parser]
        VALIDATOR[Data Validator]
    end

    subgraph "Destination"
        DELTA[DeltaLake<br/>with CDF]
        ICE[Apache Iceberg<br/>Snapshots]
    end

    subgraph "Observability"
        PROM[Prometheus]
        GRAF[Grafana]
        LOKI[Loki]
    end

    PG -->|WAL| DBZ
    MY -->|Binlog| DBZ
    DBZ -->|CDC Events| KAFKA
    KAFKA -->|Consume| PARSER
    PARSER -->|Standardized| VALIDATOR
    VALIDATOR -->|Valid Events| DELTA
    VALIDATOR -->|Valid Events| ICE

    PARSER -.->|Metrics| PROM
    VALIDATOR -.->|Metrics| PROM
    DELTA -.->|Metrics| PROM
    ICE -.->|Metrics| PROM

    PROM -->|Query| GRAF
    PARSER -.->|Logs| LOKI
    LOKI -->|Query| GRAF
```

## Postgres CDC Pipeline (User Story 1)

### Component Diagram

```mermaid
graph LR
    subgraph "PostgreSQL"
        PG_DB[(Database)]
        WAL[Write-Ahead Log]
        SLOT[Replication Slot]
    end

    subgraph "Debezium"
        CONN[Postgres Connector]
        TRANS[Transformations]
    end

    subgraph "Kafka"
        TOPIC[cdc.postgres.*]
    end

    subgraph "CDC Pipeline"
        CONSUMER[Kafka Consumer]
        PARSER[Event Parser]
        WRITER[DeltaLake Writer]
    end

    subgraph "DeltaLake"
        TABLE[Delta Table]
        CDF[Change Data Feed]
        VERSIONS[Version History]
    end

    PG_DB -->|Changes| WAL
    WAL -->|Stream| SLOT
    SLOT -->|Subscribe| CONN
    CONN -->|Transform| TRANS
    TRANS -->|Publish| TOPIC
    TOPIC -->|Poll| CONSUMER
    CONSUMER -->|Raw Events| PARSER
    PARSER -->|Standardized| WRITER
    WRITER -->|Write| TABLE
    TABLE -->|Enable| CDF
    TABLE -->|Track| VERSIONS
```

### Data Flow

```mermaid
sequenceDiagram
    participant App as Application
    participant PG as PostgreSQL
    participant Deb as Debezium
    participant Kafka as Kafka
    participant Parser as Event Parser
    participant Writer as Delta Writer
    participant Delta as DeltaLake

    App->>PG: INSERT customer
    PG->>PG: Write to WAL
    PG-->>Deb: Stream WAL changes
    Deb->>Deb: Parse WAL entry
    Deb->>Kafka: Publish CDC event
    Parser->>Kafka: Poll for events
    Kafka-->>Parser: CDC event batch
    Parser->>Parser: Parse & validate
    Parser->>Writer: Standardized events
    Writer->>Delta: Write with CDF
    Delta->>Delta: Create new version
    Note over Delta: Version N+1 available
```

## MySQL CDC Pipeline (User Story 2)

```mermaid
graph LR
    subgraph "MySQL"
        MY_DB[(Database)]
        BINLOG[Binary Log]
    end

    subgraph "Debezium"
        MY_CONN[MySQL Connector]
    end

    subgraph "Processing"
        MY_PARSER[Binlog Parser]
        MY_WRITER[Delta Writer]
    end

    MY_DB -->|Changes| BINLOG
    BINLOG -->|Read| MY_CONN
    MY_CONN -->|Events| MY_PARSER
    MY_PARSER -->|Parsed| MY_WRITER
    MY_WRITER -->|Write| DL[(DeltaLake)]
```

## DeltaLake CDF Pipeline (User Story 3)

### Component Architecture

```mermaid
graph TB
    subgraph "DeltaLake Table Storage"
        V0[Version 0<br/>Initial State]
        V1[Version 1<br/>+100 rows]
        V2[Version 2<br/>Updated rows]
        V3[Version 3<br/>-50 rows]

        PARQUET[Parquet Files<br/>Data Layer]
        TXLOG[Transaction Log<br/>_delta_log/]
        CDF_FILES[CDF Files<br/>_change_data/]
    end

    subgraph "CDC Pipeline Components"
        TM[Table Manager<br/>table_manager.py]
        VT[Version Tracker<br/>version_tracker.py]
        CR[CDF Reader<br/>cdf_reader.py]
        PO[Pipeline Orchestrator<br/>pipeline.py]
    end

    subgraph "Change Processing"
        FILTER[Change Filter<br/>by type/version]
        STATS[Statistics<br/>Aggregator]
        PROCESSOR[Custom Processor<br/>User Logic]
    end

    subgraph "Outputs"
        METRICS[Prometheus<br/>Metrics]
        CHANGES[Change Events<br/>DataFrame]
        REPORTS[Change Reports<br/>Summaries]
    end

    V0 -.->|write| V1
    V1 -.->|write| V2
    V2 -.->|write| V3

    V1 -->|record changes| CDF_FILES
    V2 -->|record changes| CDF_FILES
    V3 -->|record changes| CDF_FILES

    PARQUET -->|read| TM
    TXLOG -->|track| VT
    CDF_FILES -->|query| CR

    TM -->|coordinates| PO
    VT -->|provides versions| PO
    CR -->|reads changes| PO

    PO -->|filter| FILTER
    PO -->|calculate| STATS
    PO -->|execute| PROCESSOR

    FILTER -->|output| CHANGES
    STATS -->|generate| REPORTS
    PROCESSOR -->|export| METRICS

    style CDF_FILES fill:#f9f,stroke:#333,stroke-width:2px
    style PO fill:#bbf,stroke:#333,stroke-width:2px
    style CHANGES fill:#bfb,stroke:#333,stroke-width:2px
```

### Data Flow: Version-Based CDC

```mermaid
sequenceDiagram
    participant App as Application
    participant DT as Delta Table
    participant TM as Table Manager
    participant VT as Version Tracker
    participant CR as CDF Reader
    participant Pipeline as CDC Pipeline
    participant Consumer as Consumer

    App->>DT: Write data (v1)
    DT->>DT: Create version 1
    DT->>DT: Record changes to CDF
    Note over DT: _change_type: insert<br/>_commit_version: 1

    App->>DT: Update data (v2)
    DT->>DT: Create version 2
    DT->>DT: Record preimage + postimage
    Note over DT: _change_type: update_preimage<br/>_change_type: update_postimage

    Consumer->>Pipeline: Request changes since v0
    Pipeline->>VT: Get version range
    VT-->>Pipeline: Versions 0-2
    Pipeline->>CR: Read CDF(v0→v2)
    CR->>DT: Query CDF files
    DT-->>CR: Change records
    CR-->>Pipeline: Changes DataFrame
    Pipeline->>Pipeline: Filter & process
    Pipeline-->>Consumer: Change events
    Pipeline->>TM: Update metrics
    TM->>TM: Export to Prometheus
```

### Change Type Flow

```mermaid
graph LR
    subgraph "Operations"
        INSERT[INSERT<br/>Customer]
        UPDATE[UPDATE<br/>Customer Tier]
        DELETE[DELETE<br/>Customer]
    end

    subgraph "Delta Table Versions"
        V1[Version 1]
        V2[Version 2]
        V3[Version 3]
    end

    subgraph "CDF Change Records"
        C1[_change_type: insert<br/>after: {id:1, tier:Gold}]
        C2A[_change_type: update_preimage<br/>before: {id:1, tier:Gold}]
        C2B[_change_type: update_postimage<br/>after: {id:1, tier:Platinum}]
        C3[_change_type: delete<br/>before: {id:1, tier:Platinum}]
    end

    INSERT -->|creates| V1
    UPDATE -->|creates| V2
    DELETE -->|creates| V3

    V1 -->|records| C1
    V2 -->|records| C2A
    V2 -->|records| C2B
    V3 -->|records| C3

    style C1 fill:#bfb,stroke:#333
    style C2A fill:#fbb,stroke:#333
    style C2B fill:#bfb,stroke:#333
    style C3 fill:#fbb,stroke:#333
```

## Iceberg CDC Pipeline (User Story 4)

### Component Architecture

```mermaid
graph TB
    subgraph "Iceberg Table Storage"
        S0[Snapshot 0<br/>Initial State]
        S1[Snapshot 1<br/>+1000 rows]
        S2[Snapshot 2<br/>+500 rows]
        S3[Snapshot 3<br/>Compaction]

        PARQUET[Parquet Data Files<br/>Immutable]
        METADATA[Metadata JSON<br/>version-hint.text]
        MANIFEST[Manifest Lists<br/>Snapshot Index]
    end

    subgraph "CDC Pipeline Components"
        TM[Table Manager<br/>table_manager.py]
        ST[Snapshot Tracker<br/>snapshot_tracker.py]
        IR[Incremental Reader<br/>incremental_reader.py]
        PO[Pipeline Orchestrator<br/>pipeline.py]
    end

    subgraph "Incremental Processing"
        SNAP_COMP[Snapshot<br/>Comparator]
        FILE_SCAN[Manifest<br/>Scanner]
        INCR_READ[Incremental<br/>Data Reader]
    end

    subgraph "Outputs"
        METRICS[Prometheus<br/>Metrics]
        ARROW_DATA[PyArrow Table<br/>Incremental Data]
        STATS[Processing<br/>Statistics]
    end

    S0 -.->|write| S1
    S1 -.->|write| S2
    S2 -.->|write| S3

    S1 -->|points to| MANIFEST
    S2 -->|points to| MANIFEST
    S3 -->|points to| MANIFEST
    MANIFEST -->|references| PARQUET
    METADATA -->|tracks| S3

    PARQUET -->|read| TM
    METADATA -->|load| TM
    MANIFEST -->|track| ST
    ST -->|analyze| IR

    TM -->|coordinates| PO
    ST -->|provides snapshots| PO
    IR -->|reads data| PO

    PO -->|compare| SNAP_COMP
    PO -->|scan| FILE_SCAN
    PO -->|execute| INCR_READ

    SNAP_COMP -->|output| ARROW_DATA
    FILE_SCAN -->|generate| STATS
    INCR_READ -->|export| METRICS

    style MANIFEST fill:#f9f,stroke:#333,stroke-width:2px
    style PO fill:#bbf,stroke:#333,stroke-width:2px
    style ARROW_DATA fill:#bfb,stroke:#333,stroke-width:2px
```

### Data Flow: Snapshot-Based Incremental Read

```mermaid
sequenceDiagram
    participant App as Application
    participant IT as Iceberg Table
    participant TM as Table Manager
    participant ST as Snapshot Tracker
    participant IR as Incremental Reader
    participant Pipeline as CDC Pipeline
    participant Consumer as Consumer

    App->>IT: Write data (append)
    IT->>IT: Create Snapshot 1
    IT->>IT: Update metadata
    Note over IT: Snapshot 1: +1000 rows<br/>New manifest list<br/>New data files

    App->>IT: Write more data
    IT->>IT: Create Snapshot 2
    Note over IT: Snapshot 2: +500 rows<br/>Additional data files

    Consumer->>Pipeline: Request incremental since Snap 1
    Pipeline->>ST: Get snapshot range
    ST-->>Pipeline: Snapshots 1→2
    Pipeline->>IR: Read incremental(S1→S2)
    IR->>IT: Scan manifest diff
    IT-->>IR: Changed data files
    IR->>IT: Read only new files
    IT-->>IR: Incremental data
    IR-->>Pipeline: PyArrow Table
    Pipeline->>Pipeline: Process data
    Pipeline-->>Consumer: Incremental results
    Pipeline->>TM: Update metrics
    TM->>TM: Export to Prometheus
```

### Snapshot Chain and Incremental Read

```mermaid
graph LR
    subgraph "Write Operations"
        W1[Append<br/>1000 rows]
        W2[Append<br/>500 rows]
        W3[Overwrite<br/>Partition]
    end

    subgraph "Snapshot Chain"
        S1[Snapshot 1<br/>Manifest: A]
        S2[Snapshot 2<br/>Manifest: A+B]
        S3[Snapshot 3<br/>Manifest: A+B+C]
    end

    subgraph "Data Files"
        F1[file-1.parquet<br/>1000 rows]
        F2[file-2.parquet<br/>500 rows]
        F3[file-3.parquet<br/>200 rows]
    end

    subgraph "Incremental Reads"
        I1["Read(S0→S1)<br/>Returns: file-1"]
        I2["Read(S1→S2)<br/>Returns: file-2"]
        I3["Read(S2→S3)<br/>Returns: file-3"]
    end

    W1 -->|creates| S1
    W2 -->|creates| S2
    W3 -->|creates| S3

    S1 -->|references| F1
    S2 -->|references| F1
    S2 -->|references| F2
    S3 -->|references| F1
    S3 -->|references| F2
    S3 -->|references| F3

    S1 -.->|identifies new| I1
    S2 -.->|identifies new| I2
    S3 -.->|identifies new| I3

    I1 -->|reads| F1
    I2 -->|reads| F2
    I3 -->|reads| F3

    style I1 fill:#bfb,stroke:#333
    style I2 fill:#bfb,stroke:#333
    style I3 fill:#bfb,stroke:#333
```

## Cross-Storage CDC Pipeline (User Story 5)

```mermaid
graph LR
    subgraph "Source"
        PG2[PostgreSQL]
    end

    subgraph "Transformation"
        KAFKA2[Kafka]
        SPARK[Spark Streaming]
        TRANSFORM[Transformations]
    end

    subgraph "Destination"
        ICE2[Apache Iceberg]
    end

    PG2 -->|CDC| KAFKA2
    KAFKA2 -->|Stream| SPARK
    SPARK -->|Apply| TRANSFORM
    TRANSFORM -->|Write| ICE2
```

## Observability Stack

```mermaid
graph TB
    subgraph "Applications"
        APP1[Postgres CDC]
        APP2[MySQL CDC]
        APP3[Delta CDF]
    end

    subgraph "Metrics Collection"
        PROM_CLIENT[Prometheus Client]
        PROM_SERVER[Prometheus Server]
    end

    subgraph "Logging"
        JSON_LOG[JSON Logs]
        LOKI_SERVER[Loki Server]
    end

    subgraph "Visualization"
        DASHBOARDS[Grafana Dashboards]
    end

    APP1 -->|Export| PROM_CLIENT
    APP2 -->|Export| PROM_CLIENT
    APP3 -->|Export| PROM_CLIENT

    PROM_CLIENT -->|Scrape| PROM_SERVER

    APP1 -->|Write| JSON_LOG
    APP2 -->|Write| JSON_LOG
    APP3 -->|Write| JSON_LOG

    JSON_LOG -->|Push| LOKI_SERVER

    PROM_SERVER -->|Query| DASHBOARDS
    LOKI_SERVER -->|Query| DASHBOARDS
```

## Data Quality Framework

```mermaid
graph TB
    subgraph "Validators"
        ROW[Row Count<br/>Validator]
        CHECKSUM[Checksum<br/>Validator]
        SCHEMA[Schema<br/>Validator]
        LAG[Lag<br/>Monitor]
    end

    subgraph "Orchestrator"
        ORCH[Validation<br/>Orchestrator]
    end

    subgraph "Reporting"
        METRICS[Metrics]
        ALERTS[Alerts]
        REPORTS[Reports]
    end

    ROW -->|Results| ORCH
    CHECKSUM -->|Results| ORCH
    SCHEMA -->|Results| ORCH
    LAG -->|Results| ORCH

    ORCH -->|Export| METRICS
    ORCH -->|Trigger| ALERTS
    ORCH -->|Generate| REPORTS
```

## Deployment Architecture

```mermaid
graph TB
    subgraph "Docker Compose"
        subgraph "Databases"
            PG_C[postgres:15]
            MY_C[mysql:8]
        end

        subgraph "Streaming"
            ZK[zookeeper:7.5]
            KF[kafka:7.5]
            DB[debezium:2.x]
        end

        subgraph "Storage"
            MINIO[minio:latest]
        end

        subgraph "Observability"
            PROM_C[prometheus:latest]
            GRAF_C[grafana:latest]
            LOKI_C[loki:latest]
        end

        subgraph "Application"
            CDC_APP[CDC Pipeline<br/>Python App]
        end
    end

    PG_C <-->|5432| CDC_APP
    MY_C <-->|3306| CDC_APP
    KF <-->|9092| CDC_APP
    DB <-->|8083| CDC_APP
    MINIO <-->|9000| CDC_APP

    CDC_APP -->|Metrics| PROM_C
    CDC_APP -->|Logs| LOKI_C
    PROM_C -->|Data| GRAF_C
    LOKI_C -->|Data| GRAF_C

    ZK <-->|2181| KF
    KF <-->|Connect| DB
    DB <-->|CDC| PG_C
    DB <-->|CDC| MY_C
```

## Technology Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Source Databases** | PostgreSQL 15+, MySQL 8.0+ | Transactional data sources |
| **CDC Engine** | Debezium 2.x | Change data capture |
| **Message Queue** | Apache Kafka 3.x | Event streaming |
| **Processing** | Python 3.11+ | CDC pipeline orchestration |
| **Lakehouse** | DeltaLake, Apache Iceberg | Analytical storage |
| **Object Storage** | MinIO | S3-compatible storage |
| **Metrics** | Prometheus | Metrics collection |
| **Visualization** | Grafana | Dashboards and alerts |
| **Logging** | Loki | Log aggregation |
| **Testing** | pytest | Test framework |

## Network Ports

| Service | Port | Description |
|---------|------|-------------|
| PostgreSQL | 5432 | Database connection |
| MySQL | 3306 | Database connection |
| Kafka | 9092, 29092 | Kafka broker |
| Zookeeper | 2181 | Kafka coordination |
| Debezium | 8083 | Kafka Connect API |
| MinIO API | 9000 | S3-compatible API |
| MinIO Console | 9001 | Web UI |
| Prometheus | 9090 | Metrics API |
| Grafana | 3000 | Dashboard UI |
| Loki | 3100 | Log aggregation |
| Health Check | 8001 | Application health |
| Metrics Export | 8000 | Prometheus exporter |

## Security Considerations

1. **Database Credentials**: Stored in `.env` file (not committed to git)
2. **Kafka Security**: No authentication (demo environment only)
3. **Network Isolation**: All services in Docker bridge network
4. **MinIO**: Default credentials (change in production)
5. **Grafana**: Default admin credentials (change in production)

## Scalability

### Horizontal Scaling
- **Kafka**: Add more brokers and partitions
- **Debezium**: Run multiple connector tasks
- **Consumers**: Deploy multiple pipeline instances with consumer groups

### Vertical Scaling
- **Batch Size**: Increase for higher throughput
- **Spark**: Add more executors and memory
- **Database**: Tune connection pools and resources

## Future Enhancements

1. **Authentication**: Add Kafka SASL/SSL
2. **Encryption**: Enable TLS for all connections
3. **Schema Registry**: Add Confluent Schema Registry
4. **Monitoring**: Add distributed tracing with Jaeger
5. **CI/CD**: Automated testing and deployment pipelines
