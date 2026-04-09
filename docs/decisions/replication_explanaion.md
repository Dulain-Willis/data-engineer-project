
## Overview
Minio stores iceberg bronze and silver tables transformed using pypark. The analytics layer is hosted in clickhouse with dbt. The silver tables need to be replicated into clickhouse for anlytics use cases. I use iceberg snapshots to be able to tell what has changed since the last replication and what needs ot be updated rather than a traditional watermark.

## How Iceberg Snapshots Work
An iceberg snapshot represent the full table state at a given point in time. The flow is Snapshot → Manifest List → Manifest Files → Data Files (with partition info). 

Every snapshot is attatched to a manifest_list

| snapshot_id      | manifest_list                                                        |
|------------------|----------------------------------------------------------------------|
| 8472619384710234 | s3://lakehouse/steamspy/silver/metadata/snap-8472619384710234-1.avro |

That manifest list points to manifest files                                                                         
                                                                                                                    
| manifest_path                                              |
|------------------------------------------------------------|
| s3a://warehouse/steamspy/silver/metadata/manifest-A.avro   |
| s3a://warehouse/steamspy/silver/metadata/manifest-B.avro   |

Each manifest file points to data files                                                                    
   
manifest-A.avro:

| file_path                                                             | partition   |
|-----------------------------------------------------------------------|-------------|
| s3a://warehouse/steamspy/silver/data/dt=2026-01-01/part-00001.parquet | {2026-01-01} |
| s3a://warehouse/steamspy/silver/data/dt=2026-01-01/part-00002.parquet | {2026-01-01} |

manifest-B.avro:

| file_path                                                             | partition   |
|-----------------------------------------------------------------------|-------------|
| s3a://warehouse/steamspy/silver/data/dt=2026-01-02/part-00001.parquet | {2026-01-02} |

For example on the first day an iceberg table is ran the folder structure looks like...

snapshot_1
  └── manifest list
      └── manifest_A.avro  → [dt=2026-01-01 files]

On day 2 it looks like

snapshot_2
  └── manifest list                                                                                                   
      ├── manifest_A.avro  → [dt=2026-01-01 files]  ← reused, unchanged                                             
      └── manifest_B.avro  → [dt=2026-01-02 files]  ← new     

Iceberg reuses manifest files across snapshots. When you write 2026-01-02, the new snapshot gets a new manifest list 
that references:
  - A new manifest file for the new 2026-01-02 data files                                                             
  - The existing manifest file(s) from the previous snapshot (carried over by reference, not copied)  

Why this matters for the replication use case:                                                                     
                                                                                                                    
When you diff two snapshots, Iceberg compares their manifest lists to find which data files were added/removed.     
That's what makes incremental replication efficient, you're not scanning partition directories, you're comparing
manifest metadata. Each snapshot is self contained. 

For all intents an purposes we can think of an iceberg snapshot as a metadata file that points the data files that make up an iceberg table at a given point in time. 

The first thing to know about the replication spark job is that when it finishes writing a new snapshot to clickhouse it updates a separate state table in clickhouse. Here it uploads the snapshot_id of the snapshot just uploaded and the iceberg table the snapshot came from. This is important becuase one of the first things we do is query that clickhouse table to find out what the most recent snapshot_id replicated to clickhouse was. 

```
SELECT last_snapshot_id 
FROM {STATE_TABLE} FINAL 
WHERE table_name = %(table_name)s
```


replication spark job gets the snapshot_id of the most recent iceberg snapshot. Then gets the snapshot_id of the most recent clickhouse snapshot. It gets this because eveytime a snapshot is written to clickhouse the spark job updates a clickhouse state table with the snapshot_id that was jus/t written. 
