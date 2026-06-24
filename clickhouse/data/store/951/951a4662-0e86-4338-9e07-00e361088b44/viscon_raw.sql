ATTACH TABLE _ UUID '88fc12ea-e328-40f7-a94f-38594fad677c'
(
    `CycleId` UInt64,
    `PlantPosition` UInt16,
    `MeasureTime` DateTime64(3),
    `Batch` String,
    `Grade` LowCardinality(String),
    `Id` UInt64,
    `Min` Float32,
    `Max` Float32,
    `Average` Float32,
    `Characteristic` LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(MeasureTime)
ORDER BY (Grade, Characteristic, toDate(MeasureTime))
SETTINGS index_granularity = 8192
