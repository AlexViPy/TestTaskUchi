CREATE TABLE IF NOT EXISTS USERS.sessions (
    `ts` DateTime('Europe/Moscow'),
    `userId` Nullable(String),
    `sessionId` Nullable(Int16),
    `page` Nullable(String),
    `auth` Nullable(String),
    `method` Nullable(FixedString(3)),
    `status` Nullable(Int16),
    `level` Nullable(FixedString(4)),
    `itemInSession` Int16,
    `location` Nullable(String),
    `userAgent` Nullable(String),
    `lastName` Nullable(String),
    `firstName` Nullable(String),
    `registration` Nullable(DateTime('Europe/Moscow')),
    `gender` Nullable(FixedString(1)),
    `artist` Nullable(String),
    `song` Nullable(String),
    `length` Nullable(Float64)) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ts)
    ORDER BY (itemInSession)