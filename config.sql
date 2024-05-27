-- Set shared_buffers to 4GB
ALTER SYSTEM SET shared_buffers = '4GB';

-- Set work_mem to 16MB
ALTER SYSTEM SET work_mem = '16MB';

-- Set max_parallel_workers_per_gather to 4
ALTER SYSTEM SET max_parallel_workers_per_gather = 4;

-- Set max_parallel_workers to 16
ALTER SYSTEM SET max_parallel_workers = 16;

-- Reload the configuration
SELECT pg_reload_conf();
