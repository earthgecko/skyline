/*
This is the SQL script to update Skyline to v5.0.0-alpha with unique constraint
on metric in the the metrics table.
You must have run skyline/updates/deduplicate.metrics.v5.0.0.py and have
deduplicated all metrics before running this.
*/

USE skyline;

/*
# @added 20260425 - Task #5176: Migrate to sqlalchemy v2 API
#                   Task #5713: Test CentOS Stream 10
# Added unique constraint on metric
*/
ALTER TABLE metrics CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
ALTER TABLE metrics ADD CONSTRAINT uq_metrics_metric UNIQUE (metric);
/*

INSERT INTO `sql_versions` (version) VALUES ('v5.0.0-alpha-patch-dev-5721');
INSERT INTO `sql_versions` (version) VALUES ('v5.0.0');