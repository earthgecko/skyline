/*
This is the SQL script to update Skyline from v5.0.0 to
v5.0.0.supplemental.metric.unique-constraint.  If you have duplicate metrics
this SQL will fail.  Just continue with the upgrade process and open an issue on
github.
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

INSERT INTO `sql_versions` (version) VALUES ('5.0.0.supplemental.metric.unique-constraint');
