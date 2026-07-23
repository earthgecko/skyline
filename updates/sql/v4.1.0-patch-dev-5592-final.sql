/*
This is the SQL script to update Skyline to v4.1.0-patch-dev-5592-final
*/

/*
# @added 20250123 - Feature #5592: tenant_id column in DB tables
# Add the tenant_id columns to tables
*/

USE skyline;

ALTER TABLE `metrics` ADD COLUMN `tenant_id` INT(11) DEFAULT 0 COMMENT 'the tenant_id that the metric belongs to, if 0 none' AFTER `metric`;
ALTER TABLE `metrics` ADD INDEX `tenant_id_idx` (tenant_id, id);
ALTER TABLE `anomalies` ADD COLUMN `tenant_id` INT(11) DEFAULT 0 COMMENT 'the tenant_id that the metric belongs to, if 0 none' AFTER `metric_id`;
ALTER TABLE `anomalies` ADD INDEX `tenant_time_idx` (tenant_id, anomaly_timestamp, id);
ALTER TABLE `ionosphere` ADD COLUMN `tenant_id` INT(11) DEFAULT 0 COMMENT 'the tenant_id that the metric belongs to, if 0 none' AFTER `metric_id`;
ALTER TABLE `ionosphere` ADD INDEX `tenant_id_idx` (tenant_id, id);
ALTER TABLE `ionosphere_layers` ADD COLUMN `tenant_id` INT(11) DEFAULT 0 COMMENT 'the tenant_id that the metric belongs to, if 0 none' AFTER `metric_id`;
ALTER TABLE `ionosphere_layers` ADD INDEX `tenant_id_idx` (tenant_id, id);
ALTER TABLE `ionosphere_layers_matched` ADD COLUMN `tenant_id` INT(11) DEFAULT 0 COMMENT 'the tenant_id that the metric belongs to, if 0 none' AFTER `metric_id`;
ALTER TABLE `ionosphere_layers_matched` ADD INDEX `tenant_time_idx` (tenant_id, anomaly_timestamp, id);
ALTER TABLE `motifs_matched` ADD COLUMN `tenant_id` INT(11) DEFAULT 0 COMMENT 'the tenant_id that the metric belongs to, if 0 none' AFTER `metric_id`;
ALTER TABLE `motifs_matched` ADD INDEX `tenant_time_idx` (tenant_id, metric_timestamp, id);

INSERT INTO `sql_versions` (version) VALUES ('4.1.0-patch.dev.5592-final');
