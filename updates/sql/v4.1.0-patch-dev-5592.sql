/*
This is the SQL script to update Skyline to v4.1.0-patch-dev-5592
*/

/*
# @added 20250122 - Feature #5592: tenant_id column in DB tables
# Add the tenant_id columns to tables
*/

USE skyline;

ALTER TABLE `metrics` ADD COLUMN `tenant_id` INT(11) DEFAULT 0 COMMENT 'the tenant_id that the metric belongs to, if 0 none' AFTER `metric`;
ALTER TABLE `metrics` DROP INDEX `metric`;
ALTER TABLE `metrics` ADD INDEX `metric` (`id`,`metric`(255),`tenant_id`,`ionosphere_enabled`,`inactive`);
ALTER TABLE `metrics` ADD INDEX `tenant_id_idx` (tenant_id, id);
ALTER TABLE `anomalies` ADD COLUMN `tenant_id` INT(11) DEFAULT 0 COMMENT 'the tenant_id that the metric belongs to, if 0 none' AFTER `metric_id`;
ALTER TABLE `anomalies` DROP INDEX `anomaly`;
ALTER TABLE `anomalies` ADD INDEX `anomaly` (`id`,`metric_id`,`tenant_id`,`host_id`,`app_id`,`source_id`,`anomaly_timestamp`,`full_duration`,`triggered_algorithms`,`created_timestamp`);
ALTER TABLE `anomalies` ADD INDEX `tenant_time_idx` (tenant_id, anomaly_timestamp, id);
ALTER TABLE `ionosphere` ADD COLUMN `tenant_id` INT(11) DEFAULT 0 COMMENT 'the tenant_id that the metric belongs to, if 0 none' AFTER `metric_id`;
ALTER TABLE `ionosphere` DROP INDEX `features_profile`;
ALTER TABLE `ionosphere` ADD INDEX `features_profile` (`id`,`metric_id`,`tenant_id`,`enabled`,`layers_id`,`validated`,`alias_id`);
ALTER TABLE `ionosphere` ADD INDEX `tenant_id_idx` (tenant_id, id);
ALTER TABLE `ionosphere_layers` ADD COLUMN `tenant_id` INT(11) DEFAULT 0 COMMENT 'the tenant_id that the metric belongs to, if 0 none' AFTER `metric_id`;
ALTER TABLE `ionosphere_layers` DROP INDEX `ionosphere_layers`;
ALTER TABLE `ionosphere_layers` ADD INDEX `ionosphere_layers` (`id`,`fp_id`,`metric_id`,`tenant_id`);
ALTER TABLE `ionosphere_layers` ADD INDEX `tenant_id_idx` (tenant_id, id);
ALTER TABLE `ionosphere_layers_matched` ADD COLUMN `tenant_id` INT(11) DEFAULT 0 COMMENT 'the tenant_id that the metric belongs to, if 0 none' AFTER `metric_id`;
ALTER TABLE `ionosphere_layers_matched` DROP INDEX `layers_matched`;
ALTER TABLE `ionosphere_layers_matched` ADD INDEX `layers_matched` (`id`,`layer_id`,`fp_id`,`metric_id`,`tenant_id`,`anomaly_timestamp`);
ALTER TABLE `ionosphere_layers_matched` ADD INDEX `tenant_time_idx` (tenant_id, anomaly_timestamp, id);
ALTER TABLE `motifs_matched` ADD COLUMN `tenant_id` INT(11) DEFAULT 0 COMMENT 'the tenant_id that the metric belongs to, if 0 none' AFTER `metric_id`;
ALTER TABLE `motifs_matched` DROP INDEX `inference_motifs_matched`;
ALTER TABLE `motifs_matched` ADD INDEX `inference_motifs_matched` (`id`,`metric_id`,`tenant_id`,`fp_id`,`metric_timestamp`,`primary_match`,`validated`);
ALTER TABLE `motifs_matched` ADD INDEX `tenant_time_idx` (tenant_id, metric_timestamp, id);

INSERT INTO `sql_versions` (version) VALUES ('4.1.0-patch.dev.5592');
