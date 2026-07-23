/*
This is the SQL script to update Skyline to v4.1.0-patch-dev-5592-b
*/

/*
# @added 20250123 - Feature #5592: tenant_id column in DB tables
# Remove tenant_id from primary indices that was added in v4.1.0-patch-dev-5592.sql
# it is not required in the primary indices.
*/

USE skyline;

ALTER TABLE `metrics` DROP INDEX `metric`;
ALTER TABLE `metrics` ADD INDEX `metric` (`id`,`metric`(255),`ionosphere_enabled`,`inactive`);
ALTER TABLE `anomalies` DROP INDEX `anomaly`;
ALTER TABLE `anomalies` ADD INDEX `anomaly` (`id`,`metric_id`,`host_id`,`app_id`,`source_id`,`anomaly_timestamp`,`full_duration`,`triggered_algorithms`,`created_timestamp`);
ALTER TABLE `ionosphere` DROP INDEX `features_profile`;
ALTER TABLE `ionosphere` ADD INDEX `features_profile` (`id`,`metric_id`,`enabled`,`layers_id`,`validated`,`alias_id`);
ALTER TABLE `ionosphere_layers` DROP INDEX `ionosphere_layers`;
ALTER TABLE `ionosphere_layers` ADD INDEX `ionosphere_layers` (`id`,`fp_id`,`metric_id`);
ALTER TABLE `ionosphere_layers_matched` DROP INDEX `layers_matched`;
ALTER TABLE `ionosphere_layers_matched` ADD INDEX `layers_matched` (`id`,`layer_id`,`fp_id`,`metric_id`,`anomaly_timestamp`);
ALTER TABLE `motifs_matched` DROP INDEX `inference_motifs_matched`;
ALTER TABLE `motifs_matched` ADD INDEX `inference_motifs_matched` (`id`,`metric_id`,`fp_id`,`metric_timestamp`,`primary_match`,`validated`);

INSERT INTO `sql_versions` (version) VALUES ('4.1.0-patch.dev.5592-b');
