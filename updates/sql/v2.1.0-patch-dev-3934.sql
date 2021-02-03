/*
This is a development SQL script to patch Skyline the initial v2.1.0 and bring
the DB up to date with new index definitions.
*/

USE skyline;

/*
# @added 20210201 - Feature #3934: ionosphere_performance
*/
ALTER TABLE `ionosphere_matched` DROP INDEX `features_profile_matched`;
ALTER TABLE `ionosphere_matched` ADD INDEX `features_profile_matched` (`id`,`fp_id`,`metric_timestamp`);
ALTER TABLE `ionosphere_layers_matched` DROP INDEX `layers_matched`;
ALTER TABLE `ionosphere_layers_matched` ADD INDEX `layers_matched` (`id`,`layer_id`,`fp_id`,`metric_id`,`anomaly_timestamp`);
INSERT INTO `sql_versions` (version) VALUES ('2.1.0-patch-dev-3934');
