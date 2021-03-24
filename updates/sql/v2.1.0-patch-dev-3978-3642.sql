/*
This is a development SQL script to patch Skyline the initial v2.1.0 and bring
the DB up to date with new index definitions.
*/

USE skyline;

/*
# @added 20210201 - Feature #3934: ionosphere_performance
*/
/*
# @added 20210308 - Feature #3978: luminosity - classify_metrics
#                   Feature #3642: Anomaly type classification
*/
CREATE TABLE IF NOT EXISTS `anomaly_types` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'anomaly type id',
  `algorithm` VARCHAR(255) NOT NULL COMMENT 'algorithm name associated with type, e.g. adtk_level_shift`',
  `type` VARCHAR(255) NOT NULL COMMENT 'anomaly type name, e.g. levelshift`',
  PRIMARY KEY (id),
  INDEX `anomaly_type_id` (`id`,`type`)) ENGINE=InnoDB;
INSERT INTO `anomaly_types` (`algorithm`,`type`) VALUES ('adtk_level_shift', 'level shift');
INSERT INTO `anomaly_types` (`algorithm`,`type`) VALUES ('adtk_volatility_shift', 'volatility shift');
INSERT INTO `anomaly_types` (`algorithm`,`type`) VALUES ('adtk_persist', 'persist');
INSERT INTO `anomaly_types` (`algorithm`,`type`) VALUES ('adtk_seasonal', 'seasonal');

CREATE TABLE IF NOT EXISTS `anomalies_type` (
  `id` INT(11) NOT NULL COMMENT 'anomaly id',
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
  `type` VARCHAR(255) NOT NULL COMMENT 'a csv list of the anomaly_types ids e.g 1,2,3',
  INDEX `anomalies_type_id` (`id`,`metric_id`)) ENGINE=InnoDB;
INSERT INTO `sql_versions` (version) VALUES ('2.1.0-patch-dev-3978-3642');
