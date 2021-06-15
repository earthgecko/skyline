/*
This is a development SQL script to patch Skyline from v2.1.0-patch-dev-4014-dev-only
*/

USE skyline;

/*
# @added 20210428 - Feature #4014: Ionosphere - inference
#                   Branch #3590: inference
# Store the fps_checked and runtime
*/
ALTER TABLE `motifs_matched` ADD COLUMN `fps_checked` INT(11) DEFAULT 0 COMMENT 'the number of features profiles checked' AFTER `area_percent_diff`;
COMMIT;
ALTER TABLE `motifs_matched` ADD COLUMN `runtime` DOUBLE DEFAULT 0 COMMENT 'the inference runtime' AFTER `fps_checked`;
COMMIT;

INSERT INTO `motif_matched_types` (`id`,`type`) VALUES (4, 'not_similar_enough');
INSERT INTO `motif_matched_types` (`id`,`type`) VALUES (5, 'INVALIDATED');
INSERT INTO `motif_matched_types` (`id`,`type`) VALUES (6, 'distance');

INSERT INTO `sql_versions` (version) VALUES ('2.1.0-patch-dev-4014-3-dev-only');
