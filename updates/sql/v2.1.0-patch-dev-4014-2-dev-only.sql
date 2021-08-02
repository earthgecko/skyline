/*
This is a development SQL script to patch Skyline from v2.1.0-patch-dev-4014-dev-only
*/

USE skyline;

/*
# @added 20210427 - Feature #4014: Ionosphere - inference
#                   Branch #3590: inference
# Store the motif_area, fp_motif_area and percent_different
*/
ALTER TABLE `motifs_matched` ADD COLUMN `motif_area` DOUBLE DEFAULT 0 COMMENT 'the motif area from the composite trapezoidal rule' AFTER `distance`;
COMMIT;
ALTER TABLE `motifs_matched` ADD COLUMN `fp_motif_area` DOUBLE DEFAULT 0 COMMENT 'the fp motif area from the composite trapezoidal rule' AFTER `motif_area`;
COMMIT;
ALTER TABLE `motifs_matched` ADD COLUMN `area_percent_diff` DOUBLE DEFAULT 0 COMMENT 'the percent difference of the motif_area from the fp_motif_area' AFTER `fp_motif_area`;
COMMIT;

INSERT INTO `sql_versions` (version) VALUES ('2.1.0-patch-dev-4014-2-dev-only');
