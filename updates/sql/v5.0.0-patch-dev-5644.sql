/*
This is the SQL script to update Skyline to v5.0.0-alpha with self_validated
*/

USE skyline;

/*
# @added 20260705 - Feature #5644: ionosphere.learn_self_validation
#                   Feature #5318: common_motifs
# Added self_validated_only and set learn_self_validation as self_validated but
# not their echo fps because those are not self_validated
*/
ALTER TABLE `ionosphere` ADD COLUMN `self_validated` tinyint(1) DEFAULT 0 COMMENT 'whether the features profile was self validated, 1 being yes and 0 being no' AFTER `validated`;
CREATE INDEX self_validated ON ionosphere (self_validated, id);

UPDATE ionosphere SET self_validated=1 WHERE id IN (
    SELECT DISTINCT fp_id FROM comments WHERE
    comment LIKE 'learn_self_validation - validated at %' AND
    fp_id IS NOT NULL);

INSERT INTO `sql_versions` (version) VALUES ('v5.0.0-alpha-patch-dev-5644');
/*
INSERT INTO `sql_versions` (version) VALUES ('v5.0.0');
*/
