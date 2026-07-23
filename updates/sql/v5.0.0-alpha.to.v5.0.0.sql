/*
This is the SQL script to update Skyline to v5.0.0-alpha to v5.0.0
*/

USE skyline;

/*
# @added 20250617 - Feature #5318: common_motifs
*/
UPDATE `ionosphere` SET label='LEARNT - common_motifs' WHERE label='LEARNT - motif_annihilation';
UPDATE `ionosphere` SET label='LEARNT - common_motifs' WHERE label='LEARNT - motif_removal';

INSERT INTO `sql_versions` (version) VALUES ('v5.0.0-alpha.to.v5.0.0');
INSERT INTO `sql_versions` (version) VALUES ('v5.0.0');