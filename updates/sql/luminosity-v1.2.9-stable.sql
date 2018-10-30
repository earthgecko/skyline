/*
This is the SQL script to update Skyline from ionosphere (v1.2.8 to v1.2.9)
*/

USE skyline;

# @added 20181025 - Bug #2638: anomalies db table - anomalous_datapoint greater than DECIMAL
# Changed DECIMAL(18,6) to DECIMAL(65,6)
ALTER TABLE `anomalies` MODIFY `anomalous_datapoint` DECIMAL(65,6)
