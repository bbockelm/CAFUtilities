--Add fmd_tmp_location, CAF-231
--Cannot just add a not null field like:
--ALTER TABLE FILEMETADATA ADD (fmd_tmp_location VARCHAR(255) NOT NULL);
ALTER TABLE FILEMETADATA ADD (fmd_tmp_location VARCHAR(255));
UPDATE FILEMETADATA SET fmd_tmp_location = 'undefined';
ALTER TABLE FILEMETADATA MODIFY (fmd_tmp_location NOT NULL);

--Add fmd_creation_time field. CAF-242
ALTER TABLE FILEMETADATA ADD (fmd_creation_time TIMESTAMP);
UPDATE FILEMETADATA SET fmd_creation_time = SYS_EXTRACT_UTC(SCN_TO_TIMESTAMP(ora_rowscn));
ALTER TABLE FILEMETADATA MODIFY (fmd_creation_time NOT NULL);

--Update len of some fields of the db. CAF-202
ALTER TABLE filemetadata MODIFY fmd_outdataset VARCHAR(500);
ALTER TABLE filemetadata MODIFY fmd_lfn VARCHAR(500);
ALTER TABLE tasks MODIFY tm_input_dataset VARCHAR(500);
ALTER TABLE tasks MODIFY tm_site_whitelist VARCHAR(4000);
ALTER TABLE tasks MODIFY tm_site_blacklist VARCHAR(4000);
ALTER TABLE tasks MODIFY tm_publish_name VARCHAR(500);
