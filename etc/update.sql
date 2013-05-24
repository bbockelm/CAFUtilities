--Cannot just add a not null field like:
--ALTER TABLE FILEMETADATA ADD (fmd_tmp_location VARCHAR(255) NOT NULL);

ALTER TABLE FILEMETADATA ADD (fmd_tmp_location VARCHAR(255));
UPDATE FILEMETADATA SET fmd_tmp_location = 'undefined';
ALTER TABLE FILEMETADATA MODIFY (fmd_tmp_location NOT NULL);
