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

--Add fields tm_job_type, tm_publication + constraint. Add CAF-252
ALTER TABLE TASKS ADD (tm_job_type VARCHAR(255));
UPDATE TASKS SET tm_job_type = 'Analysis';
ALTER TABLE TASKS MODIFY (tm_job_type NOT NULL);

ALTER TABLE TASKS ADD (tm_publication VARCHAR(1));
UPDATE TASKS SET tm_publication = 'F';
ALTER TABLE TASKS MODIFY (tm_publication NOT NULL);

ALTER TABLE TASKS ADD CONSTRAINT check_tm_publication CHECK (tm_publication in ('T' , 'F'));

-- Add fields panda_submitted_jobs, tm_save_logs + constraint. CAF-291
ALTER TABLE TASKS ADD (panda_resubmitted_jobs CLOB);

ALTER TABLE TASKS ADD (tm_save_logs VARCHAR(1));
UPDATE TASKS SET tm_save_logs = 'F';
ALTER TABLE TASKS MODIFY (tm_save_logs NOT NULL);

ALTER TABLE TASKS ADD CONSTRAINT check_tm_save_logs CHECK (tm_save_logs in ('T' , 'F'));
