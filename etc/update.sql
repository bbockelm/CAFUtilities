/* tm_split_args has to become a CLOB */
/* using a temporary column to do the switch */ 
ALTER TABLE tasks ADD (tmp_tm_split_args  CLOB);
UPDATE tasks SET tmp_tm_split_args=tm_split_args;
COMMIT;
ALTER TABLE tasks DROP COLUMN tm_split_args;
ALTER TABLE tasks RENAME COLUMN tmp_tm_split_args TO tm_split_args;

/* removing unused tm_data_runs column */
ALTER TABLE tasks drop column tm_data_runs;

/* tm_publish_name has to be longer */
ALTER TABLE tasks MODIFY (tm_publish_name VARCHAR2(1000));


/* dynamically removing constraint from jobgroup */
declare
  code VARCHAR2(4000) := 'ALTER TABLE JOBGROUPS DROP CONSTRAINT |ConstraintName| ';
  constr VARCHAR2(1000);
begin
  select constraint_name into constr from all_constraints
      where owner = 'CRAB_MCINQUIL' and
            constraint_type = 'U' and
            table_name = 'JOBGROUPS';
  code := REPLACE(code, '|ConstraintName|', constr);
  EXECUTE IMMEDIATE code;
end;
/
/* adding new column to jobgroup */
ALTER TABLE JOBGROUPS ADD (tm_user_dn VARCHAR(255) NOT NULL);
COMMIT;

/* populate the new column */
UPDATE JOBGROUPS jg SET tm_user_dn = (SELECT tm_user_dn FROM TASKS t WHERE t.tm_taskname = jg.tm_taskname);

/* adding new correct constraint to jobgroup */
ALTER TABLE JOBGROUPS ADD constraint task_user_un UNIQUE(tm_taskname, tm_user_dn);
COMMIT;
