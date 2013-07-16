--Add tm_taskname foreign key to filemetadata. Improve performances of some queries (CAF-355).
ALTER TABLE filemetadata add CONSTRAINT fk_tm_taskname FOREIGN KEY (tm_taskname) REFERENCES tasks (tm_taskname);
