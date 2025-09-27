CREATE TABLE train_files (
    id SERIAL PRIMARY KEY,
    filename TEXT UNIQUE,
    filesize INT,
    downloaded BOOLEAN DEFAULT FALSE
);


select count(*) from train_files tf ; --2:10

select count(*) from  train_files where downloaded = true;

select sum(filesize ) from  train_files where downloaded = true;


DO $$
DECLARE
    total float;
    downloaded float;
    r float;
BEGIN
    select count(*) INTO downloaded  from train_files tf where tf.downloaded = true;
    select count(*) INTO total  from train_files tf;
    r := downloaded / total * 100.0;
    RAISE NOTICE 'ratio: %', r;
END $$;


select * from train_files tf limit 20; 


select sum(tf.filesize ) from train_files tf;
