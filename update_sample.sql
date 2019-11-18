delete from sample_attribute where sample_id in (select sample_id from dataset_sample where dataset_id in (select id from dataset where identifier='100XXX'));
delete from sample where id in (select sample_id from dataset_sample where dataset_id in (select id from dataset where identifier = '100XXX'));
