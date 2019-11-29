select setval('sample_id_seq',(select max(id) from sample ));
select setval('sample_attribute_id_seq',(select max(id) from sample_attribute));