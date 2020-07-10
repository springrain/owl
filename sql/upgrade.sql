use n9e_mon;
ALTER TABLE event ADD cur_node_path varchar(255) AFTER nid;
ALTER TABLE event ADD cur_nid varchar(255) AFTER nid;

ALTER TABLE event_cur ADD cur_node_path varchar(255) AFTER nid;
ALTER TABLE event_cur ADD cur_nid varchar(255) AFTER nid;


ALTER TABLE maskconf ADD category int AFTER id;

create table `maskconf_nids` (
  `id` int unsigned not null auto_increment,
  `mask_id` int unsigned not null,
  `nid` varchar(255) not null,
  `path` varchar(255) not null,
  primary key (`id`),
  key(`mask_id`)
) engine=innodb default charset=utf8;