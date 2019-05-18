CREATE TABLE if not exists `huaban_image_infos` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `infos` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `infos` (`infos`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE if not exists `huaban_complete_image_infos`(
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `infos` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `infos` (`infos`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8