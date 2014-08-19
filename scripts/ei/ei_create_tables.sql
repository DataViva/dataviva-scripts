  CREATE TABLE `ei_ymsrp` (
    `year` int(4) NOT NULL,
    `month` int(2) NOT NULL,
    `bra_id_s` varchar(8) NOT NULL,
    `cnae_id_s` varchar(5) NOT NULL,
    `bra_id_r` varchar(8) NOT NULL,
    `cnae_id_r` varchar(5) NOT NULL,
    `hs_id` varchar(6) NOT NULL,
    `product_value` decimal(16,2) DEFAULT NULL,
    `tax` decimal(16,2) DEFAULT NULL,
    `icms_tax` decimal(16,2) DEFAULT NULL,
    `transportation_cost` decimal(16,2) NULL
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

 CREATE TABLE `ei_ymsr` (
    `year` int(4) NOT NULL,
    `month` int(2) NOT NULL,
    `bra_id_s` varchar(8) NOT NULL,
    `cnae_id_s` varchar(5) NOT NULL,
    `bra_id_r` varchar(8) NOT NULL,
    `cnae_id_r` varchar(5) NOT NULL,
    `product_value` decimal(16,2) DEFAULT NULL,
    `tax` decimal(16,2) DEFAULT NULL,
    `icms_tax` decimal(16,2) DEFAULT NULL,
    `transportation_cost` decimal(16,2) NULL
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

   CREATE TABLE `ei_ymsp` (
    `year` int(4) NOT NULL,
    `month` int(2) NOT NULL,
    `bra_id_s` varchar(8) NOT NULL,
    `cnae_id_s` varchar(5) NOT NULL,
    `hs_id` varchar(6) NOT NULL,
    `product_value` decimal(16,2) DEFAULT NULL,
    `tax` decimal(16,2) DEFAULT NULL,
    `icms_tax` decimal(16,2) DEFAULT NULL,
    `transportation_cost` decimal(16,2) NULL
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

  CREATE TABLE `ei_ymrp` (
    `year` int(4) NOT NULL,
    `month` int(2) NOT NULL,
    `bra_id_r` varchar(8) NOT NULL,
    `cnae_id_r` varchar(5) NOT NULL,
    `hs_id` varchar(6) NOT NULL,
    `product_value` decimal(16,2) DEFAULT NULL,
    `tax` decimal(16,2) DEFAULT NULL,
    `icms_tax` decimal(16,2) DEFAULT NULL,
    `transportation_cost` decimal(16,2) NULL
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8;


  CREATE TABLE `ei_yms` (
    `year` int(4) NOT NULL,
    `month` int(2) NOT NULL,
    `bra_id_s` varchar(8) NOT NULL,
    `cnae_id_s` varchar(5) NOT NULL,

    `product_value` decimal(16,2) DEFAULT NULL,
    `tax` decimal(16,2) DEFAULT NULL,
    `icms_tax` decimal(16,2) DEFAULT NULL,
    `transportation_cost` decimal(16,2) NULL
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

  CREATE TABLE `ei_ymr` (
    `year` int(4) NOT NULL,
    `month` int(2) NOT NULL,
    `bra_id_r` varchar(8) NOT NULL,
    `cnae_id_r` varchar(5) NOT NULL,
    
    `product_value` decimal(16,2) DEFAULT NULL,
    `tax` decimal(16,2) DEFAULT NULL,
    `icms_tax` decimal(16,2) DEFAULT NULL,
    `transportation_cost` decimal(16,2) NULL
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

   CREATE TABLE `ei_ymp` (
    `year` int(4) NOT NULL,
    `month` int(2) NOT NULL,
    `hs_id` varchar(6) NOT NULL,
    
    `product_value` decimal(16,2) DEFAULT NULL,
    `tax` decimal(16,2) DEFAULT NULL,
    `icms_tax` decimal(16,2) DEFAULT NULL,
    `transportation_cost` decimal(16,2) NULL
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8;