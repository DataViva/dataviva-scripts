 --  CREATE TABLE `ei_raw` (
 --    `ncm` varchar(8) NOT NULL,
 --    `hs_id` varchar(4) NOT NULL,
 --    `cnae_id_r` varchar(5) NOT NULL,
 --    `cnae_id_s` varchar(5) NOT NULL,
 --    `cfop_old` TEXT NULL,
 --    `cfop` int(4) NOT NULL,
 --    `Receiver_Type` TEXT NULL,
 --    `Sender_Type` TEXT NULL,
 --    `bra_id_r` varchar(8) NOT NULL,
 --    `bra_id_s` varchar(8) NOT NULL,
 --    `year` int(4) NOT NULL,
 --    `month` int(2) NOT NULL,
 --    `Receiver_Situation` TEXT NULL,
 --    `Sender_Situation` TEXT NULL,
 --    `transportation_cost` decimal(16,2) NULL,
 --    `ICMS_ST_Value` decimal(16,2) DEFAULT NULL,
 --    `ICMS_ST_RET_Value` decimal(16,2) DEFAULT NULL,
 --    `ICMS_Value` decimal(16,2) DEFAULT NULL,
 --    `IPI_Value` decimal(16,2) DEFAULT NULL,
 --    `PIS_Value` decimal(16,2) DEFAULT NULL,
 --    `COFINS_Value` decimal(16,2) DEFAULT NULL,
 --    `II_Value` decimal(16,2) DEFAULT NULL,
 --    `product_value` decimal(16,2) DEFAULT NULL,
 --    `ISSQN_Value` decimal(16,2) DEFAULT NULL,
 --    `Origin` varchar(2) NOT NULL
 -- ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

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