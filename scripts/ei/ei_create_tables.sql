-- Create syntax for TABLE 'ei_ymp'
CREATE TABLE `ei_ymp` (
  `year` int(4) NOT NULL,
  `month` int(2) NOT NULL,
  `hs_id` varchar(6) NOT NULL,
  `tax` decimal(16,2) DEFAULT NULL,
  `icms_tax` decimal(16,2) DEFAULT NULL,
  `transportation_cost` decimal(16,2) DEFAULT NULL,
  `purchase_value` decimal(16,2) DEFAULT NULL,
  `transfer_value` decimal(16,2) DEFAULT NULL,
  `devolution_value` decimal(16,2) DEFAULT NULL,
  `icms_credit_value` decimal(16,2) DEFAULT NULL,
  `remit_value` decimal(16,2) DEFAULT NULL,
  `hs_id2` varchar(2) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Create syntax for TABLE 'ei_ymr'
CREATE TABLE `ei_ymr` (
  `year` int(4) NOT NULL,
  `month` int(2) NOT NULL,
  `bra_id_r` varchar(9) NOT NULL DEFAULT '',
  `cnae_id_r` varchar(5) NOT NULL,
  `tax` decimal(16,2) DEFAULT NULL,
  `icms_tax` decimal(16,2) DEFAULT NULL,
  `transportation_cost` decimal(16,2) DEFAULT NULL,
  `purchase_value` decimal(16,2) DEFAULT NULL,
  `transfer_value` decimal(16,2) DEFAULT NULL,
  `devolution_value` decimal(16,2) DEFAULT NULL,
  `icms_credit_value` decimal(16,2) DEFAULT NULL,
  `remit_value` decimal(16,2) DEFAULT NULL,
  `bra_id_r1` varchar(1) DEFAULT NULL,
  `bra_id_r3` varchar(3) DEFAULT NULL,
  `cnae_id_r1` varchar(1) DEFAULT NULL,
  KEY `bra_id_r` (`bra_id_r`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Create syntax for TABLE 'ei_ymrp'
CREATE TABLE `ei_ymrp` (
  `year` int(4) NOT NULL,
  `month` int(2) NOT NULL,
  `bra_id_r` varchar(9) NOT NULL DEFAULT '',
  `cnae_id_r` varchar(5) NOT NULL,
  `hs_id` varchar(6) NOT NULL,
  `tax` decimal(16,2) DEFAULT NULL,
  `icms_tax` decimal(16,2) DEFAULT NULL,
  `transportation_cost` decimal(16,2) DEFAULT NULL,
  `purchase_value` decimal(16,2) DEFAULT NULL,
  `transfer_value` decimal(16,2) DEFAULT NULL,
  `devolution_value` decimal(16,2) DEFAULT NULL,
  `icms_credit_value` decimal(16,2) DEFAULT NULL,
  `remit_value` decimal(16,2) DEFAULT NULL,
  `bra_id_r1` varchar(1) DEFAULT NULL,
  `bra_id_r3` varchar(3) DEFAULT NULL,
  `cnae_id_r1` varchar(1) DEFAULT NULL,
  `hs_id2` varchar(2) DEFAULT NULL,
  PRIMARY KEY (`year`,`month`,`bra_id_r`,`cnae_id_r`,`hs_id`),
  KEY `month` (`month`),
  KEY `cnae_id_r` (`cnae_id_r`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Create syntax for TABLE 'ei_yms'
CREATE TABLE `ei_yms` (
  `year` int(4) NOT NULL,
  `month` int(2) NOT NULL,
  `bra_id_s` varchar(9) NOT NULL DEFAULT '',
  `cnae_id_s` varchar(5) NOT NULL,
  `tax` decimal(16,2) DEFAULT NULL,
  `icms_tax` decimal(16,2) DEFAULT NULL,
  `transportation_cost` decimal(16,2) DEFAULT NULL,
  `purchase_value` decimal(16,2) DEFAULT NULL,
  `transfer_value` decimal(16,2) DEFAULT NULL,
  `devolution_value` decimal(16,2) DEFAULT NULL,
  `icms_credit_value` decimal(16,2) DEFAULT NULL,
  `remit_value` decimal(16,2) DEFAULT NULL,
  `bra_id_s1` varchar(1) DEFAULT NULL,
  `bra_id_s3` varchar(3) DEFAULT NULL,
  `cnae_id_s1` varchar(1) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Create syntax for TABLE 'ei_ymsp'
CREATE TABLE `ei_ymsp` (
  `year` int(4) NOT NULL,
  `month` int(2) NOT NULL,
  `bra_id_s` varchar(9) NOT NULL DEFAULT '',
  `cnae_id_s` varchar(5) NOT NULL,
  `hs_id` varchar(6) NOT NULL,
  `tax` decimal(16,2) DEFAULT NULL,
  `icms_tax` decimal(16,2) DEFAULT NULL,
  `transportation_cost` decimal(16,2) DEFAULT NULL,
  `purchase_value` decimal(16,2) DEFAULT NULL,
  `transfer_value` decimal(16,2) DEFAULT NULL,
  `devolution_value` decimal(16,2) DEFAULT NULL,
  `icms_credit_value` decimal(16,2) DEFAULT NULL,
  `remit_value` decimal(16,2) DEFAULT NULL,
  `bra_id_s3` varchar(3) DEFAULT NULL,
  `bra_id_s1` varchar(1) DEFAULT NULL,
  `cnae_id_s1` varchar(1) DEFAULT NULL,
  `hs_id2` varchar(2) DEFAULT NULL,
  PRIMARY KEY (`year`,`month`,`bra_id_s`,`cnae_id_s`,`hs_id`),
  KEY `bra_id_s3` (`bra_id_s3`),
  KEY `cnae_id_s` (`cnae_id_s`),
  KEY `year` (`year`),
  KEY `month` (`month`),
  KEY `bra_id_s` (`bra_id_s`),
  KEY `hs_id` (`hs_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Create syntax for TABLE 'ei_ymsr'
CREATE TABLE `ei_ymsr` (
  `year` int(4) NOT NULL,
  `month` int(2) NOT NULL,
  `bra_id_s` varchar(9) NOT NULL DEFAULT '',
  `cnae_id_s` varchar(5) NOT NULL,
  `bra_id_r` varchar(9) NOT NULL DEFAULT '',
  `cnae_id_r` varchar(5) NOT NULL,
  `tax` decimal(16,2) DEFAULT NULL,
  `icms_tax` decimal(16,2) DEFAULT NULL,
  `transportation_cost` decimal(16,2) DEFAULT NULL,
  `purchase_value` decimal(16,2) DEFAULT NULL,
  `transfer_value` decimal(16,2) DEFAULT NULL,
  `devolution_value` decimal(16,2) DEFAULT NULL,
  `icms_credit_value` decimal(16,2) DEFAULT NULL,
  `remit_value` decimal(16,2) DEFAULT NULL,
  `bra_id_s1` varchar(1) DEFAULT NULL,
  `bra_id_s3` varchar(3) DEFAULT NULL,
  `cnae_id_s1` varchar(1) DEFAULT NULL,
  `bra_id_r1` varchar(1) DEFAULT NULL,
  `bra_id_r3` varchar(3) DEFAULT NULL,
  `cnae_id_r1` varchar(1) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Create syntax for TABLE 'ei_ymsrp'
CREATE TABLE `ei_ymsrp` (
  `year` int(4) NOT NULL,
  `month` int(2) NOT NULL,
  `bra_id_s` varchar(9) NOT NULL DEFAULT '',
  `cnae_id_s` varchar(3) NOT NULL DEFAULT '',
  `bra_id_r` varchar(9) NOT NULL DEFAULT '',
  `cnae_id_r` varchar(5) NOT NULL,
  `hs_id` varchar(6) NOT NULL,
  `tax` decimal(16,2) DEFAULT NULL,
  `icms_tax` decimal(16,2) DEFAULT NULL,
  `transportation_cost` decimal(16,2) DEFAULT NULL,
  `purchase_value` decimal(16,2) DEFAULT NULL,
  `transfer_value` decimal(16,2) DEFAULT NULL,
  `devolution_value` decimal(16,2) DEFAULT NULL,
  `icms_credit_value` decimal(16,2) DEFAULT NULL,
  `remit_value` decimal(16,2) DEFAULT NULL,
  `bra_id_s1` varchar(1) DEFAULT NULL,
  `bra_id_s3` varchar(3) DEFAULT NULL,
  `cnae_id_s1` varchar(1) DEFAULT NULL,
  `bra_id_r1` varchar(1) DEFAULT NULL,
  `bra_id_r3` varchar(3) DEFAULT NULL,
  `cnae_id_r1` varchar(1) DEFAULT NULL,
  `hs_id2` varchar(2) DEFAULT NULL,
  KEY `bra_id_r` (`bra_id_r`),
  KEY `bra_id_r3` (`bra_id_r3`),
  KEY `hs_id` (`hs_id`),
  KEY `cnae_id_r` (`cnae_id_r`),
  KEY `bra_id_s` (`bra_id_s`),
  KEY `bra_id_s_2` (`bra_id_s`,`bra_id_r`),
  KEY `bra_id_s3` (`bra_id_s3`,`bra_id_r3`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;