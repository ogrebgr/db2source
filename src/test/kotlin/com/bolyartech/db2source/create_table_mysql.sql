CREATE TABLE `test` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `intcol` int(10) NOT NULL,
  `varcharcol` varchar(45) NOT NULL,
  `textcol` text NOT NULL,
  `timecol` time NOT NULL,
  `datetimecol` datetime NOT NULL,
  `floatcol` float NOT NULL,
  `doublecol` double NOT NULL,
  `datecol` date NOT NULL,
  `tscol` timestamp NOT NULL,
  `longtextcol` longtext NOT NULL,
  `booleancol` tinyint(4) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8