BEGIN;
TRUNCATE country;
INSERT INTO `country` (`name`, `population`) VALUES ('Mexico',  130497248);
INSERT INTO `country` (`name`, `population`) VALUES ('Spain', 49067981);
INSERT INTO `country` (`name`, `population`) VALUES ('Colombia', 46070146);
COMMIT;