BEGIN;
TRUNCATE countries;
INSERT INTO `countries` (`id`,`name`, `population`) VALUES (1, 'Mexico',  130497248);
INSERT INTO `countries` (`id`, `name`, `population`) VALUES (2, 'Spain', 49067981);
INSERT INTO `countries` (`id`,`name`, `population`) VALUES (3, 'Colombia', 46070146);
COMMIT;