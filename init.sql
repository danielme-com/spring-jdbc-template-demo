CREATE DATABASE `country-test`;
CREATE USER 'demo'@'%' IDENTIFIED BY 'demo';
GRANT ALL PRIVILEGES ON `country-test`.* TO 'demo'@'%';
FLUSH PRIVILEGES;
USE `country-test`;
CREATE TABLE `countries`
(
    `id`         SMALLINT     NOT NULL AUTO_INCREMENT,
    `name`       varchar(255) NOT NULL,
    `population` int(11)      NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `UK_1` (`name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = latin1;

COMMIT;

DELIMITER //
CREATE
    DEFINER = `root`@`localhost` PROCEDURE `proc_count_countries_by_name`(IN name VARCHAR(50), OUT total INT)
BEGIN
    SELECT COUNT(*) INTO total FROM countries c WHERE c.name LIKE CONCAT('%', @name, '%');
END//
DELIMITER ;

SET GLOBAL log_bin_trust_function_creators = 1;

DELIMITER //
CREATE FUNCTION `func_count_countries_by_name`(name VARCHAR(50)) RETURNS INT
BEGIN
    DECLARE total INT;
    SELECT COUNT(*)
    INTO total
    FROM countries c
    WHERE c.name LIKE CONCAT('%', @name , '%');

    RETURN total;
END//
DELIMITER ;