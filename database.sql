CREATE TABLE `history`
(
    `date`         DATETIME    NOT NULL,
    `call_sign`    VARCHAR(20) NOT NULL,
    `timestamp`    BIGINT      NULL,
    `latitude`     FLOAT       NULL,
    `longitude`    FLOAT       NULL,
    `course`       INT         NULL,
    `speed`        FLOAT       NULL,
    `altitude`     FLOAT       NULL,
    `daodatumbyte` VARCHAR(1)  NULL,
    `comment`      TEXT        NULL,
    `raw`          TEXT        NULL,
    CONSTRAINT `call_sign_date` PRIMARY KEY (`date`, `call_sign`),
    UNIQUE `call_sign_values` (`call_sign`, `latitude`, `longitude`, `altitude`, `speed`, `comment`)
) ENGINE = InnoDB;