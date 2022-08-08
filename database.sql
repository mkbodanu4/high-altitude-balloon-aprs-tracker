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
    `comment`      TEXT        NULL,
    `raw`          TEXT        NULL,
    CONSTRAINT `call_sign_date` PRIMARY KEY (`date`, `call_sign`),
    INDEX `call_sign` (`call_sign`)
) ENGINE = InnoDB;