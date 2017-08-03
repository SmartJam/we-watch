CREATE DATABASE we_watch DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_unicode_ci;

CREATE TABLE `id_mappings`(
  `id` INT PRIMARY KEY AUTO_INCREMENT COMMENT '',
  `srcPlatform` char(31) NOT NULL COMMENT '源平台',
  `srcId` varchar(127) NOT NULL COMMENT '源平台的id'
  `createAt` datetime
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- DROP TABLE IF EXISTS `movies_80s`;
CREATE TABLE `movies_80s` (
  `id` INT PRIMARY KEY COMMENT '',
  `name` varchar(127) NOT NULL DEFAULT '' COMMENT '',
  `aliases` VARCHAR(255) COMMENT '别名',
  `stars` VARCHAR(255) COMMENT '演员',
  `genres` VARCHAR(127) COMMENT '作品类型',
  `region` VARCHAR(31) COMMENT '地区',
  `languages` VARCHAR(31) COMMENT '语言',
  `director` VARCHAR(31) COMMENT '导演',
  `showTime` DATE COMMENT '上映时间',
  `duration` VARCHAR(15) COMMENT '片长',
  `platformUpdatedAt` DATE COMMENT '平台更新时间',
  `doubanScore` DECIMAL(12,1) COMMENT '豆瓣评分',
  `doubanCommentLink` VARCHAR(255) COMMENT '豆瓣短评链接',
  `outline` TEXT COMMENT '电影简介',
  `createdAt` DATETIME,
  `updatedAt` DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;


DROP TABLE IF EXISTS `video_src_80s`;
CREATE TABLE `video_src_80s` (
  `movieId` INT COMMENT '',
  `videoFormat` CHAR(15) COMMENT '视频格式',
  `videoNo` INT COMMENT '',
  `title` VARCHAR(255) COMMENT '视频名',
  `size` CHAR(15) COMMENT '视频大小',
  `videoSrc` TEXT COMMENT '视频源',
  `createdAt` DATETIME,
  `updatedAt` DATETIME,
  primary key (`movieId`, `videoFormat`, `videoNo`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

