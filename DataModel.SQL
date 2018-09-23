CREATE SCHEMA [FiCrawl];

USE FiCrawl;
SELECT * FROM INFORMATION_SCHEMA.TABLES;

CREATE TABLE FiCrawl.Keywords (
  KeywordID varchar(200) NOT NULL PRIMARY KEY,
  FullKeyword varchar(200) NOT NULL
);





CREATE TABLE FiCrawl.Users (
  UserID varchar(150) NOT NULL,
  PRIMARY KEY (UserID)
);


CREATE TABLE FiCrawl.Articles (
  ArticleID varchar(19) NOT NULL PRIMARY KEY,
  Title varchar(500) NOT NULL,
  PublicationDate datetime NOT NULL,
  LastModificationDate datetime NOT NULL,
  NewsPaper varchar(10) NOT NULL,
  URL varchar(500) NOT NULL,
  Priority NUMERIC NOT NULL,
  Language varchar(4) NOT NULL,
  HasBeenParsed BIT NOT NULL,
);




CREATE TABLE FiCrawl.ArticleKeywords (
  ArticleID varchar(19) NOT NULL FOREIGN KEY REFERENCES FiCrawl.Articles (Articleid),
  KeywordID varchar(200) NOT NULL FOREIGN KEY REFERENCES FiCrawl.Keywords (Keywordid)
);



CREATE TABLE FiCrawl.Comments (
  CommentId int NOT NULL PRIMARY KEY,
  Comment varchar(2000) NOT NULL,
  CommentDate datetime NOT NULL,
  IsJournaliste tinyint NOT NULL,
  HasChild tinyint NOT NULL,
  Sentiment int NOT NULL,
  UserId varchar(150) NOT NULL FOREIGN KEY REFERENCES FiCrawl.users(UserID),
  ArticleID varchar(19) NOT NULL FOREIGN KEY REFERENCES FiCrawl.articles(Articleid),
  ParentCommentId int DEFAULT NULL FOREIGN KEY REFERENCES FiCrawl.comments(Commentid)
);


CREATE LOGIN dbrule WITH PASSWORD = 'BenWantLove12'

CREATE USER [dbrule] FOR LOGIN [dbrule] WITH DEFAULT_SCHEMA = FiCrawl;

USE FiCrawl
ALTER ROLE db_datareader ADD MEMBER [dbrule];
ALTER ROLE db_datawriter ADD MEMBER [dbrule];