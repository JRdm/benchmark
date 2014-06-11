CREATE TABLE result AS SELECT pageURL, pageRank FROM rankings WHERE pageRank > 10;
INSERT INTO TABLE result SELECT pageURL, pageRank FROM rankings WHERE pageRank > 10;
