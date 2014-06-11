CREATE TABLE result AS SELECT pageURL, pageRank FROM rankings WHERE pageRank > 100;
INSERT INTO TABLE result SELECT pageURL, pageRank FROM rankings WHERE pageRank > 100;
