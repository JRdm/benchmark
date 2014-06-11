CREATE TABLE result AS SELECT pageURL, pageRank FROM rankings WHERE pageRank > 1000;
INSERT INTO TABLE result SELECT pageURL, pageRank FROM rankings WHERE pageRank > 1000;
