CREATE INDEX idx_votes_creationdate ON votes (creationdate);
CREATE INDEX idx_users_creationdate ON users (creationdate);

CREATE INDEX idx_users_creationdate_year on users (extract(year from creationdate));

-- DROP INDEX idx_votes_creationdate;
-- DROP INDEX idx_users_creationdate;
-- DROP INDEX idx_users_creationdate_year