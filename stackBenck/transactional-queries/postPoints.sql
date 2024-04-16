select sum(case when vt.name = 'UpMod' then 1 when vt.name = 'DownMod' then -1 else 0 end) 
    from votes v 
    join votestypes vt on vt.id = v.votetypeid 
    where v.postid = 31127474

CREATE INDEX idx_votes_postid ON votes (postid); -- Repetido na q2-v3.sql
CREATE INDEX idx_votes_votetypeid ON votes (votetypeid);
CREATE INDEX idx_votestypes_id ON votestypes (id);

DROP INDEX IF EXISTS idx_votes_postid;
DROP INDEX IF EXISTS idx_votes_votetypeid;
DROP INDEX IF EXISTS idx_votestypes_id;