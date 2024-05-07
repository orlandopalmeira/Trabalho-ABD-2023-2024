select sum(case when vt.name = 'UpMod' then 1 when vt.name = 'DownMod' then -1 else 0 end) 
from votes v 
join votestypes vt on vt.id = v.votetypeid 
where v.postid = 31127474

CREATE INDEX idx_votes_postid ON votes (postid);
CREATE INDEX idx_votes_votetypeid ON votes (votetypeid);

DROP INDEX IF EXISTS idx_votes_postid;
DROP INDEX IF EXISTS idx_votes_votetypeid;
