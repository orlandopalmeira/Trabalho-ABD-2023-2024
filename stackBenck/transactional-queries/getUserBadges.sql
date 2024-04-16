select array_agg(distinct name)
from badges
where userid = 1