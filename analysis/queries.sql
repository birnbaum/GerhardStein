# Project name
SELECT
	(SELECT SUBSTRING_INDEX(user.name, ' ', 1) as first_name
	FROM user
	JOIN comment ON comment.user = user.id
	GROUP BY first_name
	ORDER BY count(*) DESC
	LIMIT 1
	OFFSET 88) as first_name,
  (SELECT SUBSTRING_INDEX(user.name, ' ', -1) as last_name
  FROM user
  JOIN comment ON comment.user = user.id
  GROUP BY last_name
  ORDER BY count(*) DESC
  LIMIT 1
  OFFSET 88) as last_name;

# Count by Page
SELECT page.name as name, count(*) as count
FROM page
JOIN comment ON comment.page = page.id
GROUP BY page.id
UNION
SELECT 'TOTAL' as name, count(*) as count
FROM comment
ORDER BY count DESC;


# Delete all posts and comments of a page
DELETE FROM comment WHERE page=1 AND parent_comment IS NOT NULL;
DELETE FROM comment WHERE page=1;
DELETE FROM post WHERE page=1;