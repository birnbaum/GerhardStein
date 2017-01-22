import * as Bluebird from 'bluebird';
import * as mariasql from 'mariasql';

const graph: any = Bluebird.promisifyAll(require('fbgraph'));
graph.setAccessToken('EAACEdEose0cBAOCzsh2WdP6pkwXNNvohGqOWE1EhYFPIGDjMX6q9pHZByvCapvGHUR0WeLXxRQLyJTIm9wCGLDM5UZAS9YALi3EconuSxPOCvkEXgeKme9f3uoUQ5x1riDFZC6mUeFSh67D8r1Lg5d0dgjaSm7f5bvc8zKlyQZDZD');
graph.setVersion("2.8");

const startDate = new Date('2016-01-01T00:00:00.000Z');
const endDate = new Date('2016-12-31T23:59:59.999Z');

const c = Bluebird.promisifyAll(new mariasql());
c.connect({
  host: '127.0.0.1',
  user: 'root',
  password: '',
  db: 'facebook'
})

crawl()
    .catch(console.error);

async function crawl() {
    await c.queryAsync("SET NAMES 'utf8'");
    //await crawlPosts();
    await crawlComments();
    c.end();
}

async function crawlComments() {
    await cleanInProgressComments();

    const uncrawledPosts = await c.queryAsync('SELECT id, page, fb_id FROM post WHERE comments_crawled=0');
    for (const post of uncrawledPosts) {
        let fb_comments = await graph.getAsync(post.fb_id+'/comments', {limit: 500});
        console.log(post, fb_comments.data.length)
        await c.queryAsync('UPDATE post SET in_progress=1 WHERE id=?', [post.id]);
        while (fb_comments.data.length > 0) {
            const fb_comment = fb_comments.data.shift();
            const userId = await getOrCreateUser(fb_comment.from);
            const values = [userId, post.id, post.page, fb_comment.id, fb_comment.created_time, fb_comment.message];
            await c.queryAsync('INSERT INTO comment (user, post, page, fb_id, created_time, message) VALUES (?,?,?,?,?,?)', values)
            fb_comments = await paginate(fb_comments);
        }
        await c.queryAsync('UPDATE post SET in_progress=0,comments_crawled=1 WHERE id=?', [post.id]);
    }
}

async function cleanInProgressComments() {
    const inProgressPosts = (await c.queryAsync('SELECT id FROM post WHERE in_progress=1'));
    for (const posts of inProgressPosts) {
        console.log("Cleaning post " + posts.id);
        await c.queryAsync('DELETE FROM comment WHERE post=?', [posts.id]);
        await c.queryAsync('UPDATE post SET in_progress=0 WHERE id=?', [posts.id]);
    }
}

async function getOrCreateUser(user) {
    const userResult = await c.queryAsync('SELECT id FROM user WHERE fb_id=?', [user.id]);
    let userId = -1;
    if (userResult.length >= 2) {
        throw "To many results: " + userResult;
    } else if (userResult.length === 1) {
        return userResult[0].id;
    } else {
        await c.queryAsync('INSERT INTO user (fb_id, name) VALUES (?,?)', [user.id, user.name]);
        return c.lastInsertId();
    }
}

async function crawlPosts() {
    const rows = await c.queryAsync('SELECT * FROM page');
    const page = rows[0];
    let posts = await graph.getAsync(page.fb_id + "/posts", {limit: 100});
    while (posts.data.length > 0) {
        const fb_post = posts.data.shift();
        const createdTime = new Date(fb_post.created_time);
        if(createdTime.getTime() < startDate.getTime()) break;
        if(createdTime.getTime() < endDate.getTime()) {
            try {
                const values = [page.id, fb_post.id, createdTime.toISOString(), fb_post.story, fb_post.message];
                await c.queryAsync('INSERT INTO post (page, fb_id, created_time, story, message) VALUES (?,?,?,?,?)', values);
            } catch(e) {
                console.error(e)
            }
        }
        posts = await paginate(posts);
    }
}

async function paginate(el) {
    if (el.data.length === 0 && el.paging.next) {
        return await graph.getAsync(el.paging.next);
    } else {
        return el;
    }
}