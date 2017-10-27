# Racist Comment Generator

This is a small side project I was working on in late 2016/early 2017. It is about building up a database with comments from facebook pages, analyzing them and then training a recurrent neural network to generate new comments in the same style of language. Of course this would work on any kind of comments, I focused on racist comments though.

## History of this project
The project was not under active development from April to October 2017, so some libs may be a little outdated. Everything works though!

### Crawling
I started of with building a [comments crawler and preprocessor](https://github.com/birnbaum/pi-news-preprocessor) for the anti-islamic blog PI-News. Unfortunately the blog is well protrected against DoS attacs, so crawling is cumbersome and can only be done very slowly. Also PI-News's html is horribly unstructured.

Because of this I switched to directly crawling facebook comments. This is very easy and fast, thanks to the Graph API. `crawl.py` crawls one or more facebook pages you have access to and downloads all the comments and subcomments within a given date window together with additional information about the authors and likes. Everything is then stored in a relational database. Downloading ~400.000 comments with all like-relations from the official [AfD facebook page](https://www.facebook.com/alternativefuerde/) took around 2 days. Downloading ~40.000 comments without any like-relations takes only ~30mins.

### RNN
Next I preprocessed those comments and trained a RNN with the data. First with [Keras](https://github.com/fchollet/keras), later for performance reasons with [torch-rnn](https://github.com/jcjohnson/torch-rnn). The model and it's results were reasonable good but rather boring. It turned out that the comment base was by far not racist enough, "unfortunately" there are way to many people writing Anti-AfD comments on their facebook page.

### Analysis
To futher clean the data I continued with a more detailed data analysis. The main idea was to cluster commenters that like each others posts and then classify the clusters in pro- and anti-AfD commenters based on their language. Unfortunately the entire cluster idea did not work out, because facebook does no longer return the real user ID's via the Graph API for privacy reasons. So building up a graph/clusters is not possible this way.

The second problem was that there is no text corpus (like e.g. [WordNet](https://wordnet.princeton.edu/)) openly available for the german language. This is key for analyzing which words are used often in a text compared to "normal" language.

## Next Steps
- Clean the data: Many comments only contain stickers or links to friends. Some stuff can probably be filtered out with additional information from the Graph API.
- Build own "german facebook comments corpus" by downloading a few million comments from many different pages and political views.
- Get better data than AfD or right wing news papers: Both contain too many anti-racist comments. NPD is better, closed groups obviously best.
- Read more papers about NLP and try out [spaCy](https://spacy.io/) instead of [NLTK](http://www.nltk.org/)
- Improve idempotancy to allow recrawls that add all comments that have not been there on the last crawl. Currently every post can only be crawled once and will be skipped on future runs.