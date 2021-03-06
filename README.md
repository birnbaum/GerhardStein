# Gerhard Stein
A racist chatbot trained on german facebook comments.

Gerhard's current prototype is trained on 380,000 comments from german far right pages (mostly NPD related) on a small RNN for about 24h on my laptop.
The name "Gerhard Stein" was determined by searching for the 88th most popular forename and the 88th most popular surname in this data set.
Believe it or not, apparently there even was a famous Nazi called [Gerhart Stein](https://de.wikipedia.org/wiki/Gerhart_Stein).

Gerhard already produces some funny sentences, although he does many grammar mistakes and most of the stuff he says makes no sense at all (which is something it may have learned from the training data).

See this example conversation. It is not cherry picked and all answers are generated by the chatbot:
```
> Guten Morgen!
Genau so ist es dann auch nicht das deutsche Kinder aufgezogen.
> Was denn für Kinder?
Wenn du willst du das auch deutsche Kinder haben würdest du das auch keine Ausländer die deutsche sind, auch unsere Aussagen die sie dieses Problem der Vergewaltigungen und ihre Schuld nicht wissen, wenn wir deutschen auch nicht mehr......
> Das macht keinen Sinn
Genau so ist es nicht verstanden!
> Und was denkst du von Merkel?
Du bist doch nicht besser...
> Alles klar, ciao!
Ich will die NPD wählen.
```

I already extended the data set to contain more than 2,900,000 comments and want to train a new model on a larger network, stay tuned! I will publish the model once I'm done.

### The Crawler
Is idempotent and can be configured useing `config.yml`.
If you rerun it, it will only download the newest comments based on some heuristics.
You can also add new pages to the configuration at any time, the crawler will download the missing parts without creating any duplicates.

The crawler is fast, downloading up to 250,000 comments per hour.


### The RNN
Is mostly based on [this amazing project](https://github.com/pender/chatbot-rnn). Everything is built on top of the [tensorflow port](https://github.com/sherjilozair/char-rnn-tensorflow) of Andrej Karpathy's [char-rnn](https://github.com/karpathy/char-rnn).

Result after training on a 256 unit, 3 layer RNN for 24h on a i7 Laptop CPU:
![Tensorboard](img/tensorboard.png)


### Analysis
To futher clean the data my next steps will be a more detailed data analysis.
Besides obvious 'dirt' (like comments that only contain emojis or tagged friends) I am for example facing the issue that there a quite a lot of anti-fascist comments on far right facebook pages.
As I want the data set to be as racist as possible, I want to avoid training on anti-racist comments and would like to filter them out.

In the beginning my main idea was to cluster commenters in groups that like each other's posts and then classify the clusters in racist and anti-racist commenters based on their language. Unfortunately the entire cluster idea did not work out, because facebook does no longer return the real user ID's via the Graph API for privacy reasons. So building up a graph is not possible at all.

What I want to try next is to focus more on the language itself. Unfortunately there is no text corpus (like e.g. [WordNet](https://wordnet.princeton.edu/)) openly available for the german language.
This is key for analyzing which words are used often in a text compared to "normal" language.
I will probably try building up a corpus on my own by crawling all kinds of other facebook pages.


## Next steps
- Improve the preprocessing to generate more usefull "conversations"
- Find some powerful machine to train a new model
- Analyse the data and write a blog article
- Twitter bot?
- Build an interactive frontend with the model running on [deeplearn.js](https://github.com/PAIR-code/deeplearnjs)

One final thought I have is to come up with a way to convert a facebook comment into a vector that indicates it's topic/intention/style, similar to what e.g. word2vec does with words. This way one can easily train a classifier to identify hate speech. First I need to get more into NLP though...


## How to build your own Chatbot based on Facebook comments
1. Create a database on a MySQL/MariaDB server and import `schema.sql`
2. Adapt `config.yml` to define the pages you want to have in your data set
3. Run `crawl.py` to download the comments
4. Run `generate_dataset.py` to produce the data set used for training
5. Run `train.py` to train a new model
6. Run `chatbot.py` to test it out