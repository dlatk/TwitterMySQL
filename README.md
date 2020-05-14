# TwitterMySQL

TwitterMySQL is a Python library developed by the World Well-Being Project to pull tweets from the Twitter API and insert them into MySQL. twInterface.py is an interface script for TwitterMySQL. 

TwitterMySQL and twInterface work for Python 2.7.


## Getting Started

Before running this you must [sign up](https://twitter.com/signup) for a Twitter account [create](https://apps.twitter.com) a Twitter Authentication Token.

### Authentication

Once you have your tokens there are two was to use Twitter API keys: `--auth` and `--authJSON` (which allows you to cycle through multiple keys once a rate limit is hit). 

When using `--auth`: You must have an authentication file in the following form:

```
CONSUMER_KEY
CONSUMER_SECRET
ACCESS_TOKEN
ACCESS_SECRET
```

When using `--authJSON` you must have an authentication file in the following form:

```
{
  "key1": {"consumer_key":"xxx",
  "consumer_secret":"xxx",
  "access_token_key":"xxx",
  "access_token_secret":"xxx"},
  "key2": {"consumer_key":"xxx",
  "consumer_secret":"xxx",
  "access_token_key":"xxx",
  "access_token_secret":"xxx"},
  "key3": {"consumer_key":"xxx",
  "consumer_secret":"xxx",
  "access_token_key":"xxx",
  "access_token_secret":"xxx"}
}
```

The dictionary key names `key1`, `key2`, etc. do not matter. Any number of keys can be used. They keys will be cycled when hitting a rate limit.


### Get Timelines

You must have a file of usernames / screen names. Here is an example file `example_ids.txt`:

```
169686021
beyonce
taylorswift13
25073877
```

This command will write a table time_lines in the ukTwitter database:

```bash
./twInterface.py -d mySchema -t time_lines --time_lines --user_list ~/example_ids.txt --auth ~/auth
```

### By search terms

This command will write monthly tables pres_XXXX_XX (year_month) into the polTwitter database where each message contains one of the following terms: obama, trump, hillary or bernie.

```bash
./twInterface.py -d polTwitter -t pres --monthly_tables --search_terms obama trump hillary bernie --auth ~/auth
```

### Bounding Box

This command will write monthly tables pres_XXXX_XX (year_month) into the spainTwitter where all messages are contained within the bounding box given by -9.7, 35.85, 3.3, 43.6. For a bounding box you need lat/lon from the bottom left corner and top right corner of your box. (You can find the coordinates using Google Maps.) After the --bounding_box flag you must first enter the bottom left pair (as LON LAT) and then the top right pair (as LON LAT).

```bash
./twInterface.py -d spainTwitter -t msgs --monthly_tables --bounding_box -9.7 35.85 3.3 43.6 --auth ~/auth
```

### Country Bounding Box

If you want to pull tweets from an entire country you only need to use a two letter country code instead of the 2 lat/lon coordinates:

```bash
./twInterface.py -d netherlandsTwitter -t msgs --monthly_tables --country_bounding_box nl --auth ~/auth
```

### Random Stream

Takes the random sample of all tweets (~ 1%)

```bash
./twInterface.py -d randomTwitter -t msgs --random_stream --auth ~/auth
```

### Filter for spam

Use the `--check_spam` flag to create a new column in MySQL which indicates if the message is spam. Checks message against a list of commonly used spam words:

"share", "win", "check", "enter", "products", "awesome", "prize", "sweeps", "bonus", "gift"

### Profile Pictures 

**Not fully tested**: Given a list of user ids / screen names this will download large profile pictures into the directory specified by `--output_file` (in this example it will write to `~/pictures_pp`). All files are jpgs with user_id as name.  Note that this method does not save data to MySQL.

```bash
./twInterface.py --profile_pictures --user_list ~/example_ids.txt --auth ~/auth --output_file ~/pictures 
```

The above user ids file will create the following images:

```
 169686021.jpg  17919972.jpg  25073877.jpg  31239408.jpg
```

### Social Networks

**Not fully tested**: Given a list of user ids / screen names this will download a social network (list of follower and friend ids) into the directory specified by `--output_file` (in this example it will write to `~/networks_sn`). Note that this method does not save data to MySQL.

```
./twInterface.py --social_network --user_list ~/example_ids.txt --auth ~/auth --output_file ~/networks
```

Each network is saved in a separate JSON file in the folder specified by `--output_file`. Each file is named after the user id or screen name. The JSON contains the following key/values: ("user", user id or screen name), ("friends_list", array of friend user_ids) and ("followers_list", array of friend user_ids). Example file `169686021.json`:

```
 {"friends_list": [16129920, 5688592, 16827489, 15163466, 807095, 7691312, 19394188, 2384071, 14361155], 
  "user": "169686021", 
  "followers_list": [54831465, 64986608, 14689326]}
```

### Help

Full list of flags:

```
usage: twInterface.py [-h] [-d DB] [-t TABLE] [--auth AUTHFILE]
                      [--authJSON AUTHFILEJSON] [--monthly_tables]
                      [--output_file OUTPUTFILE]
                      [--bounding_box FIELDS) [FIELD(S) ...]] [--time_lines]
                      [--search_terms [FIELD(S) [FIELD(S ...]]]
                      [--search_term_file SEARCHFILE] [--random_stream]
                      [--social_network] [--profile_pictures]
                      [--user_list USERLIST] [--check_spam]

Pull tweets from Twitter API

optional arguments:
  -h, --help            show this help message and exit
  -d DB                 MySQL database where tweets will be stored.
  -t TABLE              MySQL table name. If monthly tables then M_Y will be
                        appended to end of this string. Default: msgs
  --auth AUTHFILE       Path to authentication file which contains (line
                        separated) CONSUMER_KEY, CONSUMER_SECRET,
                        ACCESS_TOKEN, ACCESS_SECRET.
  --authJSON AUTHFILEJSON
                        Path to JSON authentication file which can contain
                        multiple keys. These keys will be cycled when rate
                        limited.
  --monthly_tables      Turn on writing to monthly tables
  --output_file OUTPUTFILE
                        File where output from --social_network and
                        --profile_pictures will be written. Default: twOutput
  --bounding_box FIELD(S) [FIELD(S) ...]
                        Pull tweets from bounding box. Must specify 3 or 4
                        cooridnates, space separated: LAT LON RADIUS (in
                        miles) or LON LAT LON LAT (First pair is from bottom
                        left corner of box, second pair is from top right
                        corner).
  --time_lines          Pull timelines for given list of user names / screen
                        names.
  --search_terms [FIELD(S) [FIELD(S) ...]]
                        Pull tweets containing keywords. Must supply space
                        separated list of search terms. Ex: taylorswift #1989
                        #arianagrande
  --search_term_file SEARCHFILE
                        Optional file containing list of search terms to use
  --random_stream       Grab data from the random stream.
  --social_network      Pull social network for given list of user names /
                        screen names. Must specify path to file which contains
                        list.
  --profile_pictures    Pull profile pictures for given list of user names /
                        screen names. Must specify path to file which contains
                        list.
  --user_list USERLIST  File containing list of user ids / screen names
  --check_spam          Check each message for spam
```

## Tweet data

This is a list of what is stored in MySQL and its location in the Tweet JSON

```
'message_id': "['id_str']",
'user_id': "['user']['id_str']",
'message': "['text']",
'created_at_utc': "['created_at']",
'retweeted': "['retweeted']",
'retweet_message_id': "['retweeted_status']['id']",
'in_reply_to_message_id': "['in_reply_to_status_id_str']",
'in_reply_to_user_id': "['in_reply_to_user_id_str']",
'favorite_count': "['favorite_count']",
'retweet_count': "['retweet_count']",
'source': "['source']",
'message_lang': "['lang']",
'user_handle': "['user']['screen_name']",
'user_desc': "['user']['description']",
'user_lang': "['user']['lang']",
'time_zone': "['user']['id_str']",
'utc_offset': "['user']['utc_offset']",
'friends_count': "['user']['friends_count']",
'followers_count': "['user']['followers_count']",
'user_location': "['user']['location']",
'street_address': "['place']['attributes']['street_address']",
'region': "['place']['attributes']['region']",
'postal_code': "['place']['attributes']['postal_code']",
'bb_coordinates': "['place']['bounding_box']['coordinates']",
'country': "['place']['country']",
'country_code': "['place']['country_code']",
'tweet_location': "['place']['full_name']",
'tweet_location_short': "['place']['name']",
'place_type': "['place']['place_type']" 
```

## Dependencies
- [MySQLdb](http://mysql-python.sourceforge.net/MySQLdb.html)
- [TwitterAPI](https://github.com/geduldig/TwitterAPI)
- argparse
- requests


## License

Licensed under a [GNU General Public License v3 (GPLv3)](https://www.gnu.org/licenses/gpl-3.0.en.html)

## Background

Developed by the [World Well-Being Project](http://www.wwbp.org) based out of the University of Pennsylvania.
