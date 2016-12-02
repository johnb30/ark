import redis
import logging
import argparse

from pymongo import MongoClient


logging.basicConfig(format='%(levelname)s %(asctime)s %(filename)s %(lineno)d: %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def parse_arguments():
    parser = argparse.ArgumentParser(description='Run the clustering API.')
    parser._optionals.title = 'Options'
    parser.add_argument('-rc', '--redis_conn',
                        help='IP info for the redis connection.',
                        type=str, required=True, default='ark')
    parser.add_argument('-mc', '--mongo_conn',
                        help='IP info for the mongo connection.',
                        type=str, required=True)
    return parser.parse_args()


def make_conn(args):
    """
    Function to establish a connection to a local MonoDB instance.


    Returns
    -------

    collection: pymongo.collection.Collection.
                Collection within MongoDB that holds the scraped news stories.

    """

    client = MongoClient(args.mongo_conn)
    database = client.event_scrape
    collection = database['news_stories']
    return collection


def make_redis_conn(args):
    CONN = redis.StrictRedis(host=args.redis_conn, port=6379, db=0)

    return CONN


def query_all(collection, lt_date, gt_date, write_file=False):
    """
    Function to query the MongoDB instance and obtain results for the desired
    date range. The query constructed is: greater_than_date > results
    < less_than_date.

    Parameters
    ----------

    collection: pymongo.collection.Collection.
                Collection within MongoDB that holds the scraped news stories.

    lt_date: Datetime object.
                    Date for which results should be older than. For example,
                    if the date running is the 25th, and the desired date is
                    the 24th, then the `lt_date` is the 25th.

    gt_date: Datetime object.
                        Date for which results should be older than. For
                        example, if the date running is the 25th, and the
                        desired date is the 24th, then the `gt_date`
                        is the 23rd.

    sources: List.
                Sources to pull from the MongoDB instance.

    write_file: Boolean.
                Option indicating whether to write the results from the web
                scraper to an intermediate file. Defaults to false.

    Returns
    -------

    posts: List.
            List of dictionaries of results from the MongoDB query.


    final_out: String.
                If `write_file` is True, this contains a string representation
                of the query results. Otherwise, contains an empty string.

    """

    final_out = ''

    posts = collection.find({"$and": [{"date_added": {"$lte": lt_date}},
                                      {"date_added": {"$gt": gt_date}}]})

    logging.info('Total number of stories: {}'.format(posts.count()))

    posts = list(posts)
    logger.info('Num posts: {}'.format(len(posts)))

    return posts, final_out
