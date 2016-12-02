import json
import time
import utils
import clust
import logging
import schedule
import datetime


logging.basicConfig(format='%(levelname)s %(asctime)s %(filename)s %(lineno)d: %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def process(conn, r_conn):
    current_date = datetime.datetime.utcnow() - datetime.timedelta(days=1)
    less_than = datetime.datetime(current_date.year, current_date.month,
                                  current_date.day)
    greater_than = less_than - datetime.timedelta(days=1)
    less_than = less_than + datetime.timedelta(days=1)

    results, text = utils.query_all(conn, less_than, greater_than)
    logger.info('Obtained results...')

    #Reduced should be a dict with keys as storyids and values as dictionaries
    #of story info, e.g., title, etc
    clustered = cluster_stories(conn, r_conn, results)

    return clustered


def cluster_stories(m_conn, r_conn, stories):
    seen = []
    date = datetime.datetime.utcnow().strftime('%Y%m%d')
    n = 0
    clusters = {}
    cluster_lens = []

    hashes = {}
    for x in stories:
        hashes[x['simhash']] = x['_id']
    logger.info('Hashes: {}'.format(len(hashes)))
    logger.info(type(hashes))
    index = clust.add_to_redis(r_conn, hashes)
    for s in stories:
        if s['_id'] in seen:
            pass
        else:
            cluster_ids = clust.find_cluster(s, index)
            cluster_lens.append(len(cluster_ids))
            seen += cluster_ids
            clust_id = '{}_{}'.format(date, n)
            clusters[clust_id] = cluster_ids
            n += 1

    stories_by_id = {x['_id']: x for x in stories}

    clustered = clust.generate_clustered_data(stories_by_id, clusters)

    total_clusts = len(clustered)
    total_ids = len(seen)
    avg_clusts = sum(cluster_lens) / float(len(cluster_lens))
    for x in clustered:
        x['cluster_info'] = {'total_clusts': total_clusts,
                             'total_ids': total_ids,
                             'avg_clusts': avg_clusts}
    logger.info('Total clusters: {}'.format(total_clusts))
    logger.info('Total IDs: {}'.format(total_ids))

    return clustered


def make_friendly(clustered):
    stops = ['clust_id', 'cluster_info']
    for x in clustered:
        ids = [z for z in x.keys() if z not in stops]
        for i in ids:
            try:
                x[i]['date_added'] = x[i]['date_added'].strftime("%Y-%m-%dT%H:%M:%S")
            except Exception:
                pass

    return clustered


def main(args):
    conn = utils.make_conn(args)
    r_conn = utils.make_redis_conn(args)
    logger.info('... waiting ...')
    time.sleep(30)
    clustered_stories = process(conn, r_conn)
    to_send = make_friendly(clustered_stories)
    logger.info('Generated clustered stories. Writing to file.')
    for x in to_send:
        with open('output_file.json', 'a') as f:
            f.write(json.dumps(x) + '\n')


def run():
    args = utils.parse_arguments()
    logger.info('Hitting main...')
    main(args)

if __name__ == '__main__':
    logger.info('...booting...')
    #run()
    schedule.every().day.at("2:30").do(run)

    while True:
        schedule.run_pending()
        time.sleep(1)
