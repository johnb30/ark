import logging
from simhash import Simhash, SimhashIndex

logging.basicConfig(format='%(levelname)s %(asctime)s %(filename)s %(lineno)d: %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def add_to_redis(CONN, hashes):
    logger.info(type(hashes))
    objs = []
    for k, v in hashes.iteritems():
        a = Simhash('a')
        a.value = int(k)
        objs.append((v, a))
    logger.info(objs[0])
    logger.info('Number of objects: {}'.format(len(objs)))
    index = SimhashIndex(CONN, objs, k=3)

    return index


def find_cluster(s, index):
    a = Simhash('a')
    a.value = int(s['simhash'])
    dups = index.get_near_dups(a)

    return dups


def generate_clustered_data(stories, clusters):
    """
    clusters is a dict with an index as key and values as
    story IDs as stored in MongoDB

    clustered is a list of dictionaries. Each dictionary is a cluster
    of stories. Internal dicts have as keys the
    storyid and as values the actual story content. An additional key is
    the cluster ID.
    """
    clustered = []
    for x in clusters.keys():
        mini = {'clust_id': x}
        for sid in clusters[x]:
            mini[sid] = stories[sid]

        clustered.append(mini)

    return clustered
