ark
====

Near-duplicate detection for news stories. Pulls a day's worth of stories from
a MongoDB instance, uses the simhash algorithm to cluster the stories, and
outputs the clustered data to a file.

Usage
-----

Requires `docker` and `docker-compose`. Once both of those are installed:

```
docker-compose up -d
```

Where the `-d` flag indicates for the process to run in the background.
