from metaflow import Flow, get_metadata

# Print metadata provider
print("Using metadata provider: %s" % get_metadata())

# Load the analysis from the MovieStatsFlow.
run = Flow('GenreStatsFlow').latest_successful_run
print("Using analysis from '%s'" % str(run))

genre_stats = run.data.genre_stats
print(genre_stats)