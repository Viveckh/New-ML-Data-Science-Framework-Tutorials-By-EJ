import hiplot as hip

def fetch_local_csv_experiment(uri):
    # Only apply this fetcher if the URI starts with webxp://
    PREFIX="localcsvxp://"

    if not uri.startswith(PREFIX):
        # Let other fetchers handle this one
        raise hip.ExperimentFetcherDoesntApply()
    
    # Parse out the local file path from the uri
    local_path = uri[len(PREFIX):]  # Remove the prefix

    # Return the hiplot experiment to render
    return hip.Experiment.from_csv(local_path)