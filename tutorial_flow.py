from metaflow import FlowSpec, step, retry, catch, batch, IncludeFile, Parameter, conda, conda_base

def get_python_version():
    """
    A convenience function to get the python version used to run this
    tutorial. This ensures that the conda environment is created with an
    available version of python.

    """
    import platform
    versions = {'2' : '2.7.15',
                '3' : '3.7.4'}
    return versions[platform.python_version_tuple()[0]]


# Use the specified version of python for this flow.
@conda_base(python=get_python_version())
class MovieStatsFlow(FlowSpec):
    """
    A flow to generate some statistics about the movie genres.

    The flow performs the following steps:
    1) Ingests a CSV into a Pandas Dataframe.
    2) Fan-out over genre using Metaflow foreach.
    3) Compute quartiles for each genre.
    4) Save a dictionary of genre specific statistics.

    """
    movie_data = IncludeFile("movie_data",
                             help="The path to a movie metadata file.",
                             default = 'movies.csv')
                             
    maxgenres = Parameter('maxgenres',
                help="The max number of genres to return statistics for",
                default=5)

    @conda(libraries={'pandas' : '0.24.2'})
    @step
    def start(self):
        """
        The start step:
        1) Loads the movie metadata into pandas dataframe.
        2) Finds all the unique genres.
        3) Launches parallel statistics computation for each genre.

        """
        import pandas
        from io import StringIO

        # Load the data set into a pandas dataaframe.
        self.dataframe = pandas.read_csv(StringIO(self.movie_data))

        # The column 'genres' has a list of genres for each movie. Let's get
        # all the unique genres.
        self.genres = {genre for genres \
                       in self.dataframe['genres'] \
                       for genre in genres.split('|')}
        self.genres = list(self.genres)

        # We want to compute some statistics for each genre. The 'foreach'
        # keyword argument allows us to compute the statistics for each genre in
        # parallel (i.e. a fan-out).
        self.next(self.compute_statistics, foreach='genres')

    @catch(var='compute_failed')
    @retry
    @conda(libraries={'pandas' : '0.25.3'})
    @step
    def compute_statistics(self):
        """
        Compute statistics for a single genre.

        """
        # The genre currently being processed is a class property called
        # 'input'.
        self.genre = self.input
        print("Computing statistics for %s" % self.genre)

        # Find all the movies that have this genre and build a dataframe with
        # just those movies and just the columns of interest.
        selector = self.dataframe['genres'].\
                   apply(lambda row: self.genre in row)
        self.dataframe = self.dataframe[selector]
        self.dataframe = self.dataframe[['movie_title', 'genres', 'gross']]

        # Get some statistics on the gross box office for these titles.
        points = [.25, .5, .75]
        self.quartiles = self.dataframe['gross'].quantile(points).values

        # Join the results from other genres.
        self.next(self.join)

    @conda(libraries={'pandas' : '0.25.3'})
    @step
    def join(self, inputs):
        """
        Join our parallel branches and merge results into a dictionary.

        """
        inputs = inputs[0:self.maxgenres]
        # Merge results from the genre specific computations.
        self.genre_stats = {inp.genre.lower(): \
                            {'quartiles': inp.quartiles,
                             'dataframe': inp.dataframe} \
                            for inp in inputs}

        self.next(self.end)

    @step
    def end(self):
        """
        End the flow.

        """
        pass


if __name__ == '__main__':
    MovieStatsFlow()