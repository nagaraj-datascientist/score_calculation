''' This module is used to connect the cassandra database

class       : CassandraCluster
functions   : cassandra_session (Creates session for the given credentials)
              pandas_result_set (returns pandas df for the given query)
              cluster_shutdown (close the connection)
'''

import logging.config

import pandas as pd
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy

# Initialize log
logger = logging.getLogger(__name__)


class CassandraCluster:
    '''Cassandra cluster connection.'''

    def __init__(self, ip, port, user, pwd):
        self.logger = logging.getLogger(__name__)
        self.ip = ip
        self.port = port
        self.user = user
        self.pwd = pwd
        self.logger.debug(self)

    def cassandra_session(self):
        '''Creates session for the cassandra cluser
                Uses policy as DCAwareRoundRobinPolicy,
                    protocol version as 3 and
                    control connection timeout as 100

            Returns session
        '''
        auth_provider = PlainTextAuthProvider(
            username=self.user, password=self.pwd)
        cluster = Cluster(
            contact_points=[self.ip],
            load_balancing_policy=DCAwareRoundRobinPolicy(
                local_dc='datacenter1'),
            port=self.port,
            auth_provider=auth_provider,
            control_connection_timeout=100,
            protocol_version=3
        )
        session = cluster.connect()

        self.logger.debug(session)

        return cluster, session

    def pandas_result_set(self, session, keyspace, query):
        '''Creates result set as pandas dataframe

            Input arguments:
                session (obj)  - Cassandra cluster session object
                keyspace (str) - keyspace value
                query (str)    - query to be executed
            Output argument:
                df             - Query result set as Pandas dataframe

            Reference:
                https://groups.google.com/a/lists.datastax.com/g/python-driver-user/c/1v-KHtyA0Zs
                https://www.thetopsites.net/article/59318754.shtml
        '''
        session.set_keyspace(keyspace)

        def pandas_factory(colnames, rows):
            return pd.DataFrame(rows, columns=colnames)

        session.row_factory = pandas_factory
        session.default_fetch_size = None

        result = session.execute(query, timeout=None)
        df = result._current_rows

        return df

    def cluster_shutdown(self, cluster):
        '''Shut down the cassandra cluster'''
        if not cluster.is_shutdown:
            cluster.shutdown()
            self.logger.info('Cassandra cluster is shutdown')

    def __repr__(self):
        return f'''CassandraCluster('{self.ip}', {self.port},
                                        '{self.user}', '{self.pwd}')'''
