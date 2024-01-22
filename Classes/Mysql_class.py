from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from tqdm import tqdm

class Mysql_Class:
    """
    A class for handling MySQL operations including creating databases and uploading tables.

    Attributes:
        user (str): The username for MySQL authentication.
        host (str): The hostname or IP address of the MySQL server.
        password (str): The password for MySQL authentication.
    """

    def __init__(self, user, host, password):
        """
        Initialize the Mysql_Class instance with user credentials.

        Args:
            user (str): The username for MySQL authentication.
            host (str): The hostname or IP address of the MySQL server.
            password (str): The password for MySQL authentication.
        """
        self.user = user
        self.host = host
        self.password = password

    def Upload_tables(self, db_name, dfs_list, dfs_names):
        """
        Uploads tables to a specified MySQL database.

        Args:
            db_name (str): The name of the database where the tables are to be uploaded.
            dfs_list (list): A list of Pandas DataFrame objects to be uploaded as tables.
            dfs_names (list): A list of table names corresponding to the DataFrames in dfs_list.

        Raises:
            SQLAlchemyError: If an error occurs during database operations.
        """
        # Create an engine that doesn't connect to a specific database
        engine = create_engine(f'mysql+mysqlconnector://{self.user}:{self.password}@{self.host}')

        try:
            # Establish a connection using the engine
            with engine.connect() as conn:
                # Check if the database exists
                result = conn.execute(text(f"SHOW DATABASES LIKE '{db_name}';")).fetchone()

                # Create the database if it does not exist
                if result is None:
                    conn.execute(text(f"CREATE DATABASE {db_name};"))
                    print(f"The database {db_name} was created.")
                else:
                    print(f"The database {db_name} already exists.")

            # Dispose the previous engine
            engine.dispose()

            # Create a new engine connected to the specific database
            engine = create_engine(f'mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{db_name}')

            # Upload each DataFrame in the list to the database
            for i in tqdm(range(len(dfs_list)), desc='Uploading files to mysql'):
                table_name = dfs_names[i]
                dfs_list[i].to_sql(name=table_name, con=engine, index=False, if_exists='replace')
            print("All files uploaded")

        except SQLAlchemyError as e:
            print(f"Error: {e}")

    def create_engine(self, db_name):
        """
        Creates an SQLAlchemy engine connected to a specific MySQL database.

        Args:
            db_name (str): The name of the database to connect to.

        Returns:
            Engine: An SQLAlchemy engine instance connected to the specified database.
        """
        # Create and return an engine connected to the specified database
        return create_engine(f'mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{db_name}')