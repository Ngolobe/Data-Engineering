import pyodbc  # type: ignore
from datetime import datetime, date, time


class Table:
    """
    Abstracts a single SQL table and provides functionality to
    for example create the abstracted table to a database.
    """

    def __init__(
        self,
        table_name: str,
        schema_name: str,
        parameters: dict,
        temp: bool = False,
        randomize: bool = False,
    ):
        """
        Takes in necessary information and inits the abstracted SQL table object

        Args:
            table_name (str): Name of the table
            schema_name (str): Name of the schema
            parameters (dict): Parameters of the table in format:
                               [(parameter_name):(sql_datatype)]
            temp (bool, optional): If a temp table. Defaults to False.
            randomize (bool, optional): If one wants to add some random integers to a the tables name.
        """

        # TODO: smarted string sanitation
        self.name = table_name.replace("/", "_").replace(" ", "_")

        # if random noise wanted for the table name
        if randomize:
            random_noise = int(datetime.now().timestamp())
            self.name = f"{random_noise}_{self.name}"

        # if temp table, prepend "temp_" to the table name
        if temp:
            self.name = "temp_" + self.name

        self.schema = schema_name
        self.parameters = parameters
        self.temp = temp

    def create_to_db(self, connection, drop_existing=False):
        """
        Create this table to a database. BEWARE: if creating a temp table
        and a table with this name already exists, the old table is dropped.

        Args:
            connection ([type]): [description]
            drop_existing (bool, optional): If an existing table should be dropped. Defaults to False.
        """

        # build query
        creation_query = self.__build_create_table()

        # cursor
        cursor = connection.cursor()

        # execute the creation query
        try:
            cursor.execute(creation_query)
            cursor.commit()

        # catch pyodbc.ProgrammingErrors
        except pyodbc.ProgrammingError as e:
            # if table already exists in the database
            if "There is already an object named" in str(e):
                # if drop_existing flag is set, or creating a temp table: drop old table and recreate
                if drop_existing or self.temp:

                    # drop the temp table
                    drop_query = f"DROP TABLE [{self.schema}].[{self.name}]"
                    cursor.execute(drop_query)
                    connection.commit()

                    # execute the creation script again
                    cursor.execute(creation_query)
                    cursor.commit()

                # else, re raise error
                else:
                    raise e

            # if anything else, re raise the exception
            else:
                raise e

    def insert_data(self, data: list, params, connection, chunk_size=100):
        """
        Insert the data provided to the table in the database. Data
        needs to match the tables parameters and be SQL friendly.

        Args:
            data (list): The data in a list of dictionaires, matching the parameters
                         of the table
            connection ([type]): Connection to the database
            chunk_size (int, optional): Chunk size for executemany. Defaults to 100.
        """

        # insert query
        insert_query = self.__build_insert_to_table()

        # cursor with fast_executemany
        cursor = connection.cursor()
        cursor.fast_executemany = True

        # convert all data to sql friendly tuples
        converted = []
        for row in data:
            converted.append(self.__convert_row(row, params))

        del data

        # generate chunks of [chunk_size] from the data
        gen = self.__chunks(converted, chunk_size)

        # start iterating and inserting the chunks to the database
        for chunk in gen:
            # execute
            try:
                cursor.executemany(insert_query, chunk)
            # TODO: correct exception
            # if chunk insertion fails, split chunk to smaller peaces and try inserting them
            except:
                # TODO: validate that this works, especially on odd sizes
                # generate chunks 10 times smaller
                for smaller_chunk in self.__chunks(chunk, int(chunk_size / 10)):
                    # try inserting the smaller chunk
                    try:
                        cursor.executemany(insert_query, smaller_chunk)
                    # TODO: correct exception
                    # if still fails, execute 1 row at a time, with disabled ANSI_WARNING
                    except:
                        for row in smaller_chunk:
                            # disable ANSI_WARNINGS
                            # this causes too long data to be cut to the size of the column
                            # drops emails, html string etc, unnecessary data
                            insert_query_temp = f"SET ANSI_WARNINGS  OFF;\
                            {insert_query}\
                            SET ANSI_WARNINGS ON;"

                            cursor.execute(insert_query_temp, row)

        # if all inserts were performed successfully, commit the insertion
        # this makes sure that the insertion is performed only if all the data provided
        # is inserted
        connection.commit()

    def rename(self, connection, new_name, drop_existing=False):

        # cursor
        cursor = connection.cursor()

        # drop possible existing table if wanted
        if drop_existing:
            # query, drop if exists
            drop_query = f"DROP TABLE IF EXISTS [{self.schema}].[{new_name}]"
            # execute, commit
            cursor.execute(drop_query)
            connection.commit()

        # rename query
        rename_query = f"sp_rename '{self.schema}.{self.name}', '{new_name}';"

        cursor.execute(rename_query)
        cursor.commit()

    def __build_create_table(self) -> str:
        """
        Builds a create table query string based on this tables information

        Returns:
            str: The create table query as string
        """

        # start building string here, use tables schema and table names
        creation_query = f"CREATE TABLE [{self.schema}].[{self.name}] ("

        # loop through table parameters keys and data types
        for parameter, datatype in self.parameters.items():
            # addend the parameter and corresponding datatype
            creation_query += f"[{parameter}] {datatype}, "

        # remove the last ", "
        creation_query = creation_query[:-2]

        # close parenthesis and add semicolon
        creation_query += ");"

        # return the query str
        return creation_query

    def __build_insert_to_table(self) -> str:
        """
        Builds an "INSERT INTO [TABLE]" SQl query string based on the objects values

        Returns:
            str: The query in str
        """

        # build str here
        insert_query = f"INSERT INTO [{self.schema}].[{self.name}] ("

        # loop parameters and append to str
        for parameter in self.parameters:
            insert_query += f"[{parameter}], "

        # remove last ", "
        insert_query = insert_query[:-2]

        # append bracket and a space
        insert_query += ") "

        # build the 'VALUES' part
        insert_query += "VALUES ("
        # add as many '?' placeholders as there are keys
        for _ in range(len(self.parameters)):
            insert_query += "?, "

        # again remove last ", "
        insert_query = insert_query[:-2]

        # close parenthesis and add semicolon
        insert_query += ");"

        return insert_query

    def __chunks(self, lst: list, n: int):
        """
        Yields n sized chunks from the provided list
        Args:
            lst (List): The list to be chunked
            n (int): Size of the chunk

        Yields:
            List: Next list of size n from the list provided
        """
        for i in range(0, len(lst), n):
            yield lst[i: i + n]

    # TODO: make prettier
    def __str__(self) -> str:
        # build a visually descriptive representation of the parameters
        param_str = ""
        for parameter, datatype in self.parameters.items():
            param_str += f"[{parameter}]: {datatype} "
        # remove last space
        param_str = param_str[:-1]

        return f"[{self.schema}].[{self.name}] ({param_str})"

    # TODO: cleanup
    # converts a single row of data to a tuple of sql compatible data using initialized params
    def __convert_row(self, row, all_params):
        """
        :param row: row in a dictionary
        :param all_params: all parameters of an endpoint
        :return: returns converted row in a tuple, in a size of all_params
        """

        # build here
        values = ()
        # keys of row
        keys = list(row.keys())

        # loop all parameters
        for param in all_params:
            # if the row contains something for the parameter in question
            if param in keys:

                # convert to string to avoid errors in conversion later, since it might be too big for int
                if param == "number":
                    # add as string if anything
                    if row[param]:
                        values += (str(row[param]),)
                    else:
                        values += (None,)

                # lists and lists of dictionaries
                elif type(row[param]) is list:
                    # if nothing, append None
                    if not row[param]:
                        values += (None,)
                    # otherwise check if contains dicts with ids or just a list then append accordingly
                    else:
                        # if list of dicts then add possible ids from them
                        if type(row[param][0]) is dict:
                            # row has ids
                            if "guid" in row[param][0]:
                                # temporary list to gather ids in
                                id_list = []
                                for d in row[param]:
                                    # append ids to list
                                    id_list.append(d["guid"])
                                # append ids in a list
                                values += (f"{id_list}",)
                            # no ids, append as it is
                            else:
                                values += ("{}".format(row[param]),)
                        else:
                            values += ("{}".format(row[param]),)

                # wrap dict in square brackets
                elif type(row[param]) is dict:
                    if not row[param]:
                        values += (None,)
                    else:
                        # if nested dict has an id, then just return that
                        if "guid" in row[param].keys():
                            values += (row[param]["guid"],)
                        else:
                            values += (("[{}]".format(row[param])),)

                # convert booleans to sql bits
                elif type(row[param]) is bool:
                    if row[param]:
                        # True
                        values += (1,)
                    else:
                        # False
                        values += (0,)

                # convert datetimes, dates and times into python Datetime objects
                elif all_params[param] in ["Datetime", "Date", "Time"]:
                    values += (self.__convert_datetime(row[param], all_params[param]),)

                # else just add row as it is
                else:
                    values += (row[param],)

            # if the row in question does not have anything for the parameter
            # then add None
            else:
                values += (None,)

        # unexpected behavior
        if len(all_params) != len(values):
            raise Exception("Length of converted params did not match length of values")

        # return converted row in tuple
        return values

    # converts a datetime, date or time string to Python datetime object
    def __convert_datetime(self, element, form):
        """
        :param element: The element (string) to be converted
        :param form: The format of the element (Datetime, Date or Time)
        :return: A Datetime object
        """
        # if element is None return None
        if not element:
            return None

        if form == "Datetime":
            # if element contains timezone information etc, strip them
            if len(element) != 19:
                element = element[:19]
            # replace possible 'T' with a space
            element = element.replace("T", " ")
            return datetime.strptime(element, "%Y-%m-%d %H:%M:%S")
        elif form == "Time":
            # strip milliseconds, timezone
            if len(element) != 8:
                element = element[:8]
            return time.fromisoformat(element)
        elif form == "Date":
            return date.fromisoformat(element)
        else:
            raise Exception(f"Invalid values passed to __convert_datetime: {element, form}")
