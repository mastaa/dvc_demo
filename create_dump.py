import psycopg2


def export_table_db(host: str, port: str, db_name: str, user: str, password: str, schema: str, table: str,
                    separator: str, file_name: str):
    # set up database connection.
    conn = psycopg2.connect(host=host, port=port, dbname=db_name, user=user, password=password)
    db_cursor = conn.cursor()

    query = f"select * from {schema}.{table}".format(schema, table)

    # Use the COPY function on the SQL we created above.
    sql_for_file_output = f"COPY ({query}) TO STDOUT WITH delimiter '" + separator + "' CSV".format(query)
    print(sql_for_file_output)
    # Set up a variable to store our file path and name.
    t_path_n_file = f"./data/{file_name}".format(file_name)
    with open(t_path_n_file, 'w') as f_output:
        db_cursor.copy_expert(sql_for_file_output, f_output)
    db_cursor.close()
    conn.close()


if __name__ == "__main__":
    export_table_db('localhost', '5432', 'postgres', 'postgres', 'postgres', 'dvc_db', 'price', '\u007F', 'dump.csv')

