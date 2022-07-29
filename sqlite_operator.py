import sqlite3


def sqlite_write(db_location, df_to_write, tablename):
    try:
        sqliteConnection = sqlite3.connect(db_location)
        # cursor = sqliteConnection.cursor()
        print("Successfully Connected to SQLite")

        # sqlite_insert_query = """INSERT INTO {}
        #                       (transaction_source, transaction_type, currency, rate, ts)
        #                        VALUES
        #                       (?,?,?,?,?)""".format(tablename)

        # count = cursor.execute(sqlite_insert_query)
        # sqliteConnection.commit()
        # print("Record inserted successfully into SqliteDb_developers table ", cursor.rowcount)
        # cursor.close()
        df_to_write.to_sql(tablename, sqliteConnection, if_exists='append', index=False)
    except sqlite3.Error as error:
        print("Failed to insert data into sqlite table", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("The SQLite connection is closed")
