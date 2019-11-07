import mysql.connector
import time

class mySQL_DB():
    ### Check table
    def list_table(self):
        mydb = mysql.connector.connect(
          host="phongntt.cfoco8xqahph.ap-southeast-1.rds.amazonaws.com",
          user="phongntt",
          passwd="phongntt",
          database="phongntt"
        )
        myCursor = mydb.cursor()
        myCursor.execute("SHOW TABLES")
        count = 0
        for tb in myCursor:
            if tb is None:
                pass
            else:
                print(tb)
                count+=1
        if count == 0:
            print("No table")
        mydb.commit()
        myCursor.close()
        mydb.close()

    ### Create table
    def create_table(self, table_name):
        mydb = mysql.connector.connect(
            host="phongntt.cfoco8xqahph.ap-southeast-1.rds.amazonaws.com",
            user="phongntt",
            passwd="phongntt",
            database="phongntt"
        )
        myCursor = mydb.cursor()
        myCursor.execute(
            "CREATE TABLE {0} (`id` INTEGER(10), `name` VARCHAR(255), `file` BLOB NOT NULL , PRIMARY KEY (`id`))".format(
                table_name))
        mydb.commit()
        myCursor.close()
        mydb.close()

    ### Delete table
    def delete_table(self, table_name):
        mydb = mysql.connector.connect(
          host="phongntt.cfoco8xqahph.ap-southeast-1.rds.amazonaws.com",
          user="phongntt",
          passwd="phongntt",
          database="phongntt"
        )
        myCursor = mydb.cursor()
        myCursor.execute("DROP TABLE {0}".format(table_name))
        mydb.commit()
        myCursor.close()
        mydb.close()

    ### Insert table
    def insert_data(self, table_name, directory):
        from os import listdir
        from os.path import isfile, join

        mydb = mysql.connector.connect(
          host="phongntt.cfoco8xqahph.ap-southeast-1.rds.amazonaws.com",
          user="phongntt",
          passwd="phongntt",
          database="phongntt"
        )
        myCursor = mydb.cursor()

        files = [f for f in listdir(directory) if isfile(join(directory, f))]
        no_files = len([f for f in listdir(directory) if isfile(join(directory, f))])

        for idx in range(no_files):
            print(idx)
            start_time = time.time()
            with open(join(directory, files[idx]), 'rb') as file:
                binaryData = file.read()

            myCursor.execute("INSERT INTO {0} (id, name, file) VALUES(%s,%s,%s)".format(table_name), (idx, files[idx], binaryData))
            print("--- %s seconds ---" % (time.time() - start_time))

        mydb.commit()
        myCursor.close()
        mydb.close()

    ## create a table and upload data to this table
    def upload_data(self, table_name, directory):
        try:
            self.create_table(table_name)
        except:
            pass
        self.insert_data(table_name, directory)


    ### Retrieve values from table
    def get_no_rows(self, table_name):
        mydb = mysql.connector.connect(
          host="phongntt.cfoco8xqahph.ap-southeast-1.rds.amazonaws.com",
          user="phongntt",
          passwd="phongntt",
          database="phongntt",
          #use_pure=True
        )
        myCursor = mydb.cursor()
        myCursor.execute("SELECT COUNT(*) FROM {0}".format(table_name))
        no_rows = tuple
        for i in myCursor:
            no_rows = i
        mydb.commit()
        myCursor.close()
        mydb.close()
        return no_rows[0]

    def download_data(self, table_name, directory):
        from os.path import join
        import os

        def write_file(data, filename):
            if "xml" not in filename:
                with open(filename, 'wb') as file:
                    file.write(data)
            else:
                with open(filename, 'w', encoding="utf-8") as file:
                    file.write(data)

        mydb = mysql.connector.connect(host="phongntt.cfoco8xqahph.ap-southeast-1.rds.amazonaws.com",
          user="phongntt",
          passwd="phongntt",
          database="phongntt",
          use_pure=True)

        myCursor = mydb.cursor()

        no_rows = self.get_no_rows(table_name)
        for idx in range(no_rows):
            print(idx)
            start_time = time.time()

            myCursor.execute("SELECT name, file from {0} where id = %s".format(table_name), (idx,))
            record = myCursor.fetchall()
            file = record[0][1]
            name = record[0][0]
            try:
                write_file(file, join(directory,name))
            except:
                os.mkdir(directory)
                write_file(file, join(directory,name))

            print("--- %s seconds ---" % (time.time() - start_time))

        mydb.commit()
        myCursor.close()
        mydb.close()

    def rename_table(self, oldname, newname):
        mydb = mysql.connector.connect(
          host="phongntt.cfoco8xqahph.ap-southeast-1.rds.amazonaws.com",
          user="phongntt",
          passwd="phongntt",
          database="phongntt",
          #use_pure=True
        )
        myCursor = mydb.cursor()
        myCursor.execute("RENAME TABLE  `{0}` TO  `{1}`".format(oldname, newname))
        mydb.commit()
        myCursor.close()
        mydb.close()
    def copy_table(self, original, copy):

        mydb = mysql.connector.connect(
		host="phongntt.cfoco8xqahph.ap-southeast-1.rds.amazonaws.com",
		user="phongntt",
		passwd="phongntt",
		database="phongntt",
		#use_pure=True
		)
        myCursor = mydb.cursor()
        myCursor.execute("CREATE TABLE `{0}` LIKE `{1}`".format(copy, original))
        myCursor.execute("INSERT `{0}` SELECT * FROM `{1}`".format(copy, original))
        mydb.commit()
        myCursor.close()
        mydb.close()
if __name__ == '__main__':
    import time
    import datetime

    today = datetime.datetime.now().strftime("%Y-%m-%d")
    images_table = "images-{0}".format(today)
    annots_table = "annots-{0}".format(today)

    sql = mySQL_DB()

    ## Rename the table in the database
    #sql.rename_table("annots-2019-11-04", "annots-2019-11-05")
    #sql.rename_table("images-2019-11-04", "images-2019-11-05")
    
    ## List all the tables in the database
    sql.list_table()
    ## Get the number of file in dataset
    print("There are {0} images in the dataset".format(sql.get_no_rows("`images-2019-11-04`") + sql.get_no_rows("`images-2019-11-07`")))
    print("Downloading...")
    ## Download the table by its name (Note: table name in mysql is always between ``)
    sql.download_data('`images-2019-11-04`', 'dataset')
    sql.download_data('`annots-2019-11-04`', 'dataset')
    sql.download_data('`images-2019-11-07`', 'dataset')
    sql.download_data('`annots-2019-11-07`', 'dataset')

    ## Copy table
    #sql.copy_table("images-2019-11-07", "images-2019-11-07-copy")

    ## Delete table
    #sql.delete_table("`images-2019-11-07-copy`")

    ## Upload data
    #sql.upload_data('`{0}`'.format(images_table), 'JPEGImages')
    #sql.upload_data('`{0}`'.format(annots_table), 'Annotations')

    