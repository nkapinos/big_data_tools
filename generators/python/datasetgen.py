
#!/usr/bin/env python

#======================================================================
#   Dataset Generator v.3
#   by Nate Kapinos
#
#
# TODO: Allow passing in of parameters, theme objects
# TODO: Integrate into UI
# 
# 7/1/2014:  Initial version
# 7/1/2015:  Added Equifax dataset
# 2/25/2016:  Added NULL row injection
# 4/22/2016:  Cleanup for git checkin
#
#======================================================================

import sys
import os
import random


script_dir = os.path.dirname(os.path.abspath(__file__))
fact_themes = { "Apple Product Purchases": {
                    "factname": "apple_purchases",
                    "fact_cols": {"products": {"type": "rand_sample",
                                               "contents": ["iPhone", "Macbook", "Apple TV", "Airport"]},
                                  "sale_id": {"type": "inc_int",
                                              "start": 1,
                                              "stop": "ROW_SIZE"},
                                   "cust_id": {"type": "rand_int",
                                               "start": 1,
                                               "stop": 10000000},
                                  "date": {"type": "date",
                                           "start_yr": 2000,
                                           "stop_yr": 2013},
                                  "quantity": {"type": "rand_int",
                                               "start": 0,
                                               "stop": 4}},
                    "dimname": "apple_customers",
                    "dim_cols": {"id": {"type": "inc_int",
                                        "start": 1,
                                        "stop": 10000},
                                 "age": {"type": "rand_int",
                                         "start": 10,
                                         "stop": 80},
                                 "location": {"type": "rand_sample",
                                              "contents": ["San Francisco", "Belmont", "Natick", "Clearwater",
                                                           "Westborough", "N. Dartmouth", "New Bedford"]},
                                 "income": {"type": "rand_int",
                                            "start": 20000,
                                            "stop": 250000},
                                 "owns_cat": {"type": "boolean"}}},

                "Product Reviews": {
                                    "factname": "product_purchases",
                                    "fact_cols": {"products": {"type": "rand_sample",
                                                               "contents": ["iPhone", "Macbook", "Apple TV", "Airport"]},
                                                  "sale_id": {"type": "inc_int",
                                                              "start": 1,
                                                              "stop": "ROW_SIZE"},
                                                   "cust_id": {"type": "rand_int",
                                                               "start": 1,
                                                               "stop": 1000},
                                                  "date": {"type": "date",
                                                           "start_yr": 2010,
                                                           "stop_yr": 2015},
                                                  "rating": {"type": "rand_int",
                                                               "start": 1,
                                                               "stop": 5}},
                                    "dimname": "product_customers",
                                    "dim_cols": {"id": {"type": "inc_int",
                                                        "start": 1,
                                                        "stop": 1000},
                                                 "age": {"type": "rand_int",
                                                         "start": 10,
                                                         "stop": 80},
                                                 "location": {"type": "rand_sample",
                                                              "contents": ["San Francisco", "Belmont", "Natick", "Clearwater",
                                                                           "Westborough", "N. Dartmouth", "New Bedford"]},
                                                 "income": {"type": "rand_int",
                                                            "start": 20000,
                                                            "stop": 250000},
                                                 "owns_cat": {"type": "boolean"}}},
                "Credit Sample": {
                    "factname": "credit_fact",
                    "fact_cols": {"cms_seq_id": {"type": "inc_int",
                                              "start": 1,
                                              "stop": "ROW_SIZE"},
                                  "cust_id": {"type": "rand_int",
                                               "start": 1,
                                               "stop": 1000000},
                                  "year": {"type": "rand_int",
                                           "start": 2010,
                                           "stop": 2015},
                                  "zip": {"type": "rand_int",
                                         "start": 1,
                                         "stop": 999},
                                  "creditscore": {"type": "rand_int",
                                               "start": 200,
                                               "stop": 800},
                                  "debt": {"type": "rand_int",
                                            "start": 1,
                                            "stop": 10000},
                                 "bankruptcy": {"type": "boolean"}},
                    "dimname": "credit_dimension",
                    "dim_cols": {"id": {"type": "inc_int",
                                        "start": 1,
                                        "stop": 1000000},
                                 "firstname": {"type": "rand_sample",
                                               "contents": ["Nate", "Andrew", "Jeff", "Marty", "Tony", "Marg", "Mario"]},
                                 "lastname": {"type": "rand_sample",
                                              "contents": ["Kapinos", "Smith", "Kelly", "Jackman", "Stark", "Cricket"]},
                                 "age": {"type": "rand_int",
                                         "start": 10,
                                         "stop": 80},
                                 "city": {"type": "rand_sample",
                                              "contents": ["San Francisco", "Belmont", "Natick", "Clearwater",
                                                           "Westborough", "N. Dartmouth", "New Bedford"]},
                                 "gender": {"type": "rand_sample",
                                            "contents": ["Male", "Female"]}
                                            }},
                "Books by Author": {
                    "factname": "books_author",
                    "fact_cols": {"book_id": {"type": "inc_int",
                                              "start": 1,
                                              "stop": "ROW_SIZE"},
                                  "author_id": {"type": "rand_int",
                                                "start": 1,
                                                "stop": 1000},
                                  "genre": {"type": "rand_sample",
                                            "contents": ["Thrillah", "Sci-Fi", "Horrah", "Mystery", "Self-Help"]},
                                  "pub_date": {"type": "date",
                                               "start_yr": 1970,
                                               "stop_yr": 2013},
                                  "price": {"type": "rand_int",
                                            "start": 5,
                                            "stop": 50},
                                  "rating": {"type": "rand_int",
                                             "start": 1,
                                             "stop": 5}},
                    "dimname": "authors",
                    "dim_cols": {"id": {"type": "inc_int",
                                        "start": 1,
                                        "stop": 1000},
                                 "age": {"type": "rand_int",
                                         "start": 20,
                                         "stop": 80},
                                 "school": {"type": "rand_sample",
                                            "contents": ["UMass", "UVM", "USF", "U Wisconsin", "Berkeley", "Stanford",
                                                         "UCSB"]}}}}


supported_types = ["XML", "CSV", 'JSON', 'HIVE']
supported_answer = ["Y", "N"]
option_list = {"data_type": "",
               "references": "",
               "partitioned": "",
               "part_count": "",
               "dataset_size": "",
               "output_dir": "",
               "theme": "",
               "percent_null": 0}


def hive_writer(theme, theme_path, tbl_name):
    """
    Incomplete...need to update / clean up.  Copy / Paste from separate script
    @param rows: 
    @param theme:
    @param theme_path:
    @return:
    """

    files_per_year = 5
    count = 1
    years = ['2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010']
    hiveql_drop = "drop table if exists %s;" % tbl_name


    hiveql_create = ("CREATE TABLE IF NOT EXISTS user_dist (user_id INT, ride_date STRING, dist INT)"
                    " PARTITIONED BY (yr STRING)"
                    " ROW FORMAT DELIMITED"
                    " FIELDS TERMINATED BY \"\\t\";")
    hiveql_load = ("LOAD DATA LOCAL INPATH \"/home/platfora/sample_data/csv_partition/{0}\" INTO TABLE user_dist"
                   " PARTITION (yr=\'{1}\');")

    print("\n\nDROPPING TABLE user_dist...")
    os.system("hive -e '%s' > /dev/null" % hiveql_drop)    # drop user_data if exists
    print("\nCREATING TABLE user_dist...")
    os.system("hive -e '%s'> /dev/null" % hiveql_create)  # create user_data

    while files_per_year > 0:
        for year in years:
            x = 1
            fname = (str(year) + "_rides_part%d.csv" %files_per_year)
            f = open(fname, 'w')
            while x < rows:
                f.write('%d\t' % random.randint(1,100))   # user_id
                f.write('%s-%d-%d\t' %(year, random.randint(1,12), random.randint(1,31)))  # ride_date
                f.write('%d\n' % random.randint(5,100))     # dist
                x += 1
            f.close()

            cmd = ""
            cmd = hiveql_load.format(fname, year)
            print("\n\nYEAR:  " + year)
            print("Y: " + str(files_per_year))
            print("HIVE QUERY:  " + cmd)
            print("PERCENT COMPLETE: %.2f" % ((float(count) / (files_per_year*len(years))) * 100))
            count += 1
        os.system("hive -e '%s' > /dev/null" % cmd)
        files_per_year -= 1


def file_writer(rows, theme, theme_path, filename, total_rows, data_type, tbl_name, percent_null):
    """

    @param rows: number of rows to create in this file
    @param theme: theme of the dataset
    @param filename: name of the file to create
    @return: total_rows to keep count of incrementing value if needed (for unique values)
    """
    datafile = open(filename, 'w')

    # write header row
    header_list = []
    if data_type == "CSV":
        for field in fact_themes[theme][theme_path]:
            header_list.append(field)
        file_line = ",".join([str(x) for x in header_list])
        datafile.write(file_line)
        datafile.write("\n")

    # write rows based on type in theme list
    while rows > 0:
        # generate random values to populate csv file here
        # create list of items to write to file
        field_list = []
        for field in fact_themes[theme][theme_path]:

            if fact_themes[theme][theme_path][field]["type"] == "rand_int":
                start = fact_themes[theme][theme_path][field]["start"]
                stop = fact_themes[theme][theme_path][field]["stop"]
                field_list.append(random.randint(start, stop))

            if fact_themes[theme][theme_path][field]["type"] == "rand_sample":
                contents = fact_themes[theme][theme_path][field]["contents"]
                field_list.append("".join(random.sample(contents, 1)))

            if fact_themes[theme][theme_path][field]["type"] == "inc_int":
                field_list.append(total_rows)

            if fact_themes[theme][theme_path][field]["type"] == "date":
                year = random.randint((fact_themes[theme][theme_path][field]["start_yr"]),
                                      (fact_themes[theme][theme_path][field]["stop_yr"]))
                day = random.randint(1, 28)
                month = random.randint(1, 12)
                field_list.append("%d-%d-%d" % (month, day, year))

            if fact_themes[theme][theme_path][field]["type"] == "boolean":
                field_list.append("".join(random.sample(["True", "False"], 1)))

        if data_type == "CSV" or data_type == "HIVE":
            
            # insert NULL values into 'percent_null' percent rows
            coin_toss = random.uniform(0,100) 
            if coin_toss < int(percent_null):
              #num_fields = len(field_list)
              #field_to_null = random.randint(0, (num_fields-1))
              field_list[2] = ""  # replace just one random field with null value

            file_line = ",".join([str(x) for x in field_list])
            datafile.write(file_line)
            datafile.write("\n")



        """
        if data_type == "XML":
            file_line = "<%s " % tbl_name
            for item in fact_themes[theme][theme_path]:

            file_line += ""
            file_line += "</%s>" % tbl_name
            pass
        """

        total_rows -= 1
        rows -= 1

    datafile.close()

    if data_type == "HIVE":
            hive_writer(theme, theme_path, tbl_name)
    return total_rows


def dsg_engine(option_list):
    """

    @param option_list: dictionary containing user-specified options to generate dataset
    @return: 0 for success
    """
    part_count = option_list["part_count"]
    rows_per_file = int(option_list["dataset_size"]/part_count)
    total_rows = int(option_list["dataset_size"])
    data_type = option_list["data_type"]
    tbl_name = ""

    # create fact table, this is the main loop to create files, csvwriter() creates rows
    while part_count > 0:
        filename = (fact_themes[option_list["theme"]]["factname"] + "_part%d.%s" % (part_count, data_type.lower()))
        filename = option_list["output_dir"] + "/" + filename
        tbl_name = fact_themes[option_list["theme"]]["factname"]
        print "Writing Fact Table Files.............. %s" % filename
        total_rows = file_writer(rows_per_file, option_list["theme"], "fact_cols", filename, total_rows, data_type, tbl_name, option_list["percent_null"])
        part_count -= 1

    # references / dimension table specified ?
    if option_list["references"] == "Y":
        filename = option_list["output_dir"] + "/" + fact_themes[option_list["theme"]]["dimname"] + ".%s"\
                   % data_type.lower()
        tbl_name = fact_themes[option_list["theme"]]["dimname"]
        print "Writing Dimension Table Files......... %s" % filename
        dim_rows = fact_themes[option_list["theme"]]["dim_cols"]["id"]["stop"]
        file_writer(dim_rows, option_list["theme"], "dim_cols", filename, dim_rows, data_type, tbl_name, option_list["percent_null"])


if __name__ == "__main__":
    try:
        print "\n\n****************************************************"
        print "Dataset Generator"
        print "Version: .2 - 7/25/14"
        print "Author: Nate Kapinos"
        print "****************************************************\n\n"

        data_type = raw_input("Dataset to generate (CSV, XML, JSON, HIVE) - (CSV): ").upper()

        if data_type == "":
            data_type = "CSV"

        if data_type not in supported_types:
            print "Selected data type is not supported.  Aborting..."
            sys.exit(-1)

        references = raw_input("Create References? (Y/N) - (Y): ").upper()

        if references == "":
            references = "Y"

        if references not in supported_answer:
            print "Not a valid answer.  Aborting..."
            sys.exit(-1)

        partitioned = raw_input("Create file partitioned Data? (Y/N) - (Y): ").upper()

        if partitioned == "":
            partitioned = "Y"

        if partitioned not in supported_answer:
            print "Not a valid answer.  Aborting..."
            sys.exit(-1)

        if partitioned == "Y" and data_type != "HIVE":
            part_count = raw_input("Number of partitions (files): ")
            if part_count == "":
                part_count = 10
            else:
                try:
                    part_count = int(part_count)
                except ValueError:
                    print "Not a valid size.  Aborting..."
                    sys.exit(-1)
        else:
            part_count = 1

        dataset_size = raw_input("Enter the size of the dataset(fact) in rows (1000): ")

        if dataset_size == "":
            dataset_size = 1000
        else:
            try:
                dataset_size = int(dataset_size)
                total_rows = dataset_size
            except ValueError:
                print "Not a valid size.  Aborting..."
                sys.exit(-1)

        output_dir = raw_input("Enter the output directory to save the dataset (%s): " % script_dir)

        if output_dir == "":
            output_dir = script_dir

        if not output_dir.startswith('/'):
            output_dir = script_dir + '/' + output_dir

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        print "Choose theme of the dataset:"
        for item in fact_themes:
            print "   %s" % item
        ds_theme = raw_input("Type name of theme: ")

        if ds_theme not in fact_themes:
            print "Not a supported theme.  Aborting..."
            sys.exit(-1)

        percent_null = raw_input("Enter the percent of NULL values to insert in dataset: ")


        print "\n****************************************************"
        print "Generating dataset with parameters:"
        print "Type: %s" % data_type
        print "References: %s" % references
        print "File Partitioned: %s" % partitioned
        if partitioned == 'Y':
            print "...File partitions: %d" % part_count
        print "Size in rows: %d" % dataset_size
        print "Output directory: %s" % output_dir
        print "Dataset Theme: %s" % ds_theme
        print "Percent NULL Values %s" % percent_null
        print "****************************************************\n"

        option_list["data_type"] = data_type
        option_list["references"] = references
        option_list["partitioned"] = partitioned
        option_list["part_count"] = part_count
        option_list["dataset_size"] = dataset_size
        option_list["output_dir"] = output_dir
        option_list["theme"] = ds_theme
        option_list["percent_null"] = percent_null

        continue_gen = raw_input("Continue (Y/N): ").upper()
        if continue_gen == "Y":
            dsg_engine(option_list)
        else:
            sys.exit(-1)

    except KeyboardInterrupt:
        print "User terminated command. Bye."

    except Exception as e:
        print(e)
