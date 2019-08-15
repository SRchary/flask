import os
from datetime import datetime
import pandas as pd
from pandas import ExcelWriter, Index
import shutil

import conf
import formatter
import definitions
collection_name ="RAVI"
from bson.objectid import ObjectId


def get_line1A_data(mongo, start_year, end_year):
    """
    This function will fetch the summary info of Pole Replacements from database between the start year and end year.
    :param mongo: For mongo db connection object
    :param start_year: as per the user request.Type datetime
    :param end_year: as per the user request Type datetime
    :return: Dict which contains the line_no, description, recorded_units
    """
    data = mongo.db.poles_data.aggregate([
        {"$match": { 'IDYEAR': {'$gte': start_year, '$lte': end_year}, 'MWC': {"$in": definitions.MWC_SELECTED_CODES}}},
        {"$group": {"_id": "$MWC", 'count': {"$sum": 1}}},
        {"$group": {"_id": None, 'total': {"$sum": "$count"}}}
    ])
    data = list(data)
    total = data[0]["total"] if data else 0
    result = dict()
    result["line_no"] = "1A"
    result["description"] = "Wood Poles replaced through Pole Replacement and other Company programs"
    result["recorded_units"] = "{:,}".format(total)
    return result


def get_line1A_dataset(mongo, start_year, end_year):
    """
    This function will fetch the Pole Replacements data from database between the start year and end year.
    :param mongo: For mongo db connection object
    :param start_year: Data start year
    :param end_year: Data end year
    :return: returns the list of dataframes this list contains mat data and mwc data as a dataframes
    """
    results = mongo.db.poles_data.aggregate([
        {"$match": {'IDYEAR': {'$gte': start_year, '$lte': end_year}, 'MWC': {"$in": definitions.MWC_SELECTED_CODES}}},
        {"$group" : {"_id": "$MWC", 'count':{"$sum": 1}}}
    ])
    mwc_df = pd.DataFrame(list(results))
    mwc_df.rename(columns={"_id": 'MWC', "count": 'Count of Poles'}, inplace=True)
    mwc_df["MWC"].fillna("", inplace=True)
    mwc_df["MWC"] = mwc_df["MWC"].apply(str)
    mwc_df.sort_values(["MWC"], inplace=True)
    mwc_df = mwc_df.reset_index(drop=True)
    loc = Index(mwc_df['MWC']).get_loc("NULL")
    null_df = mwc_df.loc[[loc]]
    mwc_df.drop(mwc_df.index[loc], inplace=True)
    mwc_df = mwc_df.append(null_df)
    results = mongo.db.poles_data.aggregate([
        {"$match": {'IDYEAR': {'$gte': start_year, '$lte': end_year}}},
        {"$group": {"_id": "$MAT", 'count': {"$sum": 1}}}
    ])
    mat_df = pd.DataFrame(list(results))
    mat_df.rename(columns={"_id": 'MAT', "count": 'Count of Poles'}, inplace=True)
    data = dict()
    mat_df.fillna("NULL", inplace=True)
    mat_df = mat_df.sort_values(["MAT"])
    data["mat_df"] = mat_df
    data["mwc_df"] = mwc_df
    return data


def get_line1B_data(mongo, year):
    """
    This function will fetch the summary info of Pole age histogram data from database.
    The lines in comments based on the years user selection if the business wants to see this records based on year just
     uncomment the line
    :param mongo: For mongo db connection object
    :return: Dict which contains the line_no, description, recorded_units
    """
    pipeline = [
        # {"$match":{"$or": [{'IYEAR': {"$lte": year}},{"IYEAR": np.nan},]}},
        {'$group': {'_id': None, 'total': {'$sum': '$COUNT'}}}
    ]
    data = mongo.db.support_structure_data.aggregate(pipeline=pipeline)
    data = list(data)
    total = data[0]["total"] if data else 0
    result = dict()
    result["line_no"] = "1B"
    result["description"] = "Wood Pole age histogram and table"
    result["recorded_units"] = "{:,}".format(int(total))
    return result


def get_pole_count_by_age(pole_count_by_age, start_year=1924):
    """
    This function will generate dataframe for pole count by age.
    :param pole_count_by_age: Pole count data
    :param start_year: The year range starting from current year(2018) to given start year(1925).
    :return: Pole count by age dataframe.
    """
    unavailable_count = 0
    current_year = datetime.now().year
    pole_count_by_age_result = dict()
    for record in pole_count_by_age:
        try:
            # Pole Year
            year = int(record["INSTALLATIONYEAR"])
            # Verifying Year Range with Start Year And Current Year
            if (year < start_year) or (year > current_year):
                # If Pole year not in range, then considering unavailable count
                unavailable_count = unavailable_count + record.get("COUNT", 0.0)
            else:
                # If Pole year in range, then Caliculating Range Starting Value
                range_start_value = (current_year - year) - (current_year - year) % 5
                # Formatting Range Value using range start value.
                range_value = "{}-{}".format(range_start_value + 1, range_start_value + 5)
                # Adding Pole count to existing record or default value(0)
                pole_count_by_age_result[range_value] = pole_count_by_age_result.get(range_value, 0.0) + record.get(
                    "COUNT", 0.0)
        except:
            # If Any exception raised, then considering unavailable pole years
            unavailable_count = unavailable_count + record.get("COUNT", 0.0)

    # Converting Dict Data to List of Dicts with Dataframe Columns as keys.
    pole_count = list()
    for year_range, count in pole_count_by_age_result.items():
        pole_count.append({"AGE(YEARS)": year_range, "NUMBER OF POLES": count})
    pole_count = pole_count[::-1]
    pole_count.append({"AGE(YEARS)": "unavailable", "NUMBER OF POLES": unavailable_count})
    # Creating Pole Age Range Data frame.
    return pd.DataFrame(pole_count)


def get_line1B_dataset(mongo, year):
    """
    This function will generate support_structure, Pole_count_by_age dataframes.
    The lines in comments based on the years user selection if the business wants to see this records based on year just
     uncomment the line
    :param mongo: MongoDB connection object
    :param year:
    :return: List of dataframes
    """
    result = dict()
    support_results = mongo.db.support_structure_data.find(
        # {"IYEAR":{'$lte': year}},
        {},
        {'_id': 0}
    )
    support_structure_df = pd.DataFrame(list(support_results))
    support_structure_df = support_structure_df[['INSTALLATIONYEAR', 'COUNT']]
    support_structure_df["INSTALLATIONYEAR"] = support_structure_df["INSTALLATIONYEAR"].apply(
        lambda x: 'Null' if pd.isnull(x) else x)
    result['support_structure_df'] = support_structure_df

    result['pole_count_by_age_df'] = get_pole_count_by_age(support_structure_df.to_dict(orient='records'))
    return result


def get_line1C_data(mongo, end_year):
    """
    This function will fetch the summary info of Pole Stat Deprication count from database.
    The lines in comments based on the years user selection if the business wants to see this records based on year just
     uncomment the line
    :param mongo: For mongo db connection object
    :return: Dict which contains the line_no, description, recorded_units
    """
    result = dict()
    pipeline = [
        #{'$match': {"YEAR": {'$lte': end_year}}},
        {'$group': {'_id': None, 'total': {'$sum': '$COUNT'}}}
    ]
    data = mongo.db.pole_height_data.aggregate(pipeline=pipeline)
    data = list(data)
    total = data[0]["total"] if data else 0

    result["line_no"] = "1C"
    result["description"] = "Wood Pole stats-depreciation study"
    result["recorded_units"] = "{:,}".format(int(total))
    return result


def get_pole_height_dataframe(mongo, end_year, start_height=20, end_height=100, diff=5):
    """
    This function will generate the pole height data based on given ranges(start_height, end_height)
    other values will consider as unknown count.
    The lines in comments based on the years user selection if the business wants to see this records based on year just
     uncomment the line
    :param mongo: Mongo database connection object.
    :param start_height: start value for range
    :param end_height: end value for range.
    :param diff: range difference
    :return: pole height dataframe.
    """
    pole_height_data = mongo.db.pole_height_data.find({}, {"_id": 0})

    pole_height_df = pd.DataFrame(list(pole_height_data))

    pipeline = [
        #{'$match': {"YEAR": {'$lte': end_year}}},
        {'$group': {'_id': "$HEIGHT", 'COUNT': {'$sum': '$COUNT'}}}
    ]
    pole_height_data = mongo.db.pole_height_data.aggregate(pipeline=pipeline)
    pole_height_df = pd.DataFrame(list(pole_height_data))
    pole_height_df.rename(columns={"_id": "HEIGHT"}, inplace=True)
    data = pole_height_df.to_dict(orient='records')
    over_100 = 0
    below_20 = 0
    unknown_count = 0
    pole_count_by_height_result = dict()
    pole_count_by_height_result[20] = 0
    for record in data:
        try:
            height = int(record["HEIGHT"])
            if height <= start_height:
                below_20 = below_20 + float(record.get("COUNT", 0.0))
            elif height > end_height:
                over_100 = over_100 + float(record.get("COUNT", 0.0))
            else:
                reminder = (height % diff)
                reminder = diff - reminder if reminder else 0
                height = height + reminder
                pole_count_by_height_result[height] = pole_count_by_height_result.get(height, 0.0) + float(
                    record.get("COUNT", 0.0))
        except:
            # If Any exception raised, then considering unavailable pole years
            unknown_count = unknown_count + float(record.get("COUNT", 0.0))
    pole_count_by_height_result[20] = below_20
    pole_height_count = list()
    keys = sorted(pole_count_by_height_result)
    for height in keys:
        pole_height_count.append({
            "HEIGHT": str(height),
            "COUNT": pole_count_by_height_result[height]}
        )

    pole_height_count.append({
        "HEIGHT": "Over 100",
        "COUNT": over_100}
    )
    pole_height_count.append({
        "HEIGHT": "Unavailable",
        "COUNT": unknown_count}
    )
    return pd.DataFrame(pole_height_count)


def format_pole_stats_dataframe(data, attribute, unknown_names, order=False):
    """
    This function will format the pole(stats, height, treatment) information.
    :param data: pole stats dataframe
    :param attribute: name of the operation(SPECIES, HEIGHT, TREATMENT, CLASS)
    :param unknown_names: List of the unknown keys.
    :param order: Reverse order default False.
    :return: Dataframe object.
    """
    unknown_count = 0
    results = list()
    for record in data:
        try:
            name = record.get(attribute)
            name = name.lower() if name else name
            if name in unknown_names:
                unknown_count = unknown_count + float(record["COUNT"])
            else:
                results.append(record)
        except:
            results.append(record)

    if order:
        results = results[::-1]
    results.append({
        attribute: "Unavailable",
        "COUNT": unknown_count
    })
    return pd.DataFrame(results)


def format_pole_class_dataframe(data, attribute, unknown_names, order=False):
    """
    This function will format the pole(stats, height, treatment) information.
    :param data: pole stats dataframe
    :param attribute: name of the operation(SPECIES, HEIGHT, TREATMENT, CLASS)
    :param unknown_names: List of the unknown keys.
    :param order: Reverse order default False.
    :return: Dataframe object.
    """
    unknown_count = 0
    results = list()
    num_set = list()
    others_set = list()

    for record in data:
        try:
            name = record.get(attribute)
            name = name.lower() if name else name
            if name in unknown_names:
                unknown_count = unknown_count + float(record["COUNT"])
            elif name.isnumeric():
                num_set.append(record)
            else:
                others_set.append(record)
        except:
            others_set.append(record)

    num_set = sorted(num_set, key=lambda k: k[attribute])
    others_set = sorted(others_set, key=lambda k: k[attribute], reverse=True)

    results.extend(others_set)
    results.extend(num_set)
    results.append({
        attribute: "Unavailable",
        "COUNT": unknown_count
    })
    return pd.DataFrame(results)


def get_line1C_dataset(mongo, end_year):
    """
    This functiion will generate the dataframes for polestats(CLASS, SPECIES, HEIGHT, TREATMENT).
    The lines in comments based on the years user selection if the business wants to see this records based on year just
     uncomment the line
    :param mongo: Mongo DB Object
    :return: Return the Map of dataframes.
    """
    result = dict()
    pipeline = [
        #{'$match': {"YEAR": {'$lte': end_year}}},
        {'$group': {'_id': "$CLASS", 'COUNT': {'$sum': '$COUNT'}}}
    ]
    class_results = mongo.db.pole_class_data.aggregate(pipeline=pipeline)
    pole_class_df = pd.DataFrame(list(class_results))
    pole_class_df.rename(columns={"_id": "CLASS"}, inplace=True)
    pole_class_df["CLASS"] = pole_class_df["CLASS"].apply(lambda x: x if x else "")
    pole_class_df = pole_class_df.sort_values(['CLASS'])
    pole_class_df = format_pole_class_dataframe(pole_class_df.to_dict(orient='records'), "CLASS",
                                                ["0", "00", "ot", "null", ''], True)
    result['pole_class_df'] = pole_class_df[['CLASS', 'COUNT']]

    pipeline = [
        #{'$match': {"YEAR": {'$lte': end_year}}},
        {'$group': {'_id': "$SPECIES", 'COUNT': {'$sum': '$COUNT'}}}
    ]
    species_results = mongo.db.pole_species_data.aggregate(pipeline=pipeline)
    pole_species_df = pd.DataFrame(list(species_results))
    pole_species_df.rename(columns={"_id": "SPECIES"}, inplace=True)
    pole_species_df["SPECIES"] = pole_species_df["SPECIES"].apply(lambda x: x if x else "")
    pole_species_df = pole_species_df.sort_values(["SPECIES"])
    pole_species_df = format_pole_stats_dataframe(pole_species_df.to_dict(orient='records'), "SPECIES",
                                                  ["not available", "n/a", "null", "other", ''])
    result['pole_species_df'] = pole_species_df[['SPECIES', 'COUNT']]


    pipeline = [
        #{'$match': {"YEAR": {'$lte': end_year}}},
        {'$group': {'_id': "$TREATMENT", 'COUNT': {'$sum': '$COUNT'}}}
    ]
    treatment_results = mongo.db.pole_treatment_data.aggregate(pipeline=pipeline)
    pole_treatment_df = pd.DataFrame(list(treatment_results))
    pole_treatment_df.rename(columns={"_id": "TREATMENT"}, inplace=True)
    pole_treatment_df["TREATMENT"] = pole_treatment_df["TREATMENT"].apply(lambda x: x if x else "")
    pole_treatment_df['TREATMENT'].replace({"Untreated": "No Treatment"}, inplace=True)
    pole_treatment_df = pole_treatment_df.groupby('TREATMENT').sum().reset_index()
    pole_treatment_df = pole_treatment_df.sort_values(["TREATMENT"])
    pole_treatment_df = format_pole_stats_dataframe(pole_treatment_df.to_dict(orient='records'), "TREATMENT",
                                                    ["unknown", "n/a", "null", "other", ''])
    result['pole_treatment_df'] = pole_treatment_df[['TREATMENT', 'COUNT']]

    pole_height_df = get_pole_height_dataframe(mongo, end_year, 20, 100, 5)
    result['pole_height_df'] = pole_height_df[['HEIGHT', 'COUNT']]

    return result


def get_line2_data(mongo, year):
    """
    This function will calculate the Radial Diff between the current year and last year.
    :param mongo: mongo DB object
    :param start_date:
    :param end_date:
    :return: DICT contains the keys as line_no, description, recorded_units and their values .
    """
    results_old = mongo.db.radial_data.find_one(
        {"CONDUCTORCODE": "PILC", "YEAR": year-1}
    )
    results_new = mongo.db.radial_data.find_one(
        {"CONDUCTORCODE": "PILC", "YEAR": year}
    )
    results_old_count = results_old["COUNT"] if results_old else 0
    results_new_count = results_new["COUNT"] if results_new else 0
    result = dict()
    result["line_no"] = "2"
    result["description"] = "Miles of paper-insulated lead sheath cable (PILC) replaced across all Company programs"
    if (results_old is None) or (results_new is None):
        result["recorded_units"] = "Historian databases are not updated yet"
    else:
        result["recorded_units"] = results_old_count - results_new_count
        result["recorded_units"] = "{0:.2f}".format(result["recorded_units"])
    return result


def get_line2_dataset(mongo, year):
    """
    This function will fetch the current year, last year radial and null conductor data from database.
    :param mongo: mongo db connection object.
    :param start_date:
    :param end_date:
    :return: return the dict contains the null, radial data.
    """
    results_old = mongo.db.radial_data.find(
        {"YEAR": (year - 1)}, {'_id': 0, "YEAR": 0}
    )
    results_new = mongo.db.radial_data.find(
        {"YEAR": year}, {'_id': 0, "YEAR": 0}
    )
    results = dict()
    results["old_dataframe"] = pd.DataFrame(list(results_old))
    results["new_dataframe"] = pd.DataFrame(list(results_new))
    results["old_dataframe"].fillna("Other", inplace=True)
    results["old_dataframe"]["CONDUCTORCODE"] = results["old_dataframe"]["CONDUCTORCODE"].apply(lambda x: x if x else "Other")
    results["old_dataframe"]["CONDUCTORCODE"].replace({"XLP": "XLPE"}, inplace=True)
    results["old_dataframe"] = results["old_dataframe"].groupby(["CONDUCTORCODE"]).sum().reset_index()
    results["new_dataframe"].fillna("Other", inplace=True)
    results["new_dataframe"]["CONDUCTORCODE"] = results["new_dataframe"]["CONDUCTORCODE"].apply(lambda x: x if x else "Other")
    results["new_dataframe"]["CONDUCTORCODE"].replace({"XLP": "XLPE"}, inplace=True)
    results["new_dataframe"] = results["new_dataframe"].groupby(["CONDUCTORCODE"]).sum().reset_index()

    results_old = mongo.db.null_data.find(
        {"YEAR": (year - 1)}, {'_id': 0, "YEAR": 0}
    )
    results_new = mongo.db.null_data.find(
        {"YEAR": year}, {'_id': 0, "YEAR": 0}
    )
    results["null_old"] = pd.DataFrame(list(results_old))
    results["null_new"] = pd.DataFrame(list(results_new))
    return results


def get_line3A_data(mongo, year):
    results_old = mongo.db.radial_data.find_one(
        {"CONDUCTORCODE": "HMWPE", "YEAR": year-1}
    )
    results_new = mongo.db.radial_data.find_one(
        {"CONDUCTORCODE": "HMWPE", "YEAR": year}
    )
    results_old_count = results_old["COUNT"] if results_old else 0
    results_new_count = results_new["COUNT"] if results_new else 0
    result = dict()
    result["line_no"] = "3A"
    result["description"] = "Miles of HMWPE cable, respectively, replaced across all Company programs"
    if (results_old is None) or (results_new is None):
        result["recorded_units"] = "Historian databases are not updated yet"
    else:
        result["recorded_units"] = results_old_count - results_new_count
        result["recorded_units"] = "{0:.2f}".format(result["recorded_units"])
    return result


# def get_line3A_dataset(mongo, year):
#     results_old = mongo.db.radial_data.find(
#         {"YEAR": year-1}, {'_id': 0, "YEAR": 0}
#     )
#     results_new = mongo.db.radial_data.find(
#         {"YEAR": year}, {'_id': 0, "YEAR": 0}
#     )
#     results = dict()
#     results["old_dataframe"] = pd.DataFrame(list(results_old))
#     results["new_dataframe"] = pd.DataFrame(list(results_new))
#
#     results_old = mongo.db.null_data.find(
#         {"YEAR": year - 1}, {'_id': 0, "YEAR": 0}
#     )
#     results_new = mongo.db.null_data.find(
#         {"YEAR": year}, {'_id': 0, "YEAR": 0}
#     )
#     results["null_old"] = pd.DataFrame(list(results_old))
#     results["null_new"] = pd.DataFrame(list(results_new))
#     return results


def get_line3B_data(mongo, end_year):
    """
    This function will caliculate the sum of MILES from line_3b collection.
    Present we kept the dummy data in mongo collection.
    :param mongo: monog db collection object.
    :param year:
    :return: DICT contains the keys as line_no, description, recorded_units and their values .
    """
    results = mongo.db.hmwpe_data.find(
        {"$expr": {"$eq": [{"$year": "$TESTDATE"}, end_year]}},
        {'_id': 0, "TESTDATE": 0})
    df = pd.DataFrame(list(results))

    if df.empty:
        total = 0
    else:
        df["PGE_CONDUCTORCODE"] = df["PGE_CONDUCTORCODE"].apply(lambda x: str(int(x)) if x else "")
        df = df[df["PGE_CONDUCTORCODE"].isin(definitions.HMWPE_SELECTED_CODES)]
        total = df['MILES'].sum()

    result = dict()
    result["line_no"] = "3B"
    result["description"] = "Miles of HMWPE cable, respectively, rejuvated (injected) across all Company programs"
    result["recorded_units"] = "{0:.2f}".format(total)
    return result


def get_line3B_dataset(mongo, end_year):
    """
    This function will generate line_3b dataframe.
    :param mongo: MongoDB connection object
    :param year:
    :return: dataframe
    """
    results = mongo.db.hmwpe_data.find(
        {"$expr": {"$eq": [{"$year": "$TESTDATE"}, end_year]}},
        {'_id': 0})
    df = pd.DataFrame(list(results))

    if not df.empty:
        df["CIRCUIT_TYPE"] = df["CIRCUIT_TYPE"].apply(lambda x: definitions.HMWPE_CIRCUIT_TYPES.get(str(int(x))) if x else "")
        df["PGE_CONDUCTORCODE"] = df["PGE_CONDUCTORCODE"].apply(lambda x: str(int(x)) if x else "")
        df["STATUS"] = df["STATUS"].fillna("0")
        df["STATUS"] = df["STATUS"].apply(lambda x: definitions.HMWPE_STATUS_CODES.get(str(int(x))))
        df = df[df["PGE_CONDUCTORCODE"].isin(definitions.HMWPE_SELECTED_CODES)]
        df["TESTDATE"] = df["TESTDATE"].fillna("")
        df["TESTDATE"] = df["TESTDATE"].apply(lambda x: x.date() if x else x)
        df["CUSTOMER OWNED"] = df["CUSTOMEROWNED"]
        df["CIRCUIT TYPE"] = df["CIRCUIT_TYPE"]
        #df["TESTDATE"] = df["TESTDATE"]
        df = df.sort_values(['TESTDATE', 'INSTALLJOBNUMBER', 'STATUS', 'CUSTOMER OWNED', 'CIRCUIT TYPE'])
        df = df[["TESTDATE", "INSTALLJOBNUMBER", "STATUS", "CUSTOMER OWNED", "CIRCUIT TYPE", "MILES"]]
        df = df.groupby(["TESTDATE", "INSTALLJOBNUMBER", "STATUS", "CUSTOMER OWNED", "CIRCUIT TYPE", "MILES"]
                        ).size().reset_index(name='COUNT')
        try:
            df["MILES"] = df.apply(lambda x: x["MILES"]*x["COUNT"], axis=1)
        except Exception as e:
            print(e)
    return df


def get_line4_data(mongo, start_date, end_date):
    """
    This function will the conductor LENGTH Sum form conductor_data collection.
    :param mongo: mongo db connection object.
    :param start_date:
    :param end_date:
    :return: DICT contains the keys as line_no, description, recorded_units and their values .
    """
    data = mongo.db.conductor_data.aggregate([
        {'$match': {"IDYEAR": {'$lte': end_date.year, "$gte": start_date.year}}},
        {'$group': {'_id': None, 'total': {'$sum': '$LENGTH'}}}
    ])
    data = list(data)
    total = data[0]["total"] if data else 0
    result = dict()
    result["line_no"] = "4"
    result["description"] = "Miles of Overhead conductor replaced or installed across all Company programs"
    result["recorded_units"] = total
    result["recorded_units"] = "{0:.2f}".format(total)
    return result


def get_line4_dataset(mongo, start_date, end_date):
    """
    This function will generate conductor dataframe from mongo data. It will fetch conductor data from
    conductor_data mongo collection. Data fetched between the start and end dates.
    :param mongo: MongoDB connection object
    :param start_date:
    :param end_date:
    :return: dataframe
    """
    results = mongo.db.conductor_data.find(
        {"IDYEAR": {'$lte': end_date.year, "$gte": start_date.year}},
        {'MAT': 1, 'LENGTH': 1}
    )
    conductor_df = pd.DataFrame(list(results))
    if not conductor_df.empty:
        conductor_df = conductor_df.drop(["_id"], axis=1)
        conductor_df = conductor_df[['MAT', 'LENGTH']]

    mwc_df = conductor_df.copy()
    if not mwc_df.empty:
        mwc_df = mwc_df.rename(columns={"MAT": "MWC"})
        mwc_df["MWC"] = mwc_df["MWC"].apply(lambda x: x[:2] if x else "Null")
        mwc_df = mwc_df.groupby('MWC', as_index=False).agg({'LENGTH': 'sum'})
        mwc_df = mwc_df[['MWC', 'LENGTH']]
        loc = Index(mwc_df['MWC']).get_loc("Null")
        null_df = mwc_df.loc[[loc]]
        mwc_df.drop(mwc_df.index[loc], inplace=True)
        mwc_df = mwc_df.append(null_df)

    if not conductor_df.empty:
        conductor_df["MAT"] = conductor_df["MAT"].apply(lambda x: x if x else "Null")
        conductor_df = conductor_df.groupby('MAT', as_index=False).agg({'LENGTH': 'sum'})
        conductor_df = conductor_df[['MAT', 'LENGTH']]
        loc = Index(conductor_df['MAT']).get_loc("Null")
        null_df = conductor_df.loc[[loc]]
        conductor_df.drop(conductor_df.index[loc], inplace=True)
        conductor_df = conductor_df.append(null_df)
    return {"conductor_df": conductor_df, "mwc_df": mwc_df}


def get_line5_data(mongo, year):
    """
    This function will fetch Grasshopper switches Count for last two years.
    :param mongo: mongo database connection object
    :param start_date:
    :param end_date:
    :return: return the dict with line_no, description, recorded_units
    """
    resultsold = mongo.db.grasshopper_data.find_one({'YEAR': (year - 1)})
    resultsnew = mongo.db.grasshopper_data.find_one({'YEAR': year})
    resultsold_count = resultsold.get("RECORDS", 0) if resultsold else 0
    resultsnew_count = resultsnew.get("RECORDS", 0) if resultsnew else 0
    result = dict()
    result["line_no"] = "5"
    result["description"] = "Grasshopper switches replaced across all Company programs"
    if (resultsold is None) or (resultsnew is None):
        result["recorded_units"] = "Historian databases are not updated yet"
    else:
        result["recorded_units"] = resultsold_count - resultsnew_count
        result["recorded_units"] = int(result["recorded_units"])
    return result


def get_line5_dataset(mongo, end_year):
    """
    This function will return Grosshoppers data as dataframe. will fetch from mongo collection and creates Dataframe.
    :param mongo: mongo db connection object.
    :param start_date:
    :param end_date:
    :return: groshoppers dataframe object.
    """
    results = mongo.db.grasshopper_data.find({"YEAR": {"$in": [end_year, end_year-1]}}, {'_id': 0}).sort('YEAR', 1)
    grasshopper_df = pd.DataFrame(list(results))
    if grasshopper_df.empty:
        return pd.DataFrame()
    grasshopper_df = grasshopper_df[['YEAR', 'RECORDS']]
    return grasshopper_df


def get_line6_data(mongo, start_date, end_date):
    """
    This function will fetch fuse records Count for given date range.
    :param mongo: mongo database connection object
    :param start_date:
    :param end_date:
    :return: return the dict with line_no, description, recorded_units
    """
    results = mongo.db.fuse_data.find(
        {"IYEAR": {'$lte': end_date.year, "$gte": start_date.year}}
    )
    result = dict()
    result["line_no"] = "6"
    result["description"] = "Overhead fuse installations across all Company programs"
    result["recorded_units"] = "{:,}".format(results.count())
    return result


def get_line6_dataset(mongo, start_date, end_date):
    """
    This function will return Fuse data as dataframe. will fetch from mongo collection and creates Dataframe.
    :param mongo: mongo db connection object.
    :param start_date:
    :param end_date:
    :return: fuse dataframe object.
    """
    results = mongo.db.fuse_data.find(
        {"IYEAR": {'$lte': end_date.year, "$gte": start_date.year}},
        {'_id': 0, 'IYEAR': 0}
    )
    fuse_df = pd.DataFrame(list(results))
    if not fuse_df.empty:
        fuse_df["INSTALLATIONDATE"] = fuse_df["INSTALLATIONDATE"].apply(lambda x: x.date() if x else "Null")
        fuse_df = fuse_df.sort_values(["INSTALLATIONDATE", "INSTALLJOBNUMBER", "INSTALLJOBYEAR", "MAT"])
        fuse_df["JOBPREFIX"].fillna("Null", inplace=True)
        fuse_df["MAT"].fillna("Null", inplace=True)
        fuse_df["MWC"].fillna("Null", inplace=True)
        fuse_df["INSTALLATIONDATE"].fillna("Null", inplace=True)
        fuse_df = fuse_df.groupby(["INSTALLATIONDATE", "INSTALLJOBNUMBER", "INSTALLJOBYEAR", "JOBPREFIX", "MAT", "MWC"]
                                  ).size().reset_index(name='COUNT')
        #fuse_df = fuse_df.drop_duplicates()
    return fuse_df


def get_audit_data(mongo):
    data = mongo.db.audit_data.find({}).sort([("completed_timestamp", -1)]).limit(1)
    completed_datetime = list(data)
    completed_datetime = completed_datetime[0].get("completed_timestamp") if completed_datetime else ""
    completed_datetime = datetime.strftime(completed_datetime, "%m/%d/%y %H:%M:%S %p") if completed_datetime else ""
    result = dict()
    result["line_no"] = ""
    result["description"] = "Data As Of"
    result["recorded_units"] = completed_datetime
    return result


def get_grc_report_data(mongo, start_date, end_date):
    """
    This function will generate summary data for summary page.
    This function will call other functions for summary data
    :param mongo: For mongo db connection object
    :param start_date: as per the user request.Type datetime
    :param end_date: as per the user request Type datetime
    :return: List which contains the list of dict's
    """
    report_data = list()
    report_data.append(get_line1A_data(mongo, start_date.year, end_date.year))
    report_data.append(get_line1B_data(mongo, end_date.year))
    report_data.append(get_line1C_data(mongo, end_date.year))
    report_data.append(get_line2_data(mongo, end_date.year))
    report_data.append(get_line3A_data(mongo, end_date.year))
    report_data.append(get_line3B_data(mongo, end_date.year))
    report_data.append(get_line4_data(mongo, start_date, end_date))
    report_data.append(get_line5_data(mongo, end_date.year))
    report_data.append(get_line6_data(mongo, start_date, end_date))
    report_data.append(get_audit_data(mongo))
    return report_data


def generate_summary_dataset(mongo, start_date, end_date):
    """
    This function will generate summary sheet data as dataframe.
    :param mongo: For mongo db connection object
    :param start_date: as per the user request.Type datetime
    :param end_date: as per the user request Type datetime
    :return: This will return summary data as a dataframe
    """
    end_year = end_date.year
    empty_row = ['', '', '']
    summary_data = [
        empty_row,
        ['', '                                             PACIFIC GAS AND ELECTRIC COMPANY', ''],
        ['', '                                {} GENERAL RATE CASE APPLICATION  15-09-001'.format(end_year), ''],
        ['', '                                                 ELECTRIC DISTRIBUTION', ''],
        empty_row,
        ['', '                                                           TABLE 3-5', ''],
        ['', '                                  ELECTRIC DISTRIBUTION {} UNIT REPORT'.format(end_year), ''],
        empty_row,
        ["LINE NO.", "DESCRIPTION", "{} Recorded Units".format(end_year)],
    ]
    report_data = get_grc_report_data(mongo, start_date, end_date)
    for item in report_data:
        summary_data.append([item["line_no"], item["description"], item["recorded_units"]])
        summary_data.append(empty_row)
    return pd.DataFrame(summary_data)


def generate_grc_report_xlsx(mongo, start_date, end_date):
    """
    This function will generate data frames and create excel file and format(Styles) the excel file
    Created one helper function for each tab to fetch and process the data.
    Created on format function to apply the styles
    :param mongo: For mongo db connection object
    :param start_date: as per the user request.Type datetime
    :param end_date: as per the user request.Type datetime
    :return: Returns the excel sheet name
    """
    sheet_name = conf.GRC_FILE_NAME
    #ext = datetime.strftime(datetime.now(), "%H_%M_%S")
    sheet_name = "{}_{}.xlsx".format(sheet_name, end_date.year)

    #file_path = os.path.join(conf.GRC_FILE_DIR, sheet_name)
    file_path = os.path.join(conf.GRC_FILE_DIRS[0], sheet_name)
    writer = ExcelWriter(file_path, engine='xlsxwriter', options={'remove_timezone': True})
    workbook = writer.book

    summary_dataframe = generate_summary_dataset(mongo, start_date, end_date)
    summary_dataframe.to_excel(writer, "Summary", index=False, header=False)
    formatter.format_summary_sheet(workbook, writer.sheets['Summary'])

    line_1a = get_line1A_dataset(mongo, start_date.year, end_date.year)
    line_1a["mat_df"].to_excel(writer, "Line No. 1A", startrow=1, startcol=0, index=False)
    line_1a["mwc_df"].to_excel(writer, "Line No. 1A", startrow=1, startcol=3, index=False)
    formatter.format_sheet_1a(workbook, writer.sheets['Line No. 1A'], line_1a)

    line_1b = get_line1B_dataset(mongo, end_date.year)
    line_1b["support_structure_df"].to_excel(writer, "Line No. 1B", startrow=1, startcol=0, index=False)
    line_1b["pole_count_by_age_df"].to_excel(writer, "Line No. 1B", startrow=1, startcol=3, index=False)
    formatter.format_sheet_1b(workbook, writer.sheets['Line No. 1B'], line_1b)

    line_1c = get_line1C_dataset(mongo, end_date.year)
    formatter.format_sheet_1c(workbook, writer, line_1c)

    line_2 = get_line2_dataset(mongo, end_date.year)
    formatter.format_sheet_line2(workbook, writer, line_2, end_date.year)

    # line_3a = get_line3A_dataset(mongo, end_date.year)
    # formatter.format_sheet_line3A(workbook, writer, line_3a, end_date.year)

    line_3b_df = get_line3B_dataset(mongo, end_date.year)
    line_3b_df.to_excel(writer, "Line No. 3B", index=False)

    formatter.format_sheet_line3b(workbook, writer.sheets['Line No. 3B'], line_3b_df)

    line_4 = get_line4_dataset(mongo, start_date, end_date)
    line_4["conductor_df"].to_excel(writer, "Line No. 4", startrow=0, startcol=0, index=False)
    line_4["mwc_df"].to_excel(writer, "Line No. 4", startrow=0, startcol=3, index=False)
    formatter.format_sheet_line4(workbook, writer.sheets['Line No. 4'], line_4)

    line_5_df = get_line5_dataset(mongo, end_date.year)
    line_5_df.to_excel(writer, "Line No. 5", index=False)
    formatter.format_sheet_line5(workbook, writer.sheets['Line No. 5'], line_5_df)

    line_6_df = get_line6_dataset(mongo, start_date, end_date)
    line_6_df.to_excel(writer, "Line No. 6", index=False)
    formatter.format_sheet_line6(workbook, writer.sheets['Line No. 6'], line_6_df)

    writer.save()
    for ffname in conf.GRC_FILE_DIRS[1:]:
        dest_path = os.path.join(ffname, sheet_name)
        shutil.copy(file_path, dest_path)
    return sheet_name

def insert_data(mongo, insert_data):
	 support_results = mongo.db[collection_name].insert_one(insert_data)
	 return support_results.inserted_id


def find_project(mongo , query_obj):
    return_data ={"total_records":0 ,"documents":[]}
    if len(query_obj) >0:
        result = mongo.db[collection_name].find(query_obj)
        total = result.count()
        return_data['total_records'] =total
        if total>0:
            return_data['documents'] =result

    return return_data


def check_islocked(mongo ,selected_year='' ,check_job_type =1 ):
    # 1 = overhead ; 2 == underground
    return_count = 0
    statges =[]

    if check_job_type ==1:
        project_completion_date = "$oh_estimated_completion_date"
        is_itlocked =  "overhead.is_itlocked"
    else:
        project_completion_date = "$ug_estimated_completion_date"
        is_itlocked =  "underground.is_itlocked"

    stage_one = {'$project':{'year': { '$year': project_completion_date },"job_type":1,"overhead":1,"underground":1}}
    stage_tow ={'$match':{ "year":selected_year ,is_itlocked:1}}
    stage_three = { '$match': {'$or': [ { 'job_type': check_job_type }, { 'job_type': 3 } ] }}
    count_stage ={'$count':"totalCount"}
    statges.append(stage_one)
    statges.append(stage_tow)
    statges.append(stage_three)
    statges.append(count_stage)

    result = list(mongo.db[collection_name].aggregate(statges))
    if len(result) >0:
        l1 = result[0]
        return_count =l1['totalCount']

    return return_count

def project_update(mongo ,where_condation={} ,update_data= {}, upsert_value =False ,multi_value =True):

    result = mongo.db[collection_name].update(where_condation ,{'$set':update_data} ,upsert=upsert_value , multi =multi_value)
    return result

#project_update_rename
def project_update_rename(mongo ,where_condation={} ,update_data= {}, upsert_value =False ,multi_value =False):

    result = mongo.db[collection_name].update(where_condation ,{'$rename':update_data} ,upsert=upsert_value , multi =multi_value)
    return result

def project_update_unset(mongo ,where_condation={} ,update_data= {}, upsert_value =False ,multi_value =False):

    result = mongo.db[collection_name].update(where_condation ,{'$unset':update_data} ,upsert=upsert_value , multi =multi_value)
    return result

def lock_year(mongo ,selected_year='' ,check_job_type =1 ):
    # 1 = overhead ; 2 == underground
    return_count = 0
    statges =[]

    temp_list =[]
    not_filled_docs =[]
    not_filled_docs_ids =[]

    current_type = "overhead"
    if check_job_type ==1:
        project_completion_date = "$oh_estimated_completion_date"
        is_itlocked =  "overhead.is_itlocked"
        current_type = "overhead"

    else:
        project_completion_date = "$ug_estimated_completion_date"
        is_itlocked =  "underground.is_itlocked"
        current_type = "underground"

    stage_one = {'$project':{'year': { '$year': project_completion_date },"job_number":1, "job_owner":1,"job_type":1,"overhead":1,"underground":1}}
    stage_tow ={'$match':{ "year":selected_year ,is_itlocked:0}}
    stage_three = { '$match': {'$or': [ { 'job_type': check_job_type }, { 'job_type': 3 } ] }}
    #count_stage ={'$count':"totalCount"}
    statges.append(stage_one)
    statges.append(stage_tow)
    statges.append(stage_three)
    #statges.append(count_stage)

    result = list(mongo.db[collection_name].aggregate(statges))

    proj_completion_fields = ["from" ,"to" ,"length","which_type" ,"ave" ,"present","ultimate" ,"size", "spec","config" ,"voltage"]
    if len(result) >0:
        for doc in list(result):
            this_doc_id = str(doc["_id"])
            temp_list.append(ObjectId(this_doc_id))


            if doc.get( current_type,-1) !=-1:
                temp_curent_type_data = doc[current_type]

               #check adadion
                is_valid_addition = False
                if temp_curent_type_data.get("addition" ,-1) != -1:
                    temp_current_addition_data =temp_curent_type_data.get("addition")
                    if temp_current_addition_data.get("proj_completion" ,-1) !=-1 :
                        temp_current_addition_proj_completion = temp_current_addition_data['proj_completion']
                        value_count =0
                        null_value_count =0
                        for key in proj_completion_fields:
                            if bool(temp_current_addition_proj_completion.get(key)):
                                value_count =value_count+1
                            else:
                                null_value_count =null_value_count+1
                        if  value_count >0:
                            if value_count != len(proj_completion_fields):
                                if this_doc_id not in not_filled_docs_ids:
                                    not_filled_docs_ids.append(this_doc_id)
                                    not_filled_docs.append({"id":this_doc_id , "job_number":doc['job_number'] ,"job_owner":doc['job_owner'] })

                            #check business_finance need atleast one value
                            if value_count == len(proj_completion_fields):

                                if temp_current_addition_data.get('business_finance' ,-1) !=-1:
                                    count_business_finance =0;

                                    for key, value in temp_current_addition_data['business_finance'].items():
                                        if value !='':
                                            count_business_finance =count_business_finance+1

                                    if count_business_finance >0:
                                        is_valid_addition = True
                                    else:
                                        is_valid_addition = False
                                        if this_doc_id not in not_filled_docs_ids:
                                            not_filled_docs_ids.append(this_doc_id)
                                            not_filled_docs.append({"id":this_doc_id , "job_number":doc['job_number'] ,"job_owner":doc['job_owner'] })
                                else:
                                    if this_doc_id not in not_filled_docs_ids:
                                        not_filled_docs_ids.append(this_doc_id)
                                        not_filled_docs.append({"id":this_doc_id , "job_number":doc['job_number'] ,"job_owner":doc['job_owner'] })

                #check reconductor
                is_valid_reconductor = False
                if temp_curent_type_data.get("reconductor" ,-1) != -1:
                    temp_current_reconductor_data =temp_curent_type_data.get("reconductor")
                    if temp_current_reconductor_data.get("proj_completion" ,-1) !=-1 :
                        temp_current_reconductor_proj_completion = temp_current_reconductor_data['proj_completion']
                        value_count =0
                        null_value_count =0
                        for key in proj_completion_fields:
                            if bool(temp_current_reconductor_proj_completion.get(key)):
                                value_count =value_count+1
                            else:
                                null_value_count =null_value_count+1
                        if  value_count >0:
                            if value_count != len(proj_completion_fields):
                                if this_doc_id not in not_filled_docs_ids:
                                    not_filled_docs_ids.append(this_doc_id)
                                    not_filled_docs.append({"id":this_doc_id , "job_number":doc['job_number'] ,"job_owner":doc['job_owner'] })

                            #check business_finance need atleast one value
                            if value_count == len(proj_completion_fields):

                                if temp_current_addition_data.get('business_finance' ,-1) !=-1:
                                    count_business_finance =0;

                                    for key, value in temp_current_addition_data['business_finance'].items():
                                        if value !='':
                                            count_business_finance =count_business_finance+1

                                    if count_business_finance >0:
                                        is_valid_reconductor = True
                                    else:
                                        is_valid_reconductor = False
                                        if this_doc_id not in not_filled_docs_ids:
                                            not_filled_docs_ids.append(this_doc_id)
                                            not_filled_docs.append({"id":this_doc_id , "job_number":doc['job_number'] ,"job_owner":doc['job_owner'] })
                                else:
                                    if this_doc_id not in not_filled_docs_ids:
                                        not_filled_docs_ids.append(this_doc_id)
                                        not_filled_docs.append({"id":this_doc_id , "job_number":doc['job_number'] ,"job_owner":doc['job_owner'] })

                #check Removal removal
                is_valid_removal = False
                if temp_curent_type_data.get("removal" ,-1) != -1:
                    temp_current_removal_data =temp_curent_type_data.get("removal")
                    if temp_current_removal_data.get("proj_completion" ,-1) !=-1 :
                        temp_current_removal_proj_completion = temp_current_removal_data['proj_completion']
                        value_count =0
                        null_value_count =0
                        for key in proj_completion_fields:
                            if bool(temp_current_removal_proj_completion.get(key)):
                                value_count =value_count+1
                            else:
                                null_value_count =null_value_count+1
                        if  value_count >0:
                            if value_count != len(proj_completion_fields):
                                if this_doc_id not in not_filled_docs_ids:
                                    not_filled_docs_ids.append(this_doc_id)
                                    not_filled_docs.append({"id":this_doc_id , "job_number":doc['job_number'] ,"job_owner":doc['job_owner'] })

                            #check business_finance need atleast one value
                            if value_count == len(proj_completion_fields):

                                if temp_current_addition_data.get('business_finance' ,-1) !=-1:
                                    count_business_finance =0;

                                    for key, value in temp_current_addition_data['business_finance'].items():
                                        if value !='':
                                            count_business_finance =count_business_finance+1

                                    if count_business_finance >0:
                                        is_valid_removal = True
                                    else:
                                        is_valid_removal = False
                                        if this_doc_id not in not_filled_docs_ids:
                                            not_filled_docs_ids.append(this_doc_id)
                                            not_filled_docs.append({"id":this_doc_id , "job_number":doc['job_number'] ,"job_owner":doc['job_owner'] })
                                else:
                                    if this_doc_id not in not_filled_docs_ids:
                                        not_filled_docs_ids.append(this_doc_id)
                                        not_filled_docs.append({"id":this_doc_id , "job_number":doc['job_number'] ,"job_owner":doc['job_owner'] })

                #final Checking . Check in Current Document have valid data in adadion OR reconductor OR removal
                if is_valid_addition or is_valid_reconductor or is_valid_removal:
                    pass
                else:
                    if this_doc_id not in not_filled_docs_ids:
                        not_filled_docs_ids.append(this_doc_id)
                        not_filled_docs.append({"id":this_doc_id , "job_number":doc['job_number'] ,"job_owner":doc['job_owner'] })



            else:

                if this_doc_id not in not_filled_docs_ids:
                    not_filled_docs_ids.append(this_doc_id)
                    not_filled_docs.append({"id":this_doc_id , "job_number":doc['job_number'] ,"job_owner":doc['job_owner'] })



            #print(type(doc))

    return_data ={"update_result":False ,"not_filled_docs":[]}
    if len(temp_list)>0 and len(not_filled_docs) ==0:
        where_condation ={}
        update_data ={}
        where_condation["_id"] ={"$in":temp_list}
        if check_job_type ==1:
            #where_condation["overhead"] ={"$exists":1}
            #where_condation["overhead.is_itlocked"] ={"$exists":1}
            update_data['overhead.is_itlocked'] =1
            result = mongo.db[collection_name].update(where_condation ,{'$set':update_data} ,upsert=False , multi =True)
            return_data['update_result'] =result
        else:
            where_condation["underground"] ={"$exists":1}
            where_condation["underground.is_itlocked"] ={"$exists":1}
            update_data['underground.is_itlocked'] =1
            result = mongo.db[collection_name].update(where_condation ,{'$set':update_data} ,upsert=False , multi =True)
            return_data['update_result'] =result
    else:
        return_data['update_result'] = False
        return_data['not_filled_docs'] = not_filled_docs



    print(return_data)
    return return_data

def remove_document(mongo ,id=''):
    result = mongo.db[collection_name].delete_one({'_id': id})
    return result
