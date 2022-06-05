import pandas
import glob
import os

def read_add_year_gender(filepath: str, gender: str, year: int):
    dataframe = pandas.read_csv(filepath)
    dataframe["gender"] = gender
    dataframe["year"] = year
    return dataframe

def join_male_female(path: str, year: int):
    files = os.path.join(path, "*.csv")
    files = glob.glob(files)
    dataframe = pandas.concat(map(pandas.read_csv, files), ignore_index=True)
    return dataframe[dataframe["year"] == year]

def join_datasets_year(path: str, years: list):
    files = os.path.join(path, "*.csv")
    files = glob.glob(files)
    dataframe = pandas.concat(map(pandas.read_csv, files), ignore_index=True)
    return dataframe[dataframe.year.isin(years)]

dataframe = read_add_year_gender("./prog_datasci_2/activities/activity_4/data/female_players_16.csv", "F", 2021)
dataframe.to_csv("./prog_datasci_2/activities/activity_4/data/female_players_16.csv", index=False)
dataframe = read_add_year_gender("./fitxers/players_16.csv", "M", 2021)
dataframe.to_csv("./fitxers/resultats/ex1_a/players_16.csv", index=False)
dataframe = read_add_year_gender("./fitxers/female_players_17.csv", "F", 2021)
dataframe.to_csv("./fitxers/resultats/ex1_a/female_players_17.csv", index=False)
dataframe = read_add_year_gender("./fitxers/players_17.csv", "M", 2021)
dataframe.to_csv("./fitxers/resultats/ex1_a/players_17.csv", index=False)
dataframe = read_add_year_gender("./fitxers/female_players_18.csv", "F", 2022)
dataframe.to_csv("./fitxers/resultats/ex1_a/female_players_18.csv", index=False)
dataframe = read_add_year_gender("./fitxers/players_18.csv", "M", 2021)
dataframe.to_csv("./fitxers/resultats/ex1_a/players_18.csv", index=False)
dataframe = join_male_female("./fitxers/resultats/ex1_a/", 2021)
dataframe.to_csv("./fitxers/resultats/ex1_b/players_2021.csv", index=False)
dataframe = join_male_female("./fitxers/resultats/ex1_a/", 2022)
dataframe.to_csv("./fitxers/resultats/ex1_b/players_2022.csv", index=False)
dataframe = join_datasets_year("./fitxers/resultats/ex1_a/", [2021, 2022])
dataframe.to_csv("./fitxers/resultats/ex1_c/players_2021_2022.csv", index=False)