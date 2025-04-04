import dask.dataframe as dd
import pandas as pd

# read parquets in occurrence/ dir, can filter by year (so in this case 2019 or later)
df = dd.read_parquet(
    "occurrence/",
    columns=[
        "id",
        "date_mid",
        "decimalLongitude",
        "decimalLatitude",
        "speciesid",
        "originalScientificName",
    ],
    filters=[("year", ">=", "2019")],
)
df = df.compute()
species = df["speciesid"].value_counts() > 50  # only care about >50 obs
species_list = species[species].index

cropped = df[df["speciesid"].isin(list(species_list))]
cropped = cropped.rename(
    columns={
        "speciesid": "taxon_id",
        "decimalLongitude": "longitude",
        "decimalLatitude": "latitude",
        "originalScientificName": "latin_name",
    }
)
cropped["date"] = pd.to_datetime(
    cropped["date_mid"] * 1000000
)  # convert milliseconds to nanoseconds
cropped.to_csv("obis_train_20192024.csv")  # save as csv for training

metadata = cropped[["taxon_id", "latin_name"]].drop_duplicates()
metadata.to_json(
    "obis_prior_train_meta_20192024.json", orient="records", lines=False
)  # save as json for training
