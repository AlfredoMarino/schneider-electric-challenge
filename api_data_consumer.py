import pandas as pd

URLS = [
    "http://schneiderapihack-env.eba-3ais9akk.us-east-2.elasticbeanstalk.com/first",
    "http://schneiderapihack-env.eba-3ais9akk.us-east-2.elasticbeanstalk.com/second",
    "http://schneiderapihack-env.eba-3ais9akk.us-east-2.elasticbeanstalk.com/third"
]

dataset_dtypes = {
    "CITY ID": "object",
    "CONTINENT": "object",
    "City": "object",
    "DAY": "int8",
    "DAY WITH FOGS": "int",
    "EPRTRAnnexIMainActivityCode": "object",
    "EPRTRAnnexIMainActivityLabel": "object",
    "EPRTRSectorCode": "int",
    "FacilityInspireID": "object",
    "MONTH": "int",
    "REPORTER NAME": "object",
    "avg_temp": "float",
    "avg_wind_speed": "float",
    "countryName": "object",
    "eprtrSectorName": "object",
    "facilityName": "object",
    "max_temp": "float",
    "max_wind_speed": "float",
    "min_temp": "float",
    "min_wind_speed": "float",
    "pollutant": "object",
    "reportingYear": "int8",
    "targetRelease": "object"
}


class ApiDataConsumer:

    def __init__(self, urls, dtypes = None):
        self.urls = urls
        self.dtypes = dtypes

    def get_api_df(self, verbose: bool = True):
        print("Getting data from API")
        api_df = None
        for url in self.urls:
            json_df = pd.read_json(url, dtype=self.dtypes)
            json_df = json_df.drop("", axis=1)

            if verbose:
                print(f"url: {url} json_df.shape: {json_df.shape}")

            if api_df is not None:
                api_df = pd.concat([api_df, json_df])
            else:
                api_df = json_df

        return api_df


api = ApiDataConsumer(URLS, dataset_dtypes)

api.get_api_df()
