# UKGenderPayGapData
An analysis of Data. 

The data can be downloaded from `https://gender-pay-gap.service.gov.uk/viewing/download`

# Normalization:
1. Install mysql using `brew install mysql` in mac or `sudo apt-get install mysql` in ubuntu family. (Operating systems I use)
2. Follow ddlMysql folder to create tables.
3. Follow loadMysql to load data to gender_data_uk_raw table.
4. Follow formatDataInMysql to format data as per Bigquery format.
5. Follow dumpMysql to dump data as csv from gender_data_uk.

# Coders:
1. vishu9219
2. Abhishek Asthana
