from arcgis.gis import GIS
from arcgis.geocoding import geocode
from arcgis.geoenrichment import standard_geography_query
from arcgis.geoenrichment import enrich
import pandas as pd
import requests
import json
from arcgisvariables import variables
from realtymolesampledata import rental_data
import sys
import math
from pymongo import MongoClient
import geocoder as googlegeocoder
import sys

address = '1521 SW Arbor Creek Dr, Lee\'s Summit, MO 64082'
radius = 2

gis = GIS('https://www.arcgis.com', 'arcgis_python', 'P@ssword123')

with open("./un_pw.json", "r") as file:
    gmap_api = json.load(file)['googleapi']
    google_address = googlegeocoder.google(address, key=gmap_api)

if google_address.error:
    print('!!! Could not geocode address string !!!')
    sys.exit()

x_lon = google_address.current_result.lng
y_lat = google_address.current_result.lat


########################################################
# Getting rental data from RealtyMole. NOTE: Below is how I made requests to the API. I commented out this section and am
# just using sample data for this example. You can see I stored the response data into "realtymoledata50.json". And I pasted
# the data into "realtymolesampledata.txt"
########################################################

# with open("./un_pw.json", "r") as file:
#     realtymole = json.load(file)['realtymole_yahoo']
#
# url = "https://realty-mole-property-api.p.rapidapi.com/rentalListings"
#
# querystring = {"radius":radius,
#                "limit":50,
#                "longitude":x_lon,
#                "latitude":y_lat}
#
# headers = {
#     'x-rapidapi-host': "realty-mole-property-api.p.rapidapi.com",
#     'x-rapidapi-key': realtymole
#     }
#
# response = requests.request("GET", url, headers=headers, params=querystring)
#
# if response.status_code != 200:
#     print('*ScopeOutLog* !!! ERROR with REALTYMOLE API !!!!')
# else:
#     print('*ScopeOutLog* SUCCESS - REALTY MOLE')
#
#
# with open("testdata/RENT_{}.json".format(address), 'w') as file:
#     file.write(json.dumps(json.loads(response.text)))


##### Get sample data instead of make API call above ####
df = pd.DataFrame(data=rental_data)

writer = pd.ExcelWriter('testdata/rentalsummary.xlsx')

highestrent = df['price'].max()
lowestrent = df['price'].min()
averagerent = df['price'].mean()
medianrent = df['price'].median()
sameplesize = len(df)

bedroom_rent = pd.DataFrame(columns=['bedrooms','highestrent','lowestrent','averagerent','medianrent','sameplesize'])

for i in [0,1,2,3,4]:
    bedroom_data = df[df['bedrooms'] == i]

    bedroom_rent = bedroom_rent.append({
        'bedrooms': i,
        'highestrent': bedroom_data['price'].max(),
        'lowestrent': bedroom_data['price'].min(),
        'averagerent': bedroom_data['price'].mean(),
        'medianrent': bedroom_data['price'].median(),
        'sameplesize': len(bedroom_data)
    }, ignore_index=True)

bedroom_rent.to_excel(writer, 'bedroomrents')

rent_range = pd.DataFrame({
                        'rent_0_499':0,
                        'rent_500_999':0,
                        'rent_1000_1499':0,
                        'rent_1500_1999':0,
                        'rent_2000_2499':0,
                        'rent_2500_2999':0,
                        'rent_3000_3499':0,
                        'rent_3500_3999':0,
                        'rent_4000_4499':0,
                        'rent_4500_4999':0,
                        'rent_5000_more':0},
                        index=[0])

for price in df['price']:
    if price < 500:
        rent_range['rent_0_499'] += 1
    elif price < 1000:
        rent_range['rent_500_999'] += 1
    elif price < 1500:
        rent_range['rent_1000_1499'] += 1
    elif price < 2000:
        rent_range['rent_1500_1999'] += 1
    elif price < 2500:
        rent_range['rent_2000_2499'] += 1
    elif price < 3000:
        rent_range['rent_2500_2999'] += 1
    elif price < 3500:
        rent_range['rent_3000_3499'] += 1
    elif price < 4000:
        rent_range['rent_3500_3999'] += 1
    elif price < 4500:
        rent_range['rent_4000_4499'] += 1
    elif price < 5000:
        rent_range['rent_4500_4999'] += 1
    else:
        rent_range['rent_5000_more'] += 1

rent_range.to_excel(writer, 'rentrange')

rental_comps = df[['formattedAddress','squareFootage','bedrooms','bathrooms','price','propertyType','lastSeen','latitude','longitude']]\
    .rename(columns={'lastSeen':'lastSeenOnMarket'})

for i,comp in rental_comps.iterrows():
    R = 6373.0

    lat_prop = math.radians(y_lat)
    lon_prop = math.radians(x_lon)
    lat_comp = math.radians(comp['latitude'])
    lon_comp = math.radians(comp['longitude'])

    dlon = lon_comp - lon_prop
    dlat = lat_comp - lat_prop

    a = math.sin(dlat / 2) ** 2 + math.cos(lat_prop) * math.cos(lat_comp) * math.sin(dlon / 2) ** 2

    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c * 0.621371

    rental_comps.at[i, 'DistanceFromProperty'] = distance

rental_comps.sort_values(by=['DistanceFromProperty']).to_excel(writer, 'rentalcomps')

writer.save()



#######################################################
# Getting data from Arcgis REST API.
#######################################################

comparison_variables = variables['comparison_variables']
non_comparison_variables = variables['noncomparison_variables']

data = enrich(study_areas=[{"address":{"text":address}}],
              analysis_variables=list(non_comparison_variables.keys()),
              return_geometry=False)

if type(data) == dict:
    if data['messages'][0]['type'] == 'esriJobMessageTypeError':
        print('!!! Error with Arcgis api !!!')
        sys.exit()

non_comparison_df = data.drop(columns=['ID', 'apportionmentConfidence', 'OBJECTID', 'areaType', 'bufferUnits', 'bufferUnitsAlias',
                          'bufferRadii', 'aggregationMethod', 'populationToPolygonSizeRating', 'HasData',
                          'sourceCountry'])


non_comparison_df['OwnerOccupancyRate'] = round(non_comparison_df['OWNER_CY'] / non_comparison_df['TOTHU_CY'] * 100, 2)
non_comparison_df['RenterOccupancyRate'] = round(non_comparison_df['RENTER_CY'] / non_comparison_df['TOTHU_CY'] * 100, 2)
non_comparison_df['VacancyRate'] = round(non_comparison_df['VACANT_CY'] / non_comparison_df['TOTHU_CY'] * 100, 2)
non_comparison_df = non_comparison_df.drop(columns=['OWNER_CY','RENTER_CY','RENTER_CY'])




non_comparison_df['SingleFamilyDetached'] = non_comparison_df['ACSUNT1DET_P']
non_comparison_df['SingleFamilyAttached'] = non_comparison_df['ACSUNT1ATT_P']
non_comparison_df['DuplexTriplexQuadplex'] = non_comparison_df['ACSUNT2_P'] + non_comparison_df['ACSUNT3_P']
non_comparison_df['Apartments5to50units'] = non_comparison_df['ACSUNT5_P'] + non_comparison_df['ACSUNT10_P'] + non_comparison_df['ACSUNT20_P']
non_comparison_df['LargeApartments50plus'] = non_comparison_df['ACSUNT50UP_P']
non_comparison_df['MobileHomes'] = non_comparison_df['ACSUNTMOB_P']
non_comparison_df = non_comparison_df.drop(columns=['ACSUNT1DET_P','ACSUNT1ATT_P','ACSUNT2_P','ACSUNT3_P','ACSUNT5_P','ACSUNT10_P','ACSUNT20_P','ACSUNT50UP_P','ACSUNTMOB_P'])

non_comparison_df['1939orBefore'] = non_comparison_df['ACSBLT1939_P']
non_comparison_df['1940_1959'] = non_comparison_df['ACSBLT1940_P'] + non_comparison_df['ACSBLT1950_P']
non_comparison_df['1960_1979'] = non_comparison_df['ACSBLT1960_P'] + non_comparison_df['ACSBLT1970_P']
non_comparison_df['1980_1999'] = non_comparison_df['ACSBLT1980_P'] + non_comparison_df['ACSBLT1990_P']
non_comparison_df['2000_2013'] = non_comparison_df['ACSBLT2000_P'] + non_comparison_df['ACSBLT2010_P']
non_comparison_df['2014orAfter'] = non_comparison_df['ACSBLT2014_P']
non_comparison_df = non_comparison_df.drop(columns=['ACSBLT2014_P','ACSBLT2010_P','ACSBLT2000_P','ACSBLT1990_P','ACSBLT1980_P','ACSBLT1970_P','ACSBLT1960_P','ACSBLT1950_P','ACSBLT1940_P','ACSBLT1939_P'])

data = enrich(study_areas=[{"address":{"text":address}}],
              analysis_variables=list(comparison_variables.keys()),
              comparison_levels=['US.WholeUSA','US.CBSA','US.Counties'],
              return_geometry=False)

comparison_df = data.drop(columns=['ID', 'apportionmentConfidence', 'OBJECTID', 'areaType', 'bufferUnits', 'bufferUnitsAlias',
                          'bufferRadii', 'aggregationMethod', 'populationToPolygonSizeRating', 'HasData',
                          'sourceCountry'])

comparison_df['StdGeographyName'] = comparison_df['StdGeographyName'].str.replace('Metropolitan Statistical Area','MSA')


crime_index_multiplier = {'CRMCYMURD':5, 'CRMCYROBB':86.2, 'CRMCYRAPE':30.9, 'CRMCYASST':246.8}


for i, row in comparison_df.iterrows():
    if row['StdGeographyLevel'] == 'US.WholeUSA':
        comparison_df.at[i, 'CRMCYMURD'] = crime_index_multiplier['CRMCYMURD']
        comparison_df.at[i, 'CRMCYROBB'] = crime_index_multiplier['CRMCYROBB']
        comparison_df.at[i, 'CRMCYRAPE'] = crime_index_multiplier['CRMCYRAPE']
        comparison_df.at[i, 'CRMCYASST'] = crime_index_multiplier['CRMCYASST']
    else:
        comparison_df.at[i, 'CRMCYMURD'] = comparison_df.at[i, 'CRMCYMURD'] * crime_index_multiplier['CRMCYMURD'] * .01
        comparison_df.at[i, 'CRMCYROBB'] = comparison_df.at[i, 'CRMCYROBB'] * crime_index_multiplier['CRMCYROBB'] * .01
        comparison_df.at[i, 'CRMCYRAPE'] = comparison_df.at[i, 'CRMCYRAPE'] * crime_index_multiplier['CRMCYRAPE'] * .01
        comparison_df.at[i, 'CRMCYASST'] = comparison_df.at[i, 'CRMCYASST'] * crime_index_multiplier['CRMCYASST'] * .01


with pd.ExcelWriter('testdata/arcgisoutput.xlsx') as writer:
    non_comparison_df.to_excel(writer, sheet_name='noncomparions')
    comparison_df.to_excel(writer, sheet_name='comparison')

