from arcgis.gis import GIS
from arcgis.geocoding import geocode
from arcgis.geoenrichment import enrich
import numpy as np
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



# #######################################################
# # Provide address and radius value from Arcgis REST API.
# #######################################################
#
#
address = '905 S Parsons Ave, Deland, FL 32720'
radius = 1

gis = GIS('https://www.arcgis.com', 'arcgis_python', 'P@ssword123')

with open("./un_pw.json", "r") as file:
    gmap_api = json.load(file)['googleapi']
    google_address = googlegeocoder.google(address, key=gmap_api)

if google_address.error:
    print('!!! Could not geocode address string !!!')
    sys.exit()

x_lon = google_address.current_result.lng
y_lat = google_address.current_result.lat


#######################################################
# Getting data from Arcgis REST API.
#######################################################

comparison_variables = variables['comparison_variables']
non_comparison_variables = variables['noncomparison_variables']


data = enrich(study_areas=[{"geometry": {"x":x_lon,"y":y_lat}, "areaType":"RingBuffer","bufferUnits":"Miles","bufferRadii":[radius]}],
              analysis_variables=list(non_comparison_variables.keys()),
              return_geometry=False)

if type(data) == dict:
    if data['messages'][0]['type'] == 'esriJobMessageTypeError':
        print('!!! Error with Arcgis api !!!')
        sys.exit()
if data['TOTPOP_CY'][0] == 0:
        print('!!! Do not run if there is no population !!!')
        sys.exit()

non_comparison_df = data.drop(columns=['ID', 'apportionmentConfidence', 'OBJECTID', 'areaType', 'bufferUnits', 'bufferUnitsAlias',
                          'bufferRadii', 'aggregationMethod', 'populationToPolygonSizeRating', 'HasData', 'sourceCountry'])


# Calculate owner, renter, vacancy rate by dividing by total housing units
non_comparison_df['OwnerOccupancyRate'] = round(non_comparison_df['OWNER_CY'] / non_comparison_df['TOTHU_CY'] * 100, 2)
non_comparison_df['RenterOccupancyRate'] = round(non_comparison_df['RENTER_CY'] / non_comparison_df['TOTHU_CY'] * 100, 2)
non_comparison_df['VacancyRate'] = round(non_comparison_df['VACANT_CY'] / non_comparison_df['TOTHU_CY'] * 100, 2)
non_comparison_df = non_comparison_df.drop(columns=['OWNER_CY','RENTER_CY','RENTER_CY'])



# # 9/14/2020 NO LONGER USING HOME VALUE RANGES FOR NOW Group some columns for home value ranges. Reduce the number of ranges to 8.
# non_comparison_df['Under100k'] = non_comparison_df['VAL0_CY_P'] + non_comparison_df['VAL50K_CY_P']
# non_comparison_df['100k_200k'] = non_comparison_df['VAL100K_CY_P'] + non_comparison_df['VAL150K_CY_P']
# non_comparison_df['200k_300k'] = non_comparison_df['VAL200K_CY_P'] + non_comparison_df['VAL250K_CY_P']
# non_comparison_df['300k_500k'] = non_comparison_df['VAL300K_CY_P'] + non_comparison_df['VAL400K_CY_P']
# non_comparison_df['500K_750k'] = non_comparison_df['VAL500K_CY_P']
# non_comparison_df['750k_1M'] = non_comparison_df['VAL750K_CY_P']
# non_comparison_df['1M_2M'] = non_comparison_df['VAL1M_CY_P'] + non_comparison_df['VAL1PT5MCY_P']
# non_comparison_df['2plusM'] = non_comparison_df['VAL2M_CY_P']
# non_comparison_df.drop(columns=['VAL0_CY_P', 'VAL50K_CY_P', 'VAL100K_CY_P', 'VAL150K_CY_P',  'VAL200K_CY_P',
#                                 'VAL250K_CY_P', 'VAL300K_CY_P', 'VAL400K_CY_P', 'VAL500K_CY_P', 'VAL750K_CY_P',
#                                 'VAL1M_CY_P', 'VAL1PT5MCY_P', 'VAL2M_CY_P'])


# Get comparison data
data = enrich(study_areas=[{"address":{"text":address}}],
              analysis_variables=list(comparison_variables.keys()),
              comparison_levels=['US.WholeUSA','US.CBSA','US.Counties','US.Tracts'],
              return_geometry=False)


# Convert crime index to victims per 100,000 people crime rate.
comparison_df = data.drop(columns=['ID', 'apportionmentConfidence', 'OBJECTID', 'areaType', 'bufferUnits', 'bufferUnitsAlias',
                          'bufferRadii', 'aggregationMethod', 'populationToPolygonSizeRating', 'HasData',
                          'sourceCountry'])


comparison_df['StdGeographyName'] = comparison_df['StdGeographyName'].str.replace('Metropolitan Statistical Area','MSA')

# These are current national crime rates. This changes once a year.
crime_index_multiplier = {'CRMCYMURD':5, 'CRMCYROBB':86.2, 'CRMCYRAPE':30.9, 'CRMCYASST':246.8}


# Convert crime index to victims per 100,000 people crime rate.
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
    non_comparison_df = non_comparison_df.rename(columns=variables['noncomparison_variables'])
    comparison_df = comparison_df.rename(columns=variables['comparison_variables'])
    non_comparison_df.to_excel(writer, sheet_name='noncomparions')
    comparison_df.to_excel(writer, sheet_name='comparison')







#######################################################
## Getting rental data from RealtyMole. NOTE: Below is how I made requests to the API. I commented out this section and am
## just using sample data for this example. You can see I stored the response data into "realtymoledata50.json". And I pasted
## the data into "realtymolesampledata.txt"
#######################################################

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
#     with open("testdata/RENT_{}.json".format(address), 'w') as file:
#         file.write(json.dumps(json.loads(response.text)))


##### Get sample data instead of make API call above ####
df = pd.DataFrame(data=rental_data)
writer = pd.ExcelWriter('testdata/rentalsummary.xlsx')


bedroom_rent = pd.DataFrame(columns=['bedrooms','25thPercentile','75thPercentile','averagerent','medianrent','samplesize'])

# calculate values for box plot
for i in [0,1,2,3,4,5,6]:
    bedroom_data = df[df['bedrooms'] == i]

    if bedroom_data.empty:
        bedroom_rent = bedroom_rent.append({
            'bedrooms': i,
            'averagerent': 'N/A',
            'medianrent': 'N/A',
            '25thPercentile': 'N/A',
            '75thPercentile': 'N/A',
            'samplesize': 0
        }, ignore_index=True)
    else:
        bedroom_rent = bedroom_rent.append({
            'bedrooms': i,
            'averagerent': np.mean(bedroom_data['price']),
            'medianrent': np.median(bedroom_data['price']),
            '25thPercentile': np.percentile(bedroom_data['price'], 25),
            '75thPercentile': np.percentile(bedroom_data['price'], 75),
            'samplesize': len(bedroom_data)
        }, ignore_index=True)

bedroom_rent.to_excel(writer, 'bedroomrents')

# Create the rent ranges
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


# Distribute each rental comp we get in the results
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

# rename lastseen to lastseenonmarket
rental_comps = df[['formattedAddress','squareFootage','bedrooms','bathrooms','price','propertyType','lastSeen','latitude','longitude']]\
    .rename(columns={'lastSeen':'lastSeenOnMarket'})

# calculate distance from the main address input
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

    if comp['squareFootage']:
        rental_comps.at[i, 'pricepersqft'] = round(comp['price'] / comp['squareFootage'],2)
    else:
        rental_comps.at[i, 'pricepersqft'] = None


# store data for rental comps sections
rental_comps.sort_values(by=['DistanceFromProperty']).to_excel(writer, 'rentalcomps')



# calculate price per sqft and exclude outliers for graph
bedroomscatterplot = rental_comps[(rental_comps['pricepersqft'] > 0)]
bedroomscatterplot = bedroomscatterplot[(bedroomscatterplot['bedrooms'] > 0)]

outliers = []
threshold = 3
mean_1 = np.mean(bedroomscatterplot['pricepersqft'])
std_1 = np.std(bedroomscatterplot['pricepersqft'])


for price in bedroomscatterplot['pricepersqft']:
    z_score = (price - mean_1) / std_1
    if np.abs(z_score) > threshold:
        outliers.append(price)

bedroomscatterplot[['formattedAddress','squareFootage','bedrooms','bathrooms','price','propertyType','pricepersqft']].to_excel(writer, 'pricepersqft')


writer.save()


