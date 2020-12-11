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
import geocoder as googlegeocoder
import sys
from sqlalchemy import create_engine

# #######################################################
# # Provide address and radius value from Arcgis REST API.
# #######################################################

#Test Addresses
#Zipcode in 2 counties: 303 Cass Ave, Fairview, MT 59221
#No population: '100 Corrother St, Whitman, NE 69366'
#

address = '100 Corrother St, Whitman, NE 69366'
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
zipcode = google_address.current_result.postal


#######################################################
# Getting data from Arcgis REST API.
#######################################################

# Get comparison data
comparison_variables = variables['comparison_variables']

data = enrich(study_areas=[{"geometry": {"x":x_lon,"y":y_lat}, "areaType":"RingBuffer","bufferUnits":"Miles","bufferRadii":[radius]}],
              analysis_variables=list(comparison_variables.keys()),
              comparison_levels=['US.WholeUSA','US.CBSA','US.Counties','US.Tracts'],
              return_geometry=False)
data = data.drop(columns=['ID', 'apportionmentConfidence', 'OBJECTID', 'areaType', 'bufferUnits', 'bufferUnitsAlias',
                          'bufferRadii', 'aggregationMethod', 'populationToPolygonSizeRating', 'HasData', 'sourceCountry'])
data.to_excel('testdata/TESTDATA_{}.xlsx'.format(address))


# data = pd.read_excel('testdata/TESTDATA_802 E Rose Ave, la habra, ca 90631.xlsx',converters={'StdGeographyID':str})

if 'US.CBSA' in list(data['StdGeographyLevel']):
    msaid = data[data['StdGeographyLevel'] == 'US.CBSA']['StdGeographyID'].iloc[0]
else:
    msaid = ''

countyid = data[data['StdGeographyLevel'] == 'US.Counties']['StdGeographyID'].iloc[0]
stateid = countyid[:2]

#######################################################
#   BEGINNING OF ADJUSTMENT SECTION
#######################################################

#The folling section was added to update 3 data points that ESRI updates once a year.
#These are Unemployment Rate, Median Home Values, and Average Home Values
#To update these, I have created the following script. Here are some common issues
#We need zipcodes to query the ZIP_MacroData_Update table. This table contains values and adjustments for
#all 3 data points. If zipcode is not found in ZIP_MacroData_Update, we will run the 2nd script. This is one
#is much more complex.

with open("./un_pw.json", "r") as file:
    aws_string = json.load(file)['aws_mysql']

data_adjustment = pd.read_sql_query(""" select *
                                        from ZIP_MacroData_Update
                                        where ZIP = '{}' and COUNTYID = '{}' and MSAID = '{}'
                                        """.format(zipcode,countyid,msaid), create_engine(aws_string))


#   Get all of the adjustment values.
if not data_adjustment.empty:
#   If there is a match on the zipcode, run the following script:
    usa_unemployment = data_adjustment['USA_UnemploymentRate'].iloc[0]
    msa_unemployment = data_adjustment['MSA_UnemploymentRate'].iloc[0]
    county_unemployment = data_adjustment['COUNTY_UnemploymentRate'].iloc[0]
    msa_unemployment_adjustment = data_adjustment['MSA_Unemployment_Adjustment'].iloc[0]
    county_unemployment_adjustment = data_adjustment['COUNTY_Unemployment_Adjustment'].iloc[0]
    state_unemployment_adjustment = data_adjustment['STATE_Unemployment_Adjustment'].iloc[0]
    zip_pricechange = data_adjustment['ZIP_PriceChange'].iloc[0]
    msa_pricechange = data_adjustment['MSA_PriceChange'].iloc[0]
    county_pricechange = data_adjustment['COUNTY_PriceChange'].iloc[0]
    usa_pricechange = data_adjustment['USA_PriceChange'].iloc[0]
else:
#   If there is no match on the zipcode, run the following script. This queries the same table, but gets unique values based on
#   each individual unique id:
    zip_adjustment = pd.read_sql_query("""select DISTINCT ZIP, ZIP_PriceChange
                                          from ZIP_MacroData_Update where ZIP = '{}' 
                                            """.format(zipcode), create_engine(aws_string))

    msa_adjustment = pd.read_sql_query("""select DISTINCT MSAID, MSA_PriceChange, MSA_UnemploymentRate, MSA_Unemployment_Adjustment
                                          from ZIP_MacroData_Update where MSAID = '{}' 
                                            """.format(msaid), create_engine(aws_string))

    county_adjustment = pd.read_sql_query("""select DISTINCT COUNTYID, COUNTY_PriceChange, COUNTY_UnemploymentRate, COUNTY_Unemployment_Adjustment
                                            from ZIP_MacroData_Update where COUNTYID = '{}' 
                                            """.format(countyid), create_engine(aws_string))

    state_adjustment = pd.read_sql_query("""select DISTINCT STATEID, STATE_Unemployment_Adjustment , USA_PriceChange, USA_UnemploymentRate
                                            from ZIP_MacroData_Update where STATEID = '{}'
                                            """.format(stateid), create_engine(aws_string))

    usa_unemployment = state_adjustment['USA_UnemploymentRate'].iloc[0]
    msa_unemployment = msa_adjustment['MSA_UnemploymentRate'].iloc[0]
    county_unemployment = county_adjustment['COUNTY_UnemploymentRate'].iloc[0]
    msa_unemployment_adjustment = msa_adjustment['MSA_Unemployment_Adjustment'].iloc[0]
    county_unemployment_adjustment = county_adjustment['COUNTY_Unemployment_Adjustment'].iloc[0]
    state_unemployment_adjustment = state_adjustment['STATE_Unemployment_Adjustment'].iloc[0]
    zip_pricechange = zip_adjustment['ZIP_PriceChange'].iloc[0]
    msa_pricechange = msa_adjustment['MSA_PriceChange'].iloc[0]
    county_pricechange = county_adjustment['COUNTY_PriceChange'].iloc[0]
    usa_pricechange = state_adjustment['USA_PriceChange'].iloc[0]


#   Make sure each record is updated according to the geography (Zip, Tract, Counties, CBSA, USA)
#   We should always have a value for USA. But CBSA and Counties may not always have a value.
for i, row in data.iterrows():
    if row['StdGeographyLevel'] == 'US.WholeUSA':
        data.at[i, 'UNEMPRT_CY'] = usa_unemployment
        data.at[i, 'MEDVAL_CY'] = usa_pricechange * row['MEDVAL_CY']
        data.at[i, 'AVGVAL_CY'] = usa_pricechange * row['AVGVAL_CY']

    elif row['StdGeographyLevel'] == 'US.CBSA':
        if msa_unemployment:
            data.at[i, 'UNEMPRT_CY'] = msa_unemployment
        elif county_unemployment_adjustment:
            data.at[i, 'UNEMPRT_CY'] = math.floor((row['UNEMPRT_CY'] * county_unemployment_adjustment) * 10 ** 1) / 10 ** 1
        else:
            data.at[i, 'UNEMPRT_CY'] = math.floor((row['UNEMPRT_CY'] * state_unemployment_adjustment) * 10 ** 1) / 10 ** 1

        if msa_pricechange:
            data.at[i, 'MEDVAL_CY'] = msa_pricechange * row['MEDVAL_CY']
            data.at[i, 'AVGVAL_CY'] = msa_pricechange * row['AVGVAL_CY']

    elif row['StdGeographyLevel'] == 'US.Counties':
        if county_unemployment:
            data.at[i, 'UNEMPRT_CY'] = county_unemployment
        elif county_unemployment_adjustment:
            data.at[i, 'UNEMPRT_CY'] = math.floor((row['UNEMPRT_CY'] * county_unemployment_adjustment) * 10 ** 1) / 10 ** 1
        else:
            data.at[i, 'UNEMPRT_CY'] = math.floor((row['UNEMPRT_CY'] * state_unemployment_adjustment) * 10 ** 1) / 10 ** 1

        if county_pricechange:
            data.at[i, 'MEDVAL_CY'] = county_pricechange * row['MEDVAL_CY']
            data.at[i, 'AVGVAL_CY'] = county_pricechange * row['AVGVAL_CY']
    else:
        if msa_unemployment_adjustment:
            data.at[i, 'UNEMPRT_CY'] = math.floor((row['UNEMPRT_CY'] * msa_unemployment_adjustment) * 10 ** 1) / 10 ** 1
        elif county_unemployment_adjustment:
            data.at[i, 'UNEMPRT_CY'] = math.floor((row['UNEMPRT_CY'] * county_unemployment_adjustment) * 10 ** 1) / 10 ** 1
        else:
            data.at[i, 'UNEMPRT_CY'] = math.floor((row['UNEMPRT_CY'] * state_unemployment_adjustment) * 10 ** 1) / 10 ** 1

        if zip_pricechange:
            data.at[i, 'MEDVAL_CY'] = zip_pricechange * row['MEDVAL_CY']
            data.at[i, 'AVGVAL_CY'] = zip_pricechange * row['AVGVAL_CY']
        elif msa_pricechange:
            data.at[i, 'MEDVAL_CY'] = msa_pricechange * row['MEDVAL_CY']
            data.at[i, 'AVGVAL_CY'] = msa_pricechange * row['AVGVAL_CY']
        elif county_pricechange:
            data.at[i, 'MEDVAL_CY'] = county_pricechange * row['MEDVAL_CY']
            data.at[i, 'AVGVAL_CY'] = county_pricechange * row['AVGVAL_CY']

#######################################################
#   END OF ADJUSTMENT SECTION
#######################################################



comparison_df = data
comparison_df['StdGeographyName'] = comparison_df['StdGeographyName'].str.replace('Metropolitan Statistical Area','MSA')


# Convert crime index to victims per 100,000 people crime rate.
# These are current national crime rates per 100,000 people. This changes once a year.
# Data found at https://www.statista.com/topics/1750/violent-crime-in-the-us/#dossierSummary__chapter2
crime_index_multiplier = {'CRMCYMURD':5, 'CRMCYROBB':81.6, 'CRMCYRAPE':29.9, 'CRMCYASST':250.2}


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


# Get non comparison data

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

#Drop useless columns
non_comparison_df = data.drop(columns=['ID', 'apportionmentConfidence', 'OBJECTID', 'areaType', 'bufferUnits', 'bufferUnitsAlias',
                          'bufferRadii', 'aggregationMethod', 'populationToPolygonSizeRating', 'HasData', 'sourceCountry'])


# Set Housing Affordability to one of these: Very High, High, Low, Very Low

if data['INCMORT_CY'][0] < 15:
    non_comparison_df['HousingAffordability'] = 'Very Affordable'
elif data['INCMORT_CY'][0] <= 30:
    non_comparison_df['HousingAffordability'] = 'Affordable'
elif data['INCMORT_CY'][0] < 45:
    non_comparison_df['HousingAffordability'] = 'Unaffordable'
else:
    non_comparison_df['HousingAffordability'] = 'Very Unaffordable'


# Calculate owner, renter, vacancy rate by dividing by total housing units
non_comparison_df['OwnerOccupancyRate'] = round(non_comparison_df['OWNER_CY'] / non_comparison_df['TOTHU_CY'] * 100, 2)
non_comparison_df['RenterOccupancyRate'] = round(non_comparison_df['RENTER_CY'] / non_comparison_df['TOTHU_CY'] * 100, 2)
non_comparison_df['VacancyRate'] = round(non_comparison_df['VACANT_CY'] / non_comparison_df['TOTHU_CY'] * 100, 2)
non_comparison_df = non_comparison_df.drop(columns=['OWNER_CY','RENTER_CY','RENTER_CY'])


# Get top 5 Employment Industries
employment_industry_variables = variables['employment_industry_variables']
employment_industry_dict = non_comparison_df[list(employment_industry_variables.keys())].to_dict('records')[0]
employment_industry_dict = {k: v for k, v in sorted(employment_industry_dict.items(), key=lambda item: item[1], reverse=True)}



# Exclude top 5 Employment Industries variables. Index starts at 0
drop_employment_variables = list(employment_industry_dict)[5:]

non_comparison_df = non_comparison_df.drop(columns=drop_employment_variables)


with pd.ExcelWriter('testdata/arcgisoutput.xlsx') as writer:
    non_comparison_df = non_comparison_df.rename(columns=variables['noncomparison_variables'])
    comparison_df = comparison_df.rename(columns=variables['comparison_variables'])
    non_comparison_df.to_excel(writer, sheet_name='noncomparions')
    comparison_df.to_excel(writer, sheet_name='comparison')



######################################################
# Getting rental data from RealtyMole. NOTE: Below is how I made requests to the API. I commented out this section and am
# just using sample data for this example. You can see I stored the response data into "realtymoledata50.json". And I pasted
# the data into "realtymolesampledata.txt"
######################################################

# with open("./un_pw.json", "r") as file:
#     realtymole = json.load(file)['realtymole_gmail']
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

# calculate values for rental summary
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
bedroomscatterplot = bedroomscatterplot[(bedroomscatterplot['bedrooms'] >= 0)]

# calculate z-score for each rent. Anything with z-score > 3 is considered outlier
outliers = []
z_score_threshold = 3
mean_1 = np.mean(bedroomscatterplot['pricepersqft'])
std_1 = np.std(bedroomscatterplot['pricepersqft'])

for i,row in bedroomscatterplot.iterrows():
    if std_1 <= 0:
        z_score = 0
    else:
        z_score = (row['pricepersqft'] - mean_1) / std_1
    if np.abs(z_score) > z_score_threshold:
        bedroomscatterplot.drop(i, inplace=True)

bedroomscatterplot[['formattedAddress','squareFootage','bedrooms','bathrooms','price','propertyType','pricepersqft']].to_excel(writer, 'pricepersqft')


writer.save()


