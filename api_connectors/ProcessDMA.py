import pandas as pd
import numpy as np
import os
import logging
import datetime
import statsmodels.api as sm


class ProcessDMA():
    def __init__(self, data_directory):
        self.data_directory = data_directory
        self.strt_dt = datetime.date(2018, 2, 1)
        self.end_dt = datetime.date.today()

    def process_population(self, population):
        # add leading zeros to zip codes
        population['zip'] = population['zip'].apply(lambda x: x.zfill(5))

        # fix dma code for one record
        population.loc[population.zip == "98225", ['dma_cd']] = "819"

        # list of female population columns
        female_pop_list = [col for col in population.columns if col.startswith('col_females')]

        # list of household income columns
        hh_income_list = [col for col in population.columns if col.startswith('col_hh')]

        # list of other columns to turn into ints
        feature_cols_list = list(population.columns[27:])

        # change data types from obj to int
        for col in female_pop_list:
            population[col] = population[col].astype(str).astype(int)

        for col in hh_income_list:
            population[col] = population[col].astype(str).astype(int)

        for col in feature_cols_list:
            population[col] = population[col].astype(str).astype(float)

        population['total_female_pop'] = population['total_female_pop'].astype(str).astype(float)
        population['col_pop_2010'] = population['col_pop_2010'].astype(str).astype(float)

        # get population total of women ages 20-64
        # TODO ask SHannon about the columns 20-64 when there are columnns from age 24 - 74
        population['tot_females_20_64'] = population.loc[:, female_pop_list].sum(axis=1)

        # get households who make less than 75k ( col_hh_0_10k, col_hh_10_15k, col_hh_15_25k, col_hh_25_35k,
        #                             col_hh_35_50k, col_hh_50_75k )
        population['income_less_75k'] = population.loc[:, hh_income_list[1:7]].sum(axis=1)

        # get percentage of households in each zip who make less than 75k
        population['low_income_pct'] = population['income_less_75k'] / population['col_hh_est']

        # per each zip, remove the associated percent of low income
        population['tot_females_20_64_more_75k'] = population['tot_females_20_64'] * (1 - population['low_income_pct'])

        logging.info(f"""The total female population is: {population['total_female_pop'].sum()}""")
        logging.info(f"""The total female population ages 20-64 is: {population['tot_females_20_64'].sum()}""")
        logging.info(f"""The total female population ages 20-64 who make >75k is:
        {population['tot_females_20_64_more_75k'].sum()}""")
        # roll up total female high income population per dma
        dma_population = pd.DataFrame(
            population.groupby(['dma'])['tot_females_20_64_more_75k'].agg('sum')).reset_index()

        # account for people who dont shop online
        dma_population['tot_females_20_64_more_75k'] = dma_population['tot_females_20_64_more_75k'] * .9

        # get table of zips tagged to dma
        zip_dma = population[['zip', 'dma', 'dma_cd']]
        # exporting the dataframes to csv
        population.to_csv(os.path.join(self.data_directory, 'population.csv'), sep='|', index=False,
                          encoding='utf-8', quoting=1)
        zip_dma.to_csv(os.path.join(self.data_directory, 'zip_dma.csv'), sep='|', index=False,
                       encoding='utf-8', quoting=1)
        dma_population.to_csv(os.path.join(self.data_directory, 'dma_population.csv'), sep='|', index=False,
                              encoding='utf-8', quoting=1)

    def process_ship_zip_dma(self, ship_zip):
        zip_dma = pd.read_csv(os.path.join(self.data_directory, 'zip_dma.csv'), sep='|', quotechar='"')
        ship_zip['zip_clean'] = np.where(ship_zip['zip'].str.len() == 5,
                                         ship_zip['zip'],
                                         ship_zip['zip'].str[:5])

        # drop small amount of zips coded as 'n/a'
        ship_zip.drop(ship_zip[ship_zip.zip_clean == 'n/a'].index, inplace=True)

        # drop zip with weird characters (anything that not a number)
        ship_zip.drop(ship_zip[ship_zip.zip_clean.str.contains('[^0-9]', regex=True)].index, inplace=True)

        # drop zip column no longer used
        ship_zip = ship_zip.drop(['zip'], axis=1)

        # converting the zip field in  zip_dma to object
        zip_dma.zip = zip_dma.zip.astype(str)

        # attach dma to zip
        ship_zip_dma = ship_zip.merge(zip_dma, left_on='zip_clean', right_on='zip', how='left')

        # drop customers with zips not on census data
        ship_zip_dma.drop(ship_zip_dma[ship_zip_dma.dma.isnull()].index, inplace=True)

        # clean up data
        cust_dmas = ship_zip_dma[
            ['customer_key', 'first_order_month', 'zip_clean', 'dma', 'dma_cd', 'lifetime_net_amount', 'total_orders']]

        cust_dmas.to_csv(os.path.join(self.data_directory, 'cust_dmas.csv'), sep='|', index=False,
                         encoding='utf-8', quoting=1)

    def numerator2(self):
        prospects_agg = pd.read_csv(os.path.join(self.data_directory, 'prospects_agg.csv'), sep='|', quotechar='"')
        shoppers_agg = pd.read_csv(os.path.join(self.data_directory, 'shoppers_agg.csv'), sep='|', quotechar='"')
        prospects_agg.columns = ['month', 'dma', 'count']
        prospects_agg['cust_type'] = 'Prospects Only'

        shoppers_agg.columns = ['month', 'dma', 'count']
        shoppers_agg['cust_type'] = 'Shoppers Only'

        numerator = pd.concat([shoppers_agg, prospects_agg])

        numerator = numerator[numerator['count'] > 0]

        numerator_both = numerator.groupby(['month', 'dma']).sum().reset_index()
        numerator_both['cust_type'] = 'Shoppers + Prospects'

        numerator2 = pd.concat([numerator, numerator_both])

        numerator2 = numerator2.sort_values(['month', 'dma'])

        # change null dma names to DMA not found
        numerator2.loc[numerator2['dma'].isnull(), 'dma'] = "DMA Not Found"

        numerator2.loc[numerator2['dma'] == 'Albany et al, NY', 'dma'] = 'Albany-Schenectady-Troy, NY'
        numerator2.loc[numerator2['dma'] == 'Albany et al, NY', 'dma'] = 'Albany-Schenectady-Troy, NY'
        numerator2.loc[numerator2['dma'] == 'Birmingham et al, AL', 'dma'] = 'Birmingham (Anniston and Tuscaloosa), AL'
        numerator2.loc[numerator2['dma'] == 'Bluefield et al, WV', 'dma'] = 'Bluefield-Beckley-Oak Hill, WV'
        numerator2.loc[numerator2['dma'] == 'Boston et al, MA-NH', 'dma'] = 'Boston, MA (Manchester, NH)'
        numerator2.loc[numerator2['dma'] == 'Burlington et al, VT-NY', 'dma'] = 'Burlington, VT-Plattsburgh, NY'
        numerator2.loc[
            numerator2['dma'] == 'Cedar Rapids et al, IA', 'dma'] = 'Cedar Rapids-Waterloo-Iowa City & Dubuque, IA'
        numerator2.loc[numerator2['dma'] == 'Champaign et al, IL', 'dma'] = 'Champaign & Springfield-Decatur, IL'
        numerator2.loc[numerator2['dma'] == 'Charleston et al, WV', 'dma'] = 'Charleston-Huntington, WV'
        numerator2.loc[numerator2['dma'] == 'Cheyenne et al, WY-NE', 'dma'] = 'Cheyenne, WY-Scottsbluff, NE'
        numerator2.loc[numerator2['dma'] == 'Cleveland et al, OH', 'dma'] = 'Cleveland-Akron (Canton), OH'
        numerator2.loc[numerator2['dma'] == 'Colorado Sprgs et al, CO', 'dma'] = 'Colorado Springs-Pueblo, CO'
        numerator2.loc[numerator2['dma'] == 'Columbia et al, MO', 'dma'] = 'Columbia-Jefferson City, MO'
        numerator2.loc[numerator2['dma'] == 'Columbus et al, MS', 'dma'] = 'Columbus-Tupelo-West Point, MS'
        numerator2.loc[numerator2['dma'] == 'Davenport et al, IA-IL', 'dma'] = 'Davenport, IA-Rock Island-Moline, IL'
        numerator2.loc[numerator2['dma'] == 'Duluth-Superior, MN-WI', 'dma'] = 'Duluth, MN-Superior, WI'
        numerator2.loc[numerator2['dma'] == 'El Paso et al, TX-NM', 'dma'] = 'El Paso, TX'
        numerator2.loc[numerator2['dma'] == 'Elmira et al, NY', 'dma'] = 'Elmira, NY'
        numerator2.loc[numerator2['dma'] == 'Flint-Saginaw et al, MI', 'dma'] = 'Flint-Saginaw-Bay City, MI'
        numerator2.loc[
            numerator2['dma'] == 'Ft. Smith et al, AR', 'dma'] = 'Ft. Smith-Fayetteville-Springdale-Rogers, AR'
        numerator2.loc[numerator2['dma'] == 'Grand Junction et al, CO', 'dma'] = 'Grand Junction-Montrose, CO'
        numerator2.loc[numerator2['dma'] == 'Grand Rapids et al, MI', 'dma'] = 'Grand Rapids-Kalamazoo-Battle Creek, MI'
        numerator2.loc[numerator2['dma'] == 'Greensboro et al, NC', 'dma'] = 'Greensboro-High Point-Winston Salem, NC'
        numerator2.loc[numerator2['dma'] == 'Greenville et al, NC', 'dma'] = 'Greenville-New Bern-Washington, NC'
        numerator2.loc[numerator2['dma'] == 'Greenville et al, SC-NC', 'dma'] = f"""Greenville-Spartanburg,
        SC-Asheville, NC-Anderson,SC""".replace("\n", "")
        numerator2.loc[numerator2['dma'] == 'Harlingen et al, TX', 'dma'] = 'Harlingen-Weslaco-Brownsville-McAllen, TX'
        numerator2.loc[numerator2['dma'] == 'Harrisburg et al, PA', 'dma'] = 'Harrisburg-Lancaster-Lebanon-York, PA'
        numerator2.loc[numerator2['dma'] == 'Huntsville et al, AL', 'dma'] = 'Huntsville-Decatur (Florence), AL'
        numerator2.loc[numerator2['dma'] == 'Idaho Falls et al, ID', 'dma'] = 'Idaho Falls-Pocatello, ID'
        numerator2.loc[numerator2['dma'] == 'Joplin-Pittsburg, MO-KS', 'dma'] = 'Joplin, MO-Pittsburg, KS'
        numerator2.loc[numerator2['dma'] == 'Kansas City, MO-KS', 'dma'] = 'Kansas City, MO'
        numerator2.loc[numerator2['dma'] == 'Lincoln et al, NE', 'dma'] = 'Lincoln & Hastings-Kearney, NE'
        numerator2.loc[numerator2['dma'] == 'Little Rock et al, AR', 'dma'] = 'Little Rock-Pine Bluff, AR'
        numerator2.loc[numerator2['dma'] == 'Medford et al, OR', 'dma'] = 'Medford-Klamath Falls, OR'
        numerator2.loc[numerator2['dma'] == 'Miami-Ft. Lauderdale, FL', 'dma'] = 'Miami-Fort Lauderdale, FL'
        numerator2.loc[numerator2['dma'] == 'Minot et al, ND', 'dma'] = 'Minot-Bismarck-Dickinson(Williston), ND'
        numerator2.loc[
            numerator2['dma'] == 'Mobile et al, AL-FL', 'dma'] = 'Mobile, AL-Pensacola (Ft. Walton Beach), FL'
        numerator2.loc[numerator2['dma'] == 'Monroe-El Dorado, LA-AR', 'dma'] = 'Monroe, LA-El Dorado, AR'
        numerator2.loc[numerator2['dma'] == 'Myrtle Beach et al, SC', 'dma'] = 'Myrtle Beach-Florence, SC'
        numerator2.loc[numerator2['dma'] == 'Norfolk et al, VA', 'dma'] = 'Norfolk-Portsmouth-Newport News, VA'
        numerator2.loc[numerator2['dma'] == 'Orlando et al, FL', 'dma'] = 'Orlando-Daytona Beach-Melbourne, FL'
        numerator2.loc[numerator2['dma'] == 'Ottumwa et al, IA-MO', 'dma'] = 'Ottumwa, IA-Kirksville, MO'
        numerator2.loc[
            numerator2['dma'] == 'Paducah et al, KY-MO-IL', 'dma'] = 'Paducah, KY-Cape Girardeau, MO-Harrisburg, IL'
        numerator2.loc[numerator2['dma'] == 'Phoenix et al, AZ', 'dma'] = 'Phoenix, AZ'
        numerator2.loc[numerator2['dma'] == 'Providence et al, RI-MA', 'dma'] = 'Providence, RI-New Bedford, MA'
        numerator2.loc[numerator2['dma'] == 'Quincy et al, IL-MO-IA', 'dma'] = 'Quincy, IL-Hannibal, MO-Keokuk, IA'
        numerator2.loc[numerator2['dma'] == 'Raleigh et al, NC', 'dma'] = 'Raleigh-Durham (Fayetteville), NC'
        numerator2.loc[numerator2['dma'] == 'Rochester et al, MN-IA', 'dma'] = 'Rochester, MN-Mason City, IA-Austin, MN'
        numerator2.loc[numerator2['dma'] == 'Sacramento et al, CA', 'dma'] = 'Sacramento-Stockton-Modesto, CA'
        numerator2.loc[numerator2['dma'] == 'San Francisco et al, CA', 'dma'] = 'San Francisco-Oakland-San Jose, CA'
        numerator2.loc[
            numerator2['dma'] == 'Santa Barbara et al, CA', 'dma'] = 'Santa Barbara-Santa Maria-San Luis Obispo, CA'
        numerator2.loc[numerator2['dma'] == 'Sherman-Ada, TX-OK', 'dma'] = 'Sherman, TX-Ada, OK'
        numerator2.loc[numerator2['dma'] == 'Sioux Falls et al, SD', 'dma'] = 'Sioux Falls (Mitchell), SD'
        numerator2.loc[numerator2['dma'] == 'Tallahassee et al, FL-GA', 'dma'] = 'Tallahassee, FL-Thomasville, GA'
        numerator2.loc[numerator2['dma'] == 'Tampa et al, FL', 'dma'] = 'Tampa-St. Petersburg (Sarasota), FL'
        numerator2.loc[numerator2['dma'] == 'Traverse City et al, MI', 'dma'] = 'Traverse City-Cadillac, MI'
        numerator2.loc[numerator2['dma'] == 'Tucson(Sierra Vista), AZ', 'dma'] = 'Tucson (Sierra Vista), AZ'
        numerator2.loc[
            numerator2['dma'] == 'Tyler-Longview et al, TX', 'dma'] = 'Tyler-Longview(Lufkin & Nacogdoches), TX'
        numerator2.loc[numerator2['dma'] == 'W. Palm Beach et al, FL', 'dma'] = 'West Palm Beach-Ft. Pierce, FL'
        numerator2.loc[numerator2['dma'] == 'Washington et al, DC-MD', 'dma'] = 'Washington, DC (Hagerstown, MD)'
        numerator2.loc[numerator2['dma'] == 'Wheeling et al, WV-OH', 'dma'] = 'Wheeling, WV-Steubenville, OH'
        numerator2.loc[numerator2['dma'] == 'Wichita Fls et al, TX-OK', 'dma'] = 'Wichita Falls, TX-Lawton, OK'
        numerator2.loc[numerator2['dma'] == 'Wichita et al, KS', 'dma'] = 'Wichita-Hutchinson, KS Plus'
        numerator2.loc[numerator2['dma'] == 'Wilkes Barre et al, PA', 'dma'] = 'Wilkes Barre-Scranton, PA'
        numerator2.loc[numerator2['dma'] == 'Yakima et al, WA', 'dma'] = 'Yakima-Pasco-Richland-Kennewick, WA'
        numerator2.loc[numerator2['dma'] == 'Yuma-El Centro, AZ-CA', 'dma'] = 'Yuma, AZ-El Centro, CA'

        numerator2['last_update_datetime'] = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        numerator2['last_update_datetime'] = pd.to_datetime(numerator2['last_update_datetime'],
                                                            format="%m/%d/%Y %H:%M:%S")

        logging.info("numerator file ready")

        # output numerator file
        numerator2.to_csv(os.path.join(self.data_directory, 'numerator2.csv'), sep='|', index=False,
                          encoding='utf-8')

    def weighted_average(self, df, data_col, weight_col, by_col):
        df['_data_times_weight'] = df[data_col] * df[weight_col]
        df['_weight_where_notnull'] = df[weight_col] * pd.notnull(df[data_col])
        g = df.groupby(by_col)
        result = g['_data_times_weight'].sum() / g['_weight_where_notnull'].sum()
        del df['_data_times_weight'], df['_weight_where_notnull']
        result2 = pd.DataFrame(result).reset_index()
        result3 = result2.rename(columns={0: 'w_avg_' + data_col})
        return result3

    def final_denominator(self):
        population = pd.read_csv(os.path.join(self.data_directory, 'population.csv'), sep='|', quotechar='"')
        dma_population = pd.read_csv(os.path.join(self.data_directory, 'dma_population.csv'), sep='|', quotechar='"')
        numerator2 = pd.read_csv(os.path.join(self.data_directory, 'numerator2.csv'), sep='|', quotechar='"')
        dma_population.columns = ['dma', 'count_pop']

        dma_population.loc[dma_population['dma'] == 'Albany et al, NY', 'dma'] = 'Albany-Schenectady-Troy, NY'
        dma_population.loc[dma_population['dma'] == 'Albany et al, NY', 'dma'] = 'Albany-Schenectady-Troy, NY'
        dma_population.loc[
            dma_population['dma'] == 'Birmingham et al, AL', 'dma'] = 'Birmingham (Anniston and Tuscaloosa), AL'
        dma_population.loc[dma_population['dma'] == 'Bluefield et al, WV', 'dma'] = 'Bluefield-Beckley-Oak Hill, WV'
        dma_population.loc[dma_population['dma'] == 'Boston et al, MA-NH', 'dma'] = 'Boston, MA (Manchester, NH)'
        dma_population.loc[dma_population['dma'] == 'Burlington et al, VT-NY', 'dma'] = 'Burlington, VT-Plattsburgh, NY'
        dma_population.loc[
            dma_population['dma'] == 'Cedar Rapids et al, IA', 'dma'] = 'Cedar Rapids-Waterloo-Iowa City & Dubuque, IA'
        dma_population.loc[
            dma_population['dma'] == 'Champaign et al, IL', 'dma'] = 'Champaign & Springfield-Decatur, IL'
        dma_population.loc[dma_population['dma'] == 'Charleston et al, WV', 'dma'] = 'Charleston-Huntington, WV'
        dma_population.loc[dma_population['dma'] == 'Cheyenne et al, WY-NE', 'dma'] = 'Cheyenne, WY-Scottsbluff, NE'
        dma_population.loc[dma_population['dma'] == 'Cleveland et al, OH', 'dma'] = 'Cleveland-Akron (Canton), OH'
        dma_population.loc[dma_population['dma'] == 'Colorado Sprgs et al, CO', 'dma'] = 'Colorado Springs-Pueblo, CO'
        dma_population.loc[dma_population['dma'] == 'Columbia et al, MO', 'dma'] = 'Columbia-Jefferson City, MO'
        dma_population.loc[dma_population['dma'] == 'Columbus et al, MS', 'dma'] = 'Columbus-Tupelo-West Point, MS'
        dma_population.loc[
            dma_population['dma'] == 'Davenport et al, IA-IL', 'dma'] = 'Davenport, IA-Rock Island-Moline, IL'
        dma_population.loc[dma_population['dma'] == 'Duluth-Superior, MN-WI', 'dma'] = 'Duluth, MN-Superior, WI'
        dma_population.loc[dma_population['dma'] == 'El Paso et al, TX-NM', 'dma'] = 'El Paso, TX'
        dma_population.loc[dma_population['dma'] == 'Elmira et al, NY', 'dma'] = 'Elmira, NY'
        dma_population.loc[dma_population['dma'] == 'Flint-Saginaw et al, MI', 'dma'] = 'Flint-Saginaw-Bay City, MI'
        dma_population.loc[
            dma_population['dma'] == 'Ft. Smith et al, AR', 'dma'] = 'Ft. Smith-Fayetteville-Springdale-Rogers, AR'
        dma_population.loc[dma_population['dma'] == 'Grand Junction et al, CO', 'dma'] = 'Grand Junction-Montrose, CO'
        dma_population.loc[
            dma_population['dma'] == 'Grand Rapids et al, MI', 'dma'] = 'Grand Rapids-Kalamazoo-Battle Creek, MI'
        dma_population.loc[
            dma_population['dma'] == 'Greensboro et al, NC', 'dma'] = 'Greensboro-High Point-Winston Salem, NC'
        dma_population.loc[
            dma_population['dma'] == 'Greenville et al, NC', 'dma'] = 'Greenville-New Bern-Washington, NC'
        dma_population.loc[dma_population['dma'] == 'Greenville et al, SC-NC', 'dma'] = f"""Greenville-Spartanburg,
        SC-Asheville, NC-Anderson,SC""".replace("\n", "")
        dma_population.loc[
            dma_population['dma'] == 'Harlingen et al, TX', 'dma'] = 'Harlingen-Weslaco-Brownsville-McAllen, TX'
        dma_population.loc[
            dma_population['dma'] == 'Harrisburg et al, PA', 'dma'] = 'Harrisburg-Lancaster-Lebanon-York, PA'
        dma_population.loc[dma_population['dma'] == 'Huntsville et al, AL', 'dma'] = 'Huntsville-Decatur (Florence), AL'
        dma_population.loc[dma_population['dma'] == 'Idaho Falls et al, ID', 'dma'] = 'Idaho Falls-Pocatello, ID'
        dma_population.loc[dma_population['dma'] == 'Joplin-Pittsburg, MO-KS', 'dma'] = 'Joplin, MO-Pittsburg, KS'
        dma_population.loc[dma_population['dma'] == 'Kansas City, MO-KS', 'dma'] = 'Kansas City, MO'
        dma_population.loc[dma_population['dma'] == 'Lincoln et al, NE', 'dma'] = 'Lincoln & Hastings-Kearney, NE'
        dma_population.loc[dma_population['dma'] == 'Little Rock et al, AR', 'dma'] = 'Little Rock-Pine Bluff, AR'
        dma_population.loc[dma_population['dma'] == 'Medford et al, OR', 'dma'] = 'Medford-Klamath Falls, OR'
        dma_population.loc[dma_population['dma'] == 'Miami-Ft. Lauderdale, FL', 'dma'] = 'Miami-Fort Lauderdale, FL'
        dma_population.loc[
            dma_population['dma'] == 'Minot et al, ND', 'dma'] = 'Minot-Bismarck-Dickinson(Williston), ND'
        dma_population.loc[
            dma_population['dma'] == 'Mobile et al, AL-FL', 'dma'] = 'Mobile, AL-Pensacola (Ft. Walton Beach), FL'
        dma_population.loc[dma_population['dma'] == 'Monroe-El Dorado, LA-AR', 'dma'] = 'Monroe, LA-El Dorado, AR'
        dma_population.loc[dma_population['dma'] == 'Myrtle Beach et al, SC', 'dma'] = 'Myrtle Beach-Florence, SC'
        dma_population.loc[dma_population['dma'] == 'Norfolk et al, VA', 'dma'] = 'Norfolk-Portsmouth-Newport News, VA'
        dma_population.loc[dma_population['dma'] == 'Orlando et al, FL', 'dma'] = 'Orlando-Daytona Beach-Melbourne, FL'
        dma_population.loc[dma_population['dma'] == 'Ottumwa et al, IA-MO', 'dma'] = 'Ottumwa, IA-Kirksville, MO'
        dma_population.loc[
            dma_population['dma'] == 'Paducah et al, KY-MO-IL', 'dma'] = 'Paducah, KY-Cape Girardeau, MO-Harrisburg, IL'
        dma_population.loc[dma_population['dma'] == 'Phoenix et al, AZ', 'dma'] = 'Phoenix, AZ'
        dma_population.loc[dma_population['dma'] == 'Providence et al, RI-MA', 'dma'] = 'Providence, RI-New Bedford, MA'
        dma_population.loc[
            dma_population['dma'] == 'Quincy et al, IL-MO-IA', 'dma'] = 'Quincy, IL-Hannibal, MO-Keokuk, IA'
        dma_population.loc[dma_population['dma'] == 'Raleigh et al, NC', 'dma'] = 'Raleigh-Durham (Fayetteville), NC'
        dma_population.loc[
            dma_population['dma'] == 'Rochester et al, MN-IA', 'dma'] = 'Rochester, MN-Mason City, IA-Austin, MN'
        dma_population.loc[dma_population['dma'] == 'Sacramento et al, CA', 'dma'] = 'Sacramento-Stockton-Modesto, CA'
        dma_population.loc[
            dma_population['dma'] == 'San Francisco et al, CA', 'dma'] = 'San Francisco-Oakland-San Jose, CA'
        dma_population.loc[
            dma_population['dma'] == 'Santa Barbara et al, CA', 'dma'] = 'Santa Barbara-Santa Maria-San Luis Obispo, CA'
        dma_population.loc[dma_population['dma'] == 'Sherman-Ada, TX-OK', 'dma'] = 'Sherman, TX-Ada, OK'
        dma_population.loc[dma_population['dma'] == 'Sioux Falls et al, SD', 'dma'] = 'Sioux Falls (Mitchell), SD'
        dma_population.loc[
            dma_population['dma'] == 'Tallahassee et al, FL-GA', 'dma'] = 'Tallahassee, FL-Thomasville, GA'
        dma_population.loc[dma_population['dma'] == 'Tampa et al, FL', 'dma'] = 'Tampa-St. Petersburg (Sarasota), FL'
        dma_population.loc[dma_population['dma'] == 'Traverse City et al, MI', 'dma'] = 'Traverse City-Cadillac, MI'
        dma_population.loc[dma_population['dma'] == 'Tucson(Sierra Vista), AZ', 'dma'] = 'Tucson (Sierra Vista), AZ'
        dma_population.loc[
            dma_population['dma'] == 'Tyler-Longview et al, TX', 'dma'] = 'Tyler-Longview(Lufkin & Nacogdoches), TX'
        dma_population.loc[dma_population['dma'] == 'W. Palm Beach et al, FL', 'dma'] = 'West Palm Beach-Ft. Pierce, FL'
        dma_population.loc[
            dma_population['dma'] == 'Washington et al, DC-MD', 'dma'] = 'Washington, DC (Hagerstown, MD)'
        dma_population.loc[dma_population['dma'] == 'Wheeling et al, WV-OH', 'dma'] = 'Wheeling, WV-Steubenville, OH'
        dma_population.loc[dma_population['dma'] == 'Wichita Fls et al, TX-OK', 'dma'] = 'Wichita Falls, TX-Lawton, OK'
        dma_population.loc[dma_population['dma'] == 'Wichita et al, KS', 'dma'] = 'Wichita-Hutchinson, KS Plus'
        dma_population.loc[dma_population['dma'] == 'Wilkes Barre et al, PA', 'dma'] = 'Wilkes Barre-Scranton, PA'
        dma_population.loc[dma_population['dma'] == 'Yakima et al, WA', 'dma'] = 'Yakima-Pasco-Richland-Kennewick, WA'
        dma_population.loc[dma_population['dma'] == 'Yuma-El Centro, AZ-CA', 'dma'] = 'Yuma, AZ-El Centro, CA'
        dma_population['rank_by_TAM'] = dma_population['count_pop'].rank(ascending=False)
        # regroup so levels aren't as defined
        population['attained_hs_or_less'] = population[
            ['col_attained_no_high_school', 'col_attained_some_high_school', 'col_attained_high_school_graduate']].sum(
            axis=1)
        population['attained_associates_or_some_coll'] = population[
            ['col_attained_some_college', 'col_attained_associates']].sum(axis=1)
        # list of new education groups
        education_list = ['attained_hs_or_less', 'attained_associates_or_some_coll', 'col_attained_bachelors',
                          'col_attained_graduate_professional']
        # total education
        population['education_total'] = population[education_list].sum(axis=1)
        # get percent education for each group
        for col in education_list:
            population[col + '_pct'] = population[col] / population['col_pop_2010']

        # list of ethnicty groups
        ethnicity_list = ['col_white', 'col_hispanic', 'col_black', 'col_indian', 'col_asian']
        # total
        population['ethnicty_total'] = population[ethnicity_list].sum(axis=1)
        # get percent ethnicity for each group
        for col in ethnicity_list:
            population[col + '_pct'] = population[col] / population['ethnicty_total']

        # get percent owners and renters
        population['hu_owner_occ_pct'] = population['col_hu_owner_occ'] / population['col_hu_occupied']
        population['hu_renter_occ_pct'] = population['col_hu_renter_occ'] / population['col_hu_occupied']

        # get percent in college
        population['in_college_pct'] = population['col_in_college'] / population['col_pop_2010']
        # what columsn do we want a weighted avg of
        cols_to_rollup = ['col_hh_med_income', 'col_hh_avg_income', 'col_median_age', 'col_hu_med_rent',
                          'col_hu_med_home_value', 'attained_hs_or_less_pct',
                          'attained_associates_or_some_coll_pct', 'col_attained_bachelors_pct',
                          'col_attained_graduate_professional_pct',
                          'col_white_pct', 'col_hispanic_pct', 'col_black_pct', 'col_indian_pct', 'col_asian_pct',
                          'in_college_pct', 'hu_owner_occ_pct']
        # get weighted avg of columns we care about per dma
        weighted_avg_df = population[
            ['dma', 'col_hh_med_income', 'col_hh_avg_income', 'col_median_age', 'col_hu_med_rent',
             'col_hu_med_home_value', 'attained_hs_or_less_pct',
             'attained_associates_or_some_coll_pct', 'col_attained_bachelors_pct',
             'col_attained_graduate_professional_pct',
             'col_white_pct', 'col_hispanic_pct', 'col_black_pct', 'col_indian_pct', 'col_asian_pct', 'in_college_pct',
             'hu_owner_occ_pct']]

        for col in cols_to_rollup:
            wa = self.weighted_average(df=population, data_col=col, weight_col='col_pop_2010', by_col='dma')
            weighted_avg_df = weighted_avg_df.merge(wa, left_on='dma', right_on='dma', how='inner')

        weighted_avg_df = weighted_avg_df.drop(cols_to_rollup, axis=1)
        weighted_avg_df.drop_duplicates(inplace=True)
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Albany et al, NY', 'dma'] = 'Albany-Schenectady-Troy, NY'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Albany et al, NY', 'dma'] = 'Albany-Schenectady-Troy, NY'
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'Birmingham et al, AL', 'dma'] = 'Birmingham (Anniston and Tuscaloosa), AL'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Bluefield et al, WV', 'dma'] = 'Bluefield-Beckley-Oak Hill, WV'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Boston et al, MA-NH', 'dma'] = 'Boston, MA (Manchester, NH)'
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'Burlington et al, VT-NY', 'dma'] = 'Burlington, VT-Plattsburgh, NY'
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'Cedar Rapids et al, IA', 'dma'] = 'Cedar Rapids-Waterloo-Iowa City & Dubuque, IA'
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'Champaign et al, IL', 'dma'] = 'Champaign & Springfield-Decatur, IL'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Charleston et al, WV', 'dma'] = 'Charleston-Huntington, WV'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Cheyenne et al, WY-NE', 'dma'] = 'Cheyenne, WY-Scottsbluff, NE'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Cleveland et al, OH', 'dma'] = 'Cleveland-Akron (Canton), OH'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Colorado Sprgs et al, CO', 'dma'] = 'Colorado Springs-Pueblo, CO'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Columbia et al, MO', 'dma'] = 'Columbia-Jefferson City, MO'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Columbus et al, MS', 'dma'] = 'Columbus-Tupelo-West Point, MS'
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'Davenport et al, IA-IL', 'dma'] = 'Davenport, IA-Rock Island-Moline, IL'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Duluth-Superior, MN-WI', 'dma'] = 'Duluth, MN-Superior, WI'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'El Paso et al, TX-NM', 'dma'] = 'El Paso, TX'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Elmira et al, NY', 'dma'] = 'Elmira, NY'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Flint-Saginaw et al, MI', 'dma'] = 'Flint-Saginaw-Bay City, MI'
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'Ft. Smith et al, AR', 'dma'] = 'Ft. Smith-Fayetteville-Springdale-Rogers, AR'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Grand Junction et al, CO', 'dma'] = 'Grand Junction-Montrose, CO'
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'Grand Rapids et al, MI', 'dma'] = 'Grand Rapids-Kalamazoo-Battle Creek, MI'
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'Greensboro et al, NC', 'dma'] = 'Greensboro-High Point-Winston Salem, NC'
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'Greenville et al, NC', 'dma'] = 'Greenville-New Bern-Washington, NC'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Greenville et al, SC-NC', 'dma'] = f"""Greenville-Spartanburg,
        SC-Asheville, NC-Anderson,SC""".replace("\n", "")
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'Harlingen et al, TX', 'dma'] = 'Harlingen-Weslaco-Brownsville-McAllen, TX'
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'Harrisburg et al, PA', 'dma'] = 'Harrisburg-Lancaster-Lebanon-York, PA'
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'Huntsville et al, AL', 'dma'] = 'Huntsville-Decatur (Florence), AL'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Idaho Falls et al, ID', 'dma'] = 'Idaho Falls-Pocatello, ID'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Joplin-Pittsburg, MO-KS', 'dma'] = 'Joplin, MO-Pittsburg, KS'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Kansas City, MO-KS', 'dma'] = 'Kansas City, MO'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Lincoln et al, NE', 'dma'] = 'Lincoln & Hastings-Kearney, NE'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Little Rock et al, AR', 'dma'] = 'Little Rock-Pine Bluff, AR'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Medford et al, OR', 'dma'] = 'Medford-Klamath Falls, OR'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Miami-Ft. Lauderdale, FL', 'dma'] = 'Miami-Fort Lauderdale, FL'
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'Minot et al, ND', 'dma'] = 'Minot-Bismarck-Dickinson(Williston), ND'
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'Mobile et al, AL-FL', 'dma'] = 'Mobile, AL-Pensacola (Ft. Walton Beach), FL'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Monroe-El Dorado, LA-AR', 'dma'] = 'Monroe, LA-El Dorado, AR'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Myrtle Beach et al, SC', 'dma'] = 'Myrtle Beach-Florence, SC'
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'Norfolk et al, VA', 'dma'] = 'Norfolk-Portsmouth-Newport News, VA'
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'Orlando et al, FL', 'dma'] = 'Orlando-Daytona Beach-Melbourne, FL'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Ottumwa et al, IA-MO', 'dma'] = 'Ottumwa, IA-Kirksville, MO'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Paducah et al, KY-MO-IL', 'dma'] = f"""Paducah, KY-Cape
                                                                                        Girardeau, MO-Harrisburg, IL"""
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Phoenix et al, AZ', 'dma'] = 'Phoenix, AZ'
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'Providence et al, RI-MA', 'dma'] = 'Providence, RI-New Bedford, MA'
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'Quincy et al, IL-MO-IA', 'dma'] = 'Quincy, IL-Hannibal, MO-Keokuk, IA'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Raleigh et al, NC', 'dma'] = 'Raleigh-Durham (Fayetteville), NC'
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'Rochester et al, MN-IA', 'dma'] = 'Rochester, MN-Mason City, IA-Austin, MN'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Sacramento et al, CA', 'dma'] = 'Sacramento-Stockton-Modesto, CA'
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'San Francisco et al, CA', 'dma'] = 'San Francisco-Oakland-San Jose, CA'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Santa Barbara et al, CA', 'dma'] = f"""Santa Barbara-Santa
                                                                                            Maria-San Luis Obispo, CA"""
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Sherman-Ada, TX-OK', 'dma'] = 'Sherman, TX-Ada, OK'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Sioux Falls et al, SD', 'dma'] = 'Sioux Falls (Mitchell), SD'
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'Tallahassee et al, FL-GA', 'dma'] = 'Tallahassee, FL-Thomasville, GA'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Tampa et al, FL', 'dma'] = 'Tampa-St. Petersburg (Sarasota), FL'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Traverse City et al, MI', 'dma'] = 'Traverse City-Cadillac, MI'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Tucson(Sierra Vista), AZ', 'dma'] = 'Tucson (Sierra Vista), AZ'
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'Tyler-Longview et al, TX', 'dma'] = 'Tyler-Longview(Lufkin & Nacogdoches), TX'
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'W. Palm Beach et al, FL', 'dma'] = 'West Palm Beach-Ft. Pierce, FL'
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'Washington et al, DC-MD', 'dma'] = 'Washington, DC (Hagerstown, MD)'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Wheeling et al, WV-OH', 'dma'] = 'Wheeling, WV-Steubenville, OH'
        weighted_avg_df.loc[
            weighted_avg_df['dma'] == 'Wichita Fls et al, TX-OK', 'dma'] = 'Wichita Falls, TX-Lawton, OK'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Wichita et al, KS', 'dma'] = 'Wichita-Hutchinson, KS Plus'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Wilkes Barre et al, PA', 'dma'] = 'Wilkes Barre-Scranton, PA'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Yakima et al, WA', 'dma'] = 'Yakima-Pasco-Richland-Kennewick, WA'
        weighted_avg_df.loc[weighted_avg_df['dma'] == 'Yuma-El Centro, AZ-CA', 'dma'] = 'Yuma, AZ-El Centro, CA'

        # this isnt actually final -- need to do regression to get expected
        final_denom = dma_population.merge(weighted_avg_df, left_on='dma', right_on='dma', how='inner')
        logging.info("running regression")
        # prepare taget variable
        combined_for_regression = numerator2.merge(final_denom, left_on='dma', right_on='dma', how='inner')
        combined_for_regression = combined_for_regression[combined_for_regression['cust_type'] == 'Shoppers Only']
        max_month = combined_for_regression['month'].max()
        combined_for_regression = combined_for_regression[combined_for_regression['month'] == max_month]
        combined_for_regression['shopper_pen'] = combined_for_regression['count'] / combined_for_regression['count_pop']
        combined_for_regression['wa_bachelors_and_graduate_pct'] = \
            combined_for_regression['w_avg_col_attained_bachelors_pct'] + combined_for_regression[
                'w_avg_col_attained_graduate_professional_pct']

        combined_for_regression = combined_for_regression.drop(
            ['dma', 'month', 'cust_type', 'count', 'count_pop', 'rank_by_TAM'], axis=1)
        X = combined_for_regression.drop(['shopper_pen'], axis=1)
        X = X[['w_avg_col_hh_med_income', 'w_avg_col_median_age', 'w_avg_col_hu_med_home_value',
               'w_avg_attained_hs_or_less_pct',
               'wa_bachelors_and_graduate_pct', 'w_avg_col_white_pct']]
        y = combined_for_regression['shopper_pen']

        model = sm.OLS(y, X).fit()
        # add model prediction to denominator df
        expected_penetration = model.predict(X).reset_index()
        expected_penetration = expected_penetration.drop('index', axis=1)
        expected_penetration.columns = ['expected_penetration']

        final_denom2 = pd.concat([final_denom, expected_penetration], axis=1)
        final_denom2['last_update_datetime'] = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        final_denom2['last_update_datetime'] = pd.to_datetime(final_denom2['last_update_datetime'],
                                                              format="%m/%d/%Y %H:%M:%S")
        logging.info("denominator file ready")
        final_denom2.to_csv(os.path.join(self.data_directory, 'final_denom2.csv'), sep='|', index=False,
                            encoding='utf-8')

