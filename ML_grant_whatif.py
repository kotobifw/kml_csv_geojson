from shapely.ops import cascaded_union
from shapely.geometry import *
import time
from multiprocessing import Pool, cpu_count
import multiprocessing
from pymongo import MongoClient
import numpy as np
from concurrent.futures import TimeoutError
from pebble import ProcessPool
from geojson import Feature, FeatureCollection
from operator import itemgetter
import folium
from folium import FeatureGroup, LayerControl
import pandas as pd
from folium import plugins
import csv
import json
import ssl
import random
import glob
from utils.geo_util import compute_area_sqkm
import simplekml
kml = simplekml.Kml()
import collections
from folium.plugins import BeautifyIcon
colors_fill = ['red', 'yellow', 'orange', 'magenta', 'green', 'blue', 'purple',
               'brown', 'gray', 'turquoise', 'cyan']
fill_opacity = 0.3


def style_function(feature):
    col = random.randint(0, len(colors_fill) - 1)
    return {
        'fillColor': colors_fill[col],
        'fillOpacity': fill_opacity,
        'color': colors_fill[col],
        'weight': 3,
    }


def style_function_colordashed(feature):
    col = random.randint(0, len(colors_fill) - 1)
    return {
        'fillColor': colors_fill[col],
        'fillOpacity': fill_opacity,
        'color': colors_fill[col],
        'weight': 3,
        'dashArray': '10, 10'
    }


def style_functionred(feature):
    return {
        'fillColor': 'red',
        'fillOpacity': fill_opacity,
        'color': 'red',
        'weight': 3
    }


def style_function_dashed(feature):
    return {
        'fillColor': 'black',
        'fillOpacity': 0,
        'color': 'black',
        'weight': 4,
        'dashArray': '10, 10'
    }



colors_fill = ['red', 'yellow', 'orange', 'magenta', 'green', 'blue', 'purple',
               'brown', 'gray', 'turquoise', 'cyan']
fill_opacity = 0.3
def style_function(feature):

    col = random.randint(0, len(colors_fill)-1)
    return {
        'fillColor': colors_fill[col],
        'fillOpacity': fill_opacity,
        'color': colors_fill[col],
        'weight': 3,
    }

def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]



def style_function_blue(feature):
    col = 'blue'
    return {
        'fillColor': col,
        'fillOpacity': fill_opacity,
        'color': col,
        'weight': 3,
    }


def style_function_red(feature):
    col = 'red'
    return {
        'fillColor': col,
        'fillOpacity': fill_opacity,
        'color': col,
        'weight': 3,
    }


def style_function_green(feature):
    col = 'black'

    return {
        'fillColor': col,
        'fillOpacity': 0,
        'color': col,
        'weight': 9,
        'dashArray': '10, 10'
    }


def style_function_green2(feature):
    col = 'green'
    return {
        'fillColor': col,
        'fillOpacity': fill_opacity,
        'color': col,
        'weight': 3,
    }

#check if this is a primary or secondary
sasm_mongo_uri="mongodb+srv://engineering:l9yXdOHgPLaw4wIl@comm-prod-01-gr-7rf2r.mongodb.net/comm-prod-01-sasm?readPreference=secondary"
ce_mongo_uri="mongodb+srv://engineering:l9yXdOHgPLaw4wIl@comm-prod-01-gr.7rf2r.mongodb.net/comm-prod-01-ce?readPreference=secondary"

# mongodb+srv:\/\/engineering:l9yXdOHgPLaw4wIl@comm-prod-01-gr.7rf2r.mongodb.net\/comm-prod-01-sasm

sasm_db = MongoClient(sasm_mongo_uri, ssl_cert_reqs=ssl.CERT_NONE).get_database()
ce_db = MongoClient(ce_mongo_uri, ssl_cert_reqs=ssl.CERT_NONE).get_database()
dpa_points_collection = sasm_db['dpa.points']
dpa_coastal_collection = sasm_db['dpa.coastal']
dpa_inland_collection = sasm_db['dpa.points']
# Randy change
# cbsd_incumbent_collection = sasm_db['cbsd.incumbent.local']
cbsd_incumbent_collection = sasm_db['cbsd.incumbent']
# cbsd_registration_collection = sasm_db['cbsd.registrations']
cbsd_registration_collection = sasm_db['cbsd.sas']
cbsd_contour_collection = sasm_db['cbsd.contour']
# cbsd_grant_collection = sasm_db['cbsd.grants']
cbsd_grant_collection = sasm_db['grant.sas']
dpa_collection = ce_db['dpa']
if __name__ == '__main__':

    whatiftests = 2 ##How many CBSDs to plot
    initial_number = 100
    Intersection_threshold = 10
    Intersectionperc_threshold = 0.2

    ##Add what if CBSDs
    with open("Greensboro CBSD locations Jan 22.csv") as fp:
        reader = csv.reader(fp, delimiter=",", quotechar='"')
        next(reader, None)  # skip the headers
        CBSDs = [row for row in reader]

    # ['Site No', 'Latitude', 'Longitude', 'Site Type', 'Height (m)', 'CBSD Category', 'Desirable Operational Power (dBm/10MHz)', 'Sectors', 'Antenna Azimuths', 'Antenna beamwidth (Deg)', 'Antenna Gain (dBi)', 'Coverage radius (m)']

    final_dict_tot = {}

    dpa_ids = ['East1', 'East2', 'East11', 'East10', 'East12', 'East9', 'Norfolk']
    dpa_ids_prob = [0.01, 0.003, 0.003, 0.01, 0.003, 0.01, 0.01]
    dpa_ids_prob = [x / 2 for x in dpa_ids_prob]


    panda_keys_tot = ['Site No', 'Latitude', 'Longitude', 'Antenna Azimuth', 'Antenna beamwidth (Deg)']
    #all_calculated = ['NumberOfOverlappingGrantsChannel1', 'NumberOfOverlappingGrantsChannel2', 'NumberOfOverlappingGrantsChannel3', 'NumberOfOverlappingGrantsChannel4', 'NumberOfOverlappingGrantsChannel5', 'NumberOfOverlappingGrantsChannel6', 'NumberOfOverlappingGrantsChannel7', 'NumberOfOverlappingGrantsChannel8', 'NumberOfOverlappingGrantsChannel9', 'NumberOfOverlappingGrantsChannel10', 'NumberOfOverlappingGrantsChannel11', 'NumberOfOverlappingGrantsChannel12', 'NumberOfOverlappingGrantsChannel13', 'NumberOfOverlappingGrantsChannel14', 'NumberOfOverlappingGrantsChannel15', 'DPAsOnMLChan1', 'DPAsOnMLChan2', 'DPAsOnMLChan3', 'DPAsOnMLChan4', 'DPAsOnMLChan5', 'DPAsOnMLChan6', 'DPAsOnMLChan7', 'DPAsOnMLChan8', 'DPAsOnMLChan9', 'DPAsOnMLChan10']
    #all_calculated = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', 'DPA_1', 'DPA_2', 'DPA_3', 'DPA_4', 'DPA_5', 'DPA_6', 'DPA_7', 'DPA_8', 'DPA_9', 'DPA_10']
    all_calculated = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', 'DPA_1', 'DPA_2', 'DPA_3', 'DPA_4', 'DPA_5', 'DPA_6', 'DPA_7', 'DPA_8', 'DPA_9', 'DPA_10', 'Prob_1', 'Prob_2', 'Prob_3', 'Prob_4', 'Prob_5', 'Prob_6', 'Prob_7', 'Prob_8', 'Prob_9', 'Prob_10']


    panda_keys_tot = panda_keys_tot + all_calculated

    angles = [0, 90, 180, 270]

    devices = []
    reg_request_list_whatif = []
    grant_request_list_whatif = []
    counter = 0
    for cnt, any_CBSD in enumerate(CBSDs):
        if any_CBSD[7] == "1":
            device = {}
            device['cbsdSerialNumber'] = any_CBSD[0]
            device['cbsdCategory'] = any_CBSD[5]
            device['installationParam'] = {}
            device['installationParam']['latitude'] = float(any_CBSD[1])
            device['installationParam']['longitude'] = float(any_CBSD[2])
            device['installationParam']['height'] = float(any_CBSD[4])
            device['installationParam']['indoorDeployment'] = False
            device['installationParam']['heightType'] = "AGL"
            if any_CBSD[8] == "Omni":
                device['installationParam']['antennaAzimuth'] = 360
            else:
                device['installationParam']['antennaAzimuth'] = float(any_CBSD[8])
            if any_CBSD[9] == "Omni":
                device['installationParam']['antennaBeamwidth'] = 360
            else:
                device['installationParam']['antennaBeamwidth'] = float(any_CBSD[9])
            device['installationParam']['antennaGain'] = float(any_CBSD[10])
            device['name'] = any_CBSD[0]
            final_dict_tot[str(any_CBSD[0])] = {}
            final_dict_tot[str(any_CBSD[0])][panda_keys_tot[0]] = str(any_CBSD[0])
            final_dict_tot[str(any_CBSD[0])][panda_keys_tot[1]] = float(any_CBSD[1])
            final_dict_tot[str(any_CBSD[0])][panda_keys_tot[2]] = float(any_CBSD[2])
            final_dict_tot[str(any_CBSD[0])][panda_keys_tot[3]] = device['installationParam']['antennaAzimuth']
            final_dict_tot[str(any_CBSD[0])][panda_keys_tot[4]] = device['installationParam']['antennaBeamwidth']
        else:
            for i in range(int(any_CBSD[7])):
                device = {}
                device['cbsdSerialNumber'] = any_CBSD[0] + '^' + str(angles[i])
                device['cbsdCategory'] = any_CBSD[5]
                device['installationParam'] = {}
                device['installationParam']['latitude'] = float(any_CBSD[1])
                device['installationParam']['longitude'] = float(any_CBSD[2])
                device['installationParam']['height'] = float(any_CBSD[4])
                device['installationParam']['antennaAzimuth'] = angles[i]
                device['installationParam']['antennaBeamwidth'] = float(any_CBSD[9])
                device['installationParam']['antennaGain'] = float(any_CBSD[10])
                device['installationParam']['indoorDeployment'] = False
                device['installationParam']['heightType'] = "AGL"
                device['name'] = any_CBSD[0] + '^' + str(angles[i])
                devices.append(device)
                final_dict_tot[str(any_CBSD[0])+"_"+str(i+1)] = {}
                final_dict_tot[str(any_CBSD[0]) + "_" + str(i + 1)][panda_keys_tot[0]] = str(any_CBSD[0])
                final_dict_tot[str(any_CBSD[0])+"_"+str(i+1)][panda_keys_tot[1]] = float(any_CBSD[1])
                final_dict_tot[str(any_CBSD[0])+"_"+str(i+1)][panda_keys_tot[2]] = float(any_CBSD[2])
                final_dict_tot[str(any_CBSD[0])+"_"+str(i+1)][panda_keys_tot[3]] = device['installationParam']['antennaAzimuth']
                final_dict_tot[str(any_CBSD[0])+"_"+str(i+1)][panda_keys_tot[4]] = device['installationParam']['antennaBeamwidth']

    with open("DenseAir_Contours_names" + ".geojson") as f:
        data = json.load(f)


    my_contours = []
    my_locations = []
    for contour in data["features"]:
        my_contours.append(shape(contour['geometry']))
        my_locations.append([contour['properties']['installationParam']['latitude'],contour['properties']['installationParam']['longitude']])

    AOI = cascaded_union(my_contours)

    query = {"features.0.geometry": {"$geoWithin": {"$geometry": mapping(AOI)}}}

    cbsd_data = list(cbsd_registration_collection.find(query, {'_id': 0, 'features.properties.admin.cbsdId': 1,
                                                               'features.geometry.coordinates': 1}))

    cbsd_ids = []
    for cbsd in cbsd_data:
        cbsd_id = cbsd['features'][0]['properties']['admin']['cbsdId']
        cbsd_ids.append(cbsd_id)

    query = {'features.properties.cbsdId': {'$in': cbsd_ids}}
    cbsd_contours_data = list(cbsd_contour_collection.find(query, {'_id': 0}))

    list_countour_cbsdid = []
    list_countour = []
    for any_contour in cbsd_contours_data:
        list_countour_cbsdid.append(any_contour['features'][0]['properties']['cbsdId'])
        list_countour.append(any_contour['features'][0]['geometry'])



    hmap_PPA = folium.Map([36.0451086,-79.8023057], zoom_start=9, control_scale=True)
    plotdic = {}

    query = {'features.properties.admin.cbsdId': {'$in': list_countour_cbsdid}}
    grant_data = list(cbsd_grant_collection.find(query, {'_id': 0}))
    plotdic_feature = {}
    map_list = []

    grant_dict = {}
    for grant in grant_data:
        flag_grant = 0
        low_freq = grant['features'][0]['properties']['admin']['frequencyRange']['lowFrequency']
        high_freq = grant['features'][0]['properties']['admin']['frequencyRange']['highFrequency']
        if high_freq - low_freq != 10000000.0:
            flag_grant = 1

        cbsd_id = grant['features'][0]['properties']['admin']['cbsdId']
        this_contour = list_countour[list_countour_cbsdid.index(cbsd_id)]
        ## feature
        if [cbsd_id, low_freq] not in map_list:
            if str(low_freq) not in plotdic_feature.keys():
                plotdic_feature[str(low_freq)] = [[this_contour, cbsd_id]]
            else:
                plotdic_feature[str(low_freq)].append([this_contour, cbsd_id])

            map_list.append([cbsd_id, low_freq])
        if flag_grant == 1:
            print("No low freq in 10 MHz steps")


    final_dict = {}
    map_list2 = []

    panda_keys_before = [3550000000.0, 3560000000.0, 3570000000.0, 3580000000.0, 3590000000.0, 3600000000.0, 3610000000.0, 3620000000.0, 3630000000.0, 3640000000.0, 3650000000.0, 3660000000.0, 3670000000.0, 3680000000.0, 3690000000.0]
    panda_keys = ['LF = 3550 MHz', 'LF = 3560 MHz', 'LF = 3570 MHz', 'LF = 3580 MHz', 'LF = 3590 MHz', 'LF = 3600 MHz', 'LF = 3610 MHz', 'LF = 3620 MHz', 'LF = 3630 MHz', 'LF = 3640 MHz', 'LF = 3650 MHz', 'LF = 3660 MHz', 'LF = 3670 MHz', 'LF = 3680 MHz', 'LF = 3690 MHz']

    for cnt, contour in enumerate(data["features"]):
        this_cbsd_name = contour['properties']['name']
        if "^" in this_cbsd_name:
            [this_site, this_az] = this_cbsd_name.split("^")
            the_key = this_site+"_"+str(angles.index(int(this_az))+1)
            for any_key in panda_keys_tot[5:20]:
                final_dict_tot[the_key][any_key] = 0
            for any_key2 in panda_keys_tot[20:]:
                final_dict_tot[the_key][any_key2] = []

        else:
            the_key = this_cbsd_name
            for any_key in panda_keys_tot[5:20]:
                final_dict_tot[the_key][any_key] = 0
            for any_key2 in panda_keys_tot[20:]:
                final_dict_tot[the_key][any_key2] = []

    featgrp_dict_whatif = FeatureGroup(name="What if CBSDs", show=False)
    featgrp_dict_whatif_list = []

    for cnt, contour in enumerate(data["features"]):
        this_cbsd_name = contour['properties']['name']

        if "^" in this_cbsd_name:
            [this_site, this_az] = this_cbsd_name.split("^")
            the_key = this_site+"_"+str(angles.index(int(this_az))+1)
        else:
            this_site = this_cbsd_name
            this_az = ""
            the_key = this_cbsd_name


        if initial_number <= cnt and cnt <= initial_number+ whatiftests:
            #my_locations
            featgrp_dict_whatif = FeatureGroup(name="Site Name_"+this_site+"_Azimuth_"+this_az, show=False)

            folium.GeoJson(contour, name="Site Name_"+this_site+"_Azimuth_"+this_az,overlay=True,
                           style_function=style_function_green, show=False,tooltip  ="Site Name_"+this_site+"_Azimuth_"+this_az).add_to(featgrp_dict_whatif)
            featgrp_dict_whatif_list.append(featgrp_dict_whatif)

            folium.Marker(my_locations[cnt],name= "Site Name_"+this_site+"_Azimuth_"+this_az, show=True).add_to(featgrp_dict_whatif)

        cbsd_ids = []
        for cbsd in cbsd_contours_data:
            if (shape(cbsd['features'][0]['geometry'])).intersects(shape(contour['geometry'])):
                intersected_area =(shape(cbsd['features'][0]['geometry'])).intersection(shape(contour['geometry']))
                cbsd_area = (shape(contour['geometry'])).area
                if compute_area_sqkm(intersected_area) > Intersection_threshold or intersected_area.area / cbsd_area > Intersectionperc_threshold:
                    cbsd_id = cbsd['features'][0]['properties']['cbsdId']
                    cbsd_ids.append(cbsd_id)

        query = {'features.properties.admin.cbsdId': {'$in': cbsd_ids}}
        grant_data = list(cbsd_grant_collection.find(query, {'_id': 0}))

        grant_dict = {}
        for grant in grant_data:
            flag_grant = 0
            low_freq = grant['features'][0]['properties']['admin']['frequencyRange']['lowFrequency']
            high_freq = grant['features'][0]['properties']['admin']['frequencyRange']['highFrequency']
            if high_freq-low_freq != 10000000.0:
                flag_grant = 1

            cbsd_id = grant['features'][0]['properties']['admin']['cbsdId']
            this_contour = list_countour[list_countour_cbsdid.index(cbsd_id)]
            ## feature
            if [cbsd_id, low_freq] not in map_list and 0:
                if initial_number <= cnt and cnt <= initial_number+ whatiftests:
                    if str(cnt) + str(low_freq) not in plotdic_feature.keys():
                        plotdic_feature[str(cnt) + str(low_freq)] = {}
                    if low_freq not in plotdic_feature[str(cnt)+str(low_freq)].keys():
                        plotdic_feature[str(cnt)+str(low_freq)][low_freq] = [[this_contour,cbsd_id]]
                    else:
                        plotdic_feature[str(cnt)+str(low_freq)][low_freq].append([this_contour,cbsd_id])

                map_list.append([cbsd_id, low_freq])

            ##For plotting the html
            if [cbsd_id,low_freq] not in map_list2:
                if initial_number <= cnt and cnt <= initial_number+ whatiftests:
                    mykey = str(grant['features'][0]['geometry']['coordinates'][1])+str(grant['features'][0]['geometry']['coordinates'][0])
                    if mykey not in plotdic.keys():
                        plotdic[mykey] = [[grant['features'][0]['geometry']['coordinates'][1], grant['features'][0]['geometry']['coordinates'][0]],[],[cbsd_id]]
                        plotdic[mykey][1].append(cbsd_id + "^" + str(low_freq))
                    else:
                        plotdic[mykey][1].append(cbsd_id + "^" + str(low_freq))
                        plotdic[mykey][2].append(cbsd_id)

                map_list2.append([cbsd_id,low_freq])

            if flag_grant == 0:
                this_index = int((low_freq-3550000000.0)/10000000.0)
                final_dict_tot[the_key][panda_keys_tot[5+this_index]] += 1

            else:
                print("grant with multiple channels")
                for i in range(high_freq-low_freq/10000000.0):
                    new_low = i * 10000000.0 + low_freq
                    this_index = int((new_low - 3550000000.0) / 10000000.0)
                    final_dict_tot[the_key][panda_keys_tot[5 + this_index]] += 1

    ##plotting the html for testing
    ploted_cbsds = []

    featgrp_dict = {}
    #ordered
    for any_k in panda_keys_before:
        k_name = panda_keys[panda_keys_before.index(float(any_k))]
        featgrp_dict[any_k] = FeatureGroup(name=k_name, show=False)
        these_countours = []
        v = plotdic_feature[str(any_k)]
        for cbsd in v:
            if cbsd[0] not in these_countours:
                these_countours.append(cbsd[0])
                folium.GeoJson(cbsd[0],
                               name=cbsd[1],
                               style_function=style_function, show=False).add_to(featgrp_dict[any_k])
            else:
                folium.GeoJson(shape(cbsd[0]).buffer(random.uniform(0, 0.001)),
                               name=cbsd[1],
                               style_function=style_function, show=False).add_to(featgrp_dict[any_k])

        featgrp_dict[any_k].add_to(hmap_PPA)

    for item in featgrp_dict_whatif_list:
        item.add_to(hmap_PPA)
    folium.LayerControl().add_to(hmap_PPA)
    hmap_PPA.save(outfile='verification_grants'+'.html')

    panda_keys_before = [3550000000.0, 3560000000.0, 3570000000.0, 3580000000.0, 3590000000.0, 3600000000.0,
                         3610000000.0, 3620000000.0, 3630000000.0, 3640000000.0]
    panda_keys = ['LF = 3550 MHz', 'LF = 3560 MHz', 'LF = 3570 MHz', 'LF = 3580 MHz', 'LF = 3590 MHz', 'LF = 3600 MHz',
                  'LF = 3610 MHz', 'LF = 3620 MHz', 'LF = 3630 MHz', 'LF = 3640 MHz']

    for k, v in final_dict_tot.items():
        for i in range(10):
            final_dict_tot[k][panda_keys_tot[30 + i]] = 0

    geojson_files = []
    for file in glob.glob("*.geojson"):
        if "CBSD_MoveList" in file:
            geojson_files.append(file)
            y = file.replace('CBSD_MoveList', '')
            y1 = y.replace('.geojson', '')
            [this_DPA, this_channel_str] = y1.split("^")
            # print(y1)
            # print(this_DPA)
            # print(this_channel_str)
            this_channel = float(this_channel_str)

            with open(file) as f:
                data = json.load(f)

            for cbsd in data["features"]:
                this_cbsd_name = cbsd['properties']['cbsdId']
                if len(this_cbsd_name) < 12:
                    if "^" in this_cbsd_name:
                        [this_site, this_az] = this_cbsd_name.split("^")
                        the_key = this_site + "_" + str(angles.index(int(this_az)) + 1)
                    else:
                        the_key = this_cbsd_name

                    for k, v in cbsd['properties']['MoveList'].items():

                        if v == True:
                            this_index = int((this_channel - 3550000000.0) / 10000000.0)
                            final_dict_tot[the_key][panda_keys_tot[20 + this_index]].append(this_DPA)
                            final_dict_tot[the_key][panda_keys_tot[30 + this_index]] += dpa_ids_prob[dpa_ids.index(this_DPA)]

    #print(len(geojson_files))
    # od_final_dict = collections.OrderedDict(sorted(final_dict.items()))
    pd.options.display.max_rows = None
    pd.set_option('display.max_columns', None)
    pd.set_option('display.expand_frame_repr', False)
    pd.set_option('max_colwidth', -1)
    pd2 = pd.DataFrame.from_dict(final_dict_tot, orient='index')
    # adding a row

    #pd2.index = pd2.index + 1  # shifting index
    #pd2 = pd2.sort_index()  # sorting by index
    pd2.sort_index()

    # cbsd_grants_df = pd2.set_axis(sorted(panda_keys),axis=1, inplace=False)

    cbsd_grants_df = pd2.set_axis(panda_keys_tot, axis=1, inplace=False)
    #cbsd_grants_df.loc[-1] = ['What-if CBSD', '', '', '', '', 'Number of Overlapping Grants/Channel', '', '', '', '', '', '', '', '', '', '', '', '', '', '', 'DPAs for which CBSD is on the ML/Channel', '', '', '', '', '', '', '', '', '']

    cbsd_grants_df.to_csv('cbsd_ML_grants_combined_before.csv')

    ##Add what if CBSDs
    with open("cbsd_ML_grants_combined_before.csv") as fp:
        reader = csv.reader(fp)
        saved = [row for row in reader]
    new = []
    new.append(['What-if CBSD', '', '', '', '','', 'Number of Overlapping Grants/Channel', '', '', '', '', '', '', '', '', '', '', '', '', '', '', 'DPAs for which CBSD is on the ML/Channel', '', '', '', '', '', '', '', '', '','Probability for DPAs for which CBSD is on the ML/Channel', '', '', '', '', '', '', '', '', ''])

    for row in saved:
        new.append(row)

    # Write CSV file
    with open("cbsd_ML_grants_combined.csv", "wt",newline='') as fp:
        writer = csv.writer(fp)
        # writer.writerow(["your", "header", "foo"])  # write header
        writer.writerows(new)


    ##KML

    # open the input geojson file
    in_file = 'DenseAir_Contours_names.geojson'
    with open("DenseAir_Contours_names" + ".geojson") as f:
        data = json.load(f)

    hmap = folium.Map(location=[36.068194, -79.829444], zoom_start=8, control_scale=True, tiles="Stamen Terrain")
    cbsd_info = ['Site No', 'Antenna Azimuth', 'Antenna beamwidth (Deg)']
    probs = ['Prob_1', 'Prob_2', 'Prob_3', 'Prob_4', 'Prob_5', 'Prob_6', 'Prob_7', 'Prob_8', 'Prob_9','Prob_10']
    intersects_list = []
    for i in range(15):
        intersects_list.append(str(i+1))

    my_contours = []
    for contour in data["features"]:
        my_contours.append(shape(contour['geometry']))

    geo_json_data = json.load(open(in_file))
    features = []
    angles = [0, 90, 180, 270]
    for k,v in final_dict_tot.items():

        this_cbsd_name = k
        if "^" in this_cbsd_name:
            [this_site, this_az] = this_cbsd_name.split("^")
        else:
            this_site = this_cbsd_name
            this_az = "Omni"

        folium.Marker([v['Latitude'],v['Longitude']],
                       name=this_cbsd_name, tooltip=this_cbsd_name, show=True).add_to(hmap)

        cbsd_info_str = ""
        for cnt, item in enumerate(cbsd_info):
            cbsd_info_str += item
            cbsd_info_str += ": "
            cbsd_info_str += str(v[item])
            cbsd_info_str += ","
        count = "Grant_Intersection: "
        dpa_prob = "DPA_prob: "
        for cnt, item in enumerate(intersects_list):
            count += str(v[item])
            count += ","
        for cnt, item in enumerate(probs):
            dpa_prob += str(v[item])
            dpa_prob += ","

        pnt = kml.newpoint(name=k, description=cbsd_info_str+count+dpa_prob,
                           coords=[(v['Longitude'], v['Latitude'])])  # lon, lat optional height

    kml.save("DensAir_Marker.kml")

    folium.LayerControl().add_to(hmap)

    hmap.save(outfile="DenseAir_contours_Marker" + '.html')