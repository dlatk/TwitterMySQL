import io
import re
import pprint
import time
import MySQLdb
import string
import gzip
import os, sys
import csv
sys.path.append(os.getcwd())
import shapefile
from optparse import OptionParser
import subprocess
from urllib2 import urlopen, URLError, HTTPError
from httplib import BadStatusLine
import getpass
PERMA_PATH = None
for root, dirs, files in os.walk("/home/"+getpass.getuser()):
    if "/PERMA" == root[-6:]:
        PERMA_PATH = root
        break

def warn(string):
    print >>sys.stderr, string

state_to_code = {"VERMONT": "VT", "GEORGIA": "GA", "IOWA": "IA", "Armed Forces Pacific": "AP", "GUAM": "GU", "KANSAS": "KS", "FLORIDA": "FL", "AMERICAN SAMOA": "AS", "NORTH CAROLINA": "NC", "HAWAII": "HI", "NEW YORK": "NY", "CALIFORNIA": "CA", "ALABAMA": "AL", "IDAHO": "ID", "FEDERATED STATES OF MICRONESIA": "FM", "Armed Forces Americas": "AA", "DELAWARE": "DE", "ALASKA": "AK", "ILLINOIS": "IL", "Armed Forces Africa": "AE", "SOUTH DAKOTA": "SD", "CONNECTICUT": "CT", "MONTANA": "MT", "MASSACHUSETTS": "MA", "PUERTO RICO": "PR", "Armed Forces Canada": "AE", "NEW HAMPSHIRE": "NH", "MARYLAND": "MD", "NEW MEXICO": "NM", "MISSISSIPPI": "MS", "TENNESSEE": "TN", "PALAU": "PW", "COLORADO": "CO", "Armed Forces Middle East": "AE", "NEW JERSEY": "NJ", "UTAH": "UT", "MICHIGAN": "MI", "WEST VIRGINIA": "WV", "WASHINGTON": "WA", "MINNESOTA": "MN", "OREGON": "OR", "VIRGINIA": "VA", "VIRGIN ISLANDS": "VI", "MARSHALL ISLANDS": "MH", "WYOMING": "WY", "OHIO": "OH", "SOUTH CAROLINA": "SC", "INDIANA": "IN", "NEVADA": "NV", "LOUISIANA": "LA", "NORTHERN MARIANA ISLANDS": "MP", "NEBRASKA": "NE", "ARIZONA": "AZ", "WISCONSIN": "WI", "NORTH DAKOTA": "ND", "Armed Forces Europe": "AE", "PENNSYLVANIA": "PA", "OKLAHOMA": "OK", "KENTUCKY": "KY", "RHODE ISLAND": "RI", "DISTRICT OF COLUMBIA": "DC", "ARKANSAS": "AR", "MISSOURI": "MO", "TEXAS": "TX", "MAINE": "ME", "U.S. VIRGIN ISLANDS" : "VI"}

stateFipsToStateCode = {
    "01": "AL", "02": "AK","60": "AS","04": "AZ",
    "05": "AR", "06": "CA","08": "CO","09": "CT",
    "10": "DE", "11": "DC","12": "FL","64": "FM",
    "13": "GA", "66": "GU","15": "HI","16": "ID",
    "17": "IL", "18": "IN","19": "IA","20": "KS",
    "21": "KY", "22": "LA","23": "ME","68": "MH",
    "24": "MD", "25": "MA","26": "MI","27": "MN",
    "28": "MS", "29": "MO","30": "MT","31": "NE",
    "32": "NV", "33": "NH","34": "NJ","35": "NM",
    "36": "NY", "37": "NC","38": "ND","69": "MP",
    "39": "OH", "40": "OK","41": "OR","70": "PW",
    "42": "PA", "72": "PR","44": "RI","45": "SC",
    "46": "SD", "47": "TN","48": "TX","74": "UM",
    "49": "UT", "50": "VT","51": "VA","78": "VI",
    "53": "WA", "54": "WV","55": "WI","56": "WY",
}

MAX_ERRORS = 6
ERROR_PAUSE = 10
SQLMANYLIMIT = 8000

code_to_state = {v:k for k, v in state_to_code.items()}

state_to_matches = {}
state_to_compiled_regular_expressions = {}
code_to_timeZone = {}

states_usa = state_to_code.keys()
stateCodes_usa = state_to_code.values()

AAA = "Alaska"
HHH = "Hawaii"
CCC = "Central Time (US & Canada)"
EEE = "Eastern Time (US & Canada)"
MMM = "Mountain Time (US & Canada)"
PPP = "Pacific Time (US & Canada)"
GGG = "Guam"
SSS = "Samoa"
RRR = "Atlantic Time (Canada)"

state_to_timeZone = {'VERMONT': [EEE], 'GEORGIA': [EEE], 'IOWA': [CCC], 'DISTRICT OF COLUMBIA': [EEE], 'GUAM': [GGG], 'KANSAS': [MMM, CCC], 'FLORIDA': [CCC, EEE], 'AMERICAN SAMOA': [SSS], 'NORTH CAROLINA': [EEE], 'ALASKA': [AAA, HHH], 'NEW YORK': [EEE], 'CALIFORNIA': [PPP], 'ALABAMA': [CCC, EEE], 'IDAHO': [MMM, PPP], 'DELAWARE': [EEE], 'HAWAII': [HHH], 'ILLINOIS': [CCC], 'CONNECTICUT': [EEE], 'TENNESSEE': [CCC, EEE], 'MISSOURI': [CCC], 'MASSACHUSETTS': [EEE], 'PUERTO RICO': [RRR], 'OHIO': [EEE], 'MARYLAND': [EEE], 'WASHINGTON': [PPP], 'ARKANSAS': [CCC], 'NEW MEXICO': [MMM], 'SOUTH DAKOTA': [MMM, CCC], 'COLORADO': [MMM], 'NEW JERSEY': [EEE], 'UTAH': [MMM], 'MICHIGAN': [CCC, EEE], 'WYOMING': [MMM], 'MISSISSIPPI': [CCC], 'MINNESOTA': [CCC], 'OREGON': [PPP, MMM], 'VIRGINIA': [EEE], 'SOUTH CAROLINA': [EEE], 'INDIANA': [CCC, EEE], 'NEVADA': [PPP, MMM], 'LOUISIANA': [CCC], 'NORTH DAKOTA': [MMM, CCC], 'NEBRASKA': [MMM, CCC], 'ARIZONA': [MMM], 'WISCONSIN': [CCC], 'PENNSYLVANIA': [EEE], 'OKLAHOMA': [CCC], 'KENTUCKY': [CCC, EEE], 'RHODE ISLAND': [EEE], 'MONTANA': [MMM], 'NEW HAMPSHIRE': [EEE], 'WEST VIRGINIA': [EEE], 'TEXAS': [MMM, CCC], 'MAINE': [EEE]}

def makeCodeToTimezoneDict( code_to_state_dict, state_to_timeZone_dict ):
    code_to_timeZone = {}
    for k, v in code_to_state_dict.items():
        if v in state_to_timeZone_dict.keys():
            code_to_timeZone[k] = state_to_timeZone_dict[v]
    return code_to_timeZone
code_to_timeZone = makeCodeToTimezoneDict( code_to_state, state_to_timeZone )

def makeStateMatchesDict( code_to_state_dict ):
    state_to_matches = {}
    for k, v in code_to_state_dict.items():
        state_to_matches[k] = [k, v]
    return state_to_matches
state_to_matches = makeStateMatchesDict( code_to_state )

def makeStateLookupRegularExpressionDict( state_to_matches_dict ):
    state_to_regExp = {}
    for k, v in state_to_matches_dict.items():
        state_to_regExp[k] = re.compile('\\b(' + '|'.join(v)  + ')\\b', re.I)
    return state_to_regExp
state_to_compiled_regular_expressions = makeStateLookupRegularExpressionDict( state_to_matches )

     
coordRE = re.compile(r'\w{0,8}\s*\:?\s*\b(\-?[0-9]+\.[0-9]+)\s*\,\s*(\-?[0-9]+\.[0-9]+)\b')

#Location Mapping Class#
class LocationMap:
    #gmaps = GoogleMaps(GOOGLE_MAPS_API_KEY)#destination = gmaps.latlng_to_address(38.887563, -77.019929)
    zip_code_data= 'http://www.census.gov/tiger/tms/gazetteer/zips.txt'

    code_to_state = dict((v,k) for k, v in state_to_code.iteritems())
    STATESHAPEFILE = 'state-boundaries/statesp020'

    #instance variables (become instance once overwritten by an instance method):
    geocode = None
    zip_to_data = {}
    stateShapes = {} #set to a dict of 'st' => shape object
    countyShapes = {}

    def __init__(self, directory=None):
        self.directory = directory
        self.loadZipData()

    def loadZipData(self, data_file="/data/twitter/free-zipcode-database.csv"):
        if self.directory:
            data_file = self.directory+'/'+data_file
	else:
	    data_file=PERMA_PATH+data_file
        with open(data_file, 'rb') as read_file:
            csv_reader = csv.reader(read_file)
            for ii, record in enumerate(csv_reader):
                if ii == 0: continue
                ## header: ['RecordNumber', 'Zipcode', 'ZipCodeType', 'City', 'State', 'LocationType', 'Lat', 'Long', 'Xaxis', 'Yaxis', 'Zaxis', 'WorldRegion', 'Country', 'LocationText', 'Location', 'Decommisioned', 'TaxReturnsFiled', 'EstimatedPopulation', 'TotalWages', 'Notes']
                if record[6] and record[7]:
                    self.zip_to_data[record[1]] = {'id' : record[0],
                                                   'state' : record[4],
                                                   'city' : record[3],
                                                   'lat' : float(record[6]),
                                                   'lon' : float(record[7])
                                                   }
                    self.zip_to_data[record[1][:4]] = {'state' : record[2]}
                # import pdb
                # pdb.set_trace()

    def reverseGeocode(self, lat, lon): 
        if not self.geocode:
            self.geocode = geocoders.GeoNames()#import geopy geocoders if using

        count = 0;
        while (True):
            try:
                return self.geocode.reverse((round(float(lat),6),round(float(lon),6)))
            except ValueError:
                return ('', (lat, lon))
            except HTTPError:
                count += 1
                global MAX_ERRORS
                global ERROR_PAUSE
                if (count < MAX_ERRORS): 
                    warn("      ERROR: " + str(sys.exc_info()[0]) + ", " + str(count) + " try, trying again in " + str(ERROR_PAUSE) + " seconds")
                    time.sleep(ERROR_PAUSE)
                else:
                    warn("HTTPError: too many tries, exiting")
                    sys.exit(1);

    def loadLocalFIPSData(self):
        if len(self.countyShapes) < 1:
            sf = shapefile.Reader("/home/maarten/research/PERMA/data/twitter/county-boundaries/tl_2014_us_county")
            fields = sf.fields

            FIPSindex = 0
            stateFIPSindex = 0
            nameIndex = 0
        
            for i in range(len(fields)):
                if (fields[i][0] == 'NAMELSAD'):
                    nameIndex = i-1
                if (fields[i][0] == 'COUNTYFP'):
                    FIPSindex = i-1
                    counties = True
                if (fields[i][0] == 'STATEFP'):
                    stateFIPSindex = i-1
                    counties = True
            srs = sf.shapeRecords()
            count = 0
            for sr in srs:
                fips = sr.record[stateFIPSindex]+sr.record[FIPSindex]
                name = sr.record[nameIndex]
                state = stateFipsToStateCode[fips[:2]]

                try:
                    self.countyShapes[(fips,name,state)].append(sr.shape)
                except KeyError:
                    self.countyShapes[(fips,name,state)] = [sr.shape]


    def loadLocalData(self):
        if len(self.stateShapes) < 1:
            # sf= shapefile.Reader(str(self.directory)+'/'+self.STATESHAPEFILE)
            sf = shapefile.Reader(PERMA_PATH+"/data/twitter/"+self.STATESHAPEFILE) 
            
            
            fields = sf.fields
            stateFieldI = 0
        
            for i in range(len(fields)):
                if (fields[i][0] == 'STATE'):
                    stateFieldI = i-1
                
            srs = sf.shapeRecords()

            for sr in srs:
                scode = sr.record[stateFieldI].strip()
                if (len(scode) > 2): 
                    scode = state_to_code[scode.upper()]
                if scode not in self.stateShapes:
                    self.stateShapes[scode] = []
                self.stateShapes[scode].append(sr.shape)
                #self.stateShapes[sr.record[stateFieldI]] = sr.shape


    def zipToState(self, add):
        """Searches for a zipcode in the string address, and returns the corresponding state"""
        l = re.split(r'[\, ]', add)
        for z in l:
            m = re.match(r'(\d{4})\d', z)#convert to 4 digits - all that is needed to match states
            if m:
                zi = m.group(1)
                if zi in self.zip_to_data:
                    return self.zip_to_data[zi]['state']
        return ''

    def reverseGeocodeLocalFips(self, lat, lon):
        """uses local maps and pip to reverse geocode"""
        self.loadLocalFIPSData()
        
        for fipsTuple, shapes in self.countyShapes.iteritems():
            for shape in shapes:
                if self.pointInBox(lon, lat, shape.bbox):
                    if self.pointInPoly(lon, lat, shape.points):
                        return (fipsTuple, (lat, lon))

        return (None, (lat, lon))


    def reverseGeocodeLocal(self, lat, lon):
        """uses local maps and pip to reverse geocode"""
        self.loadLocalData()
        
        for state, shapes in self.stateShapes.iteritems():
            for shape in shapes:
                if self.pointInBox(lon, lat, shape.bbox):
                    if self.pointInPoly(lon, lat, shape.points):
                        return (state, (lat, lon))

        return (None, (lat, lon))

    def pointInBox(self, x, y, box):
        """checks if a point is in a box"""

        if x >= box[0] and x <= box[2] and y >= box[1] and y <= box[3]:
            #print "     >p in box: ", [x, y], box
            return True
        return False


    def pointInPoly(self, x,y,poly):
        """Checks if a point is in a polygon, thanks to Patrick Jordan"""
        n = len(poly)
        inside =False

        p1x,p1y = poly[0]
        for i in range(n+1):
            p2x,p2y = poly[i % n]
            if y > min(p1y,p2y):
                if y <= max(p1y,p2y):
                    if x <= max(p1x,p2x):
                        if p1y != p2y:
                            xinters = (y-p1y)*(p2x-p1x)/(p2y-p1y)+p1x
                        if p1x == p2x or x <= xinters:
                            inside = not inside
            p1x,p1y = p2x,p2y

        return inside
