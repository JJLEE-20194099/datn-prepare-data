

import re
from bs4 import BeautifulSoup
import json
from unidecode import unidecode

with open('streets.json', encoding='utf-8') as f:
   streets = json.load(f)

def search_street(location):
   find_address = []
   location = unidecode(location)
   for street in streets:
      if unidecode(street['STREET']).lower() in location.lower():
         if unidecode(street['WARD']).lower() in location.lower():
            if unidecode(street['DISTRICT']).lower() in location.lower():
               if unidecode(street['CITY']).lower() in location.lower():
                  find_address.append(street)


   if len(find_address) == 0:
      return None

   if len(find_address) == 1:
      return find_address[0]

   if len(find_address) > 1:
      for address_item in find_address:
         # kiem tra ten street, ward, district, city co cap nao trung ten nhau khong
         if address_item['STREET'] == address_item['WARD']:
            dulicate = address_item['STREET']
            address_dulicate = address_item
         if address_item['STREET'] == address_item['DISTRICT']:
            dulicate = address_item['STREET']
            address_dulicate = address_item
         if address_item['STREET'] == address_item['CITY']:
            dulicate = address_item['STREET']
            address_dulicate = address_item
         if address_item['WARD'] == address_item['DISTRICT']:
            dulicate = address_item['WARD']
            address_dulicate = address_item
         if address_item['WARD'] == address_item['CITY']:
            dulicate = address_item['WARD']
            address_dulicate = address_item
         if address_item['DISTRICT'] == address_item['CITY']:
            dulicate = address_item['DISTRICT']
            address_dulicate = address_item


      try:
         # dem so lan xuat hien cua dulicate trong chuoi location
         count = location.lower().count(unidecode(dulicate).lower())
         if count == 2:
            return address_dulicate
         if count == 1:
            if len(find_address) == 2:
               index_duplicate = find_address.index(address_dulicate)
               if index_duplicate == 0:
                  return find_address[1]
               else:
                  return find_address[0]
            else:
               # khi find_address > 2 logic chua xu ly
               return None
      except:
         return None
   return None

def time(a):
   timepost = a['publish_at']
   # 2023-09-13T11:57:14.040178+07:00
   # bỏ milisecond
   timepost = timepost.split('.')[0]
   # 2023-09-13T11:57:14
   return timepost

def description(a):
   soup = BeautifulSoup(a['body'], 'html.parser')
   return soup.get_text()

def certificateOfLandUseRight(a):
   for i in a['parameters']:
      if i['label'] == 'Giấy tờ pháp lý':
         if i['value'] == 'Sổ đỏ' or i['value'] == 'Sổ hồng':
            return { "certificateStatus": "yes","media": [] }
         else:
            return { "certificateStatus": "no","media": [] }
   return { "certificateStatus": "no","media": [] }
def houseInfo(a):
   value = {
               "numberOfFloors": 1,
               "numberOfBedRooms": 0,
               "numberOfBathRooms": 0,
               "numberOfKitchens": 0,
               "numberOfLivingRooms": 0,
               "numberOfGarages": 0
           }
   for i in a['parameters']:
      if i['label'] == 'Số phòng ngủ':
         value['numberOfBedRooms'] = int(i['value'].split(' ')[0])
      if i['label'] == 'Số phòng vệ sinh':
         value['numberOfBathRooms'] = int(i['value'].split(' ')[0])
      if i['label'] == 'Tổng số tầng':
         value['numberOfFloors'] = int(i['value'])
   return value
def propertyBasicInfo(a):
   if a['property_subtype'] == 2519 or 2803 or 2801 or 2508:
      return 'residentialLand'
   if a['property_subtype'] == 2521 or 2522 or 2537:
      return 'urbanResidentialLand'
   if a['property_subtype'] == 2520:
      return 'farmLand'
   return 'residentialLand'
def accessibility(a):
   if a['property_subtype'] == 2522:
      return 'deepInTheAlley'
   else:
      return 'fitOneCarSmall'
def address(a):
   data = {
   "addressDetails": '',
   "street": '',
   "ward": '',
   "district": '',
   "city": '',
   "country": "Việt Nam"
   }
   try:
      data['addressDetails'] = a['ad']['street_number']
   except:
      data['addressDetails'] = None
   if 'address' not in a:
      return None
   if a['address'] == None:
      return None
   location_ = a['address'].lower()
   address_ = search_street(location_)
   if address_ == None:
      return None
   data['street'] = address_['STREET']
   data['ward'] = address_['WARD']
   data['district'] = address_['DISTRICT']
   data['city'] = address_['CITY']
   lat = address_['LAT']
   lon = address_['LNG']
   return [data,[lat,lon]]
def houseDirection(a):
   for i in a['parameters']:
      if 'Hướng cửa chính' in i['label']:
         if i['value'] == 'Đông':
            return 'east'
         elif i['value'] == 'Tây':
            return 'west'
         elif i['value'] == 'Nam':
            return 'south'
         elif i['value'] == 'Bắc':
            return 'north'
         elif i['value'] == 'Đông Nam':
            return 'southeast'
         elif i['value'] == 'Đông Bắc':
            return 'northeast'
         elif i['value'] == 'Tây Nam':
            return 'southwest'
         elif i['value'] == 'Tây Bắc':
            return 'northwest'
   return None
def typeOfRealEstate(a):
   if a['property_subtype'] == 2521:
      return 'shophouse'
   if a['property_subtype'] == 2522:
      return 'privateProperty'
   if a['property_subtype'] == 2537:
      return 'semiDetachedVilla'
   if a['property_type'] == 2536:
      return 'condominium'
   if a['property_subtype'] == 2508:
      return 'projectLand'
   if a['property_subtype'] == 2519:
      return 'privateLand'
   else:
      return 'otherTypesOfProperty'
def frontWidth(a):
   for i in a['parameters']:
      if 'Diện tích' in i['label']:
         # Sử dụng biểu thức chính quy để tìm giá trị frontwidth
         match = re.search(r'\(([\d,]+)x', i['value'])
         if match:
            frontwidth = match.group(1).replace(',', '.')
            return float(frontwidth)
         else:
            return 0
   return 0
def facade(a):
   if '2 mặt' in a['body'].lower() or '2 mặt' in a['title'].lower():
      return 'twoSideOpen'
   elif '3 mặt' in a['body'].lower() or '3 mặt' in a['title'].lower():
      return 'threeSideOpen'
   elif '4 mặt' in a['body'].lower() or '4 mặt' in a['title'].lower():
      return 'fourSideOpen'
   elif typeOfRealEstate(a) == 'townhouse':
      return 'twoSideOpen'
   elif typeOfRealEstate(a) == 'semiDetachedVilla':
      return 'twoSideOpen'
   elif typeOfRealEstate(a) == 'shophouse':
      return 'twoSideOpen'
   else:
      return 'oneSideOpen'
def landSize(a):
   for i in a['attributes']:
      if 'm²' in i['value']:
         try:
            return float(i['value'].replace('m²', '').replace(',', ''))
         except:
            return None


def transferMuaban(a):
   # convert string to json
   a = json.loads(a)
   address_full = address(a)
   if address_full == None:
      return None

   # nếu houseDirection không có thì bỏ qua
   houseDirection_ = houseDirection(a)
   data_merge = {
               "propertyType": 'houseForSale',
               "propertyStatus": "SURVEYING",
               "mediaInfo": { "propertyGeneralImage" : [],
                              "houseTourVideo" : [], # chưa có video
                              "certificateOfLandUseRight": certificateOfLandUseRight(a),
                              "constructionPermit": { "certificateStatus": "no", "media": []  }},
               "houseInfo": { "value": houseInfo(a), "status": "UNSELECTED", "comment": [] },
               "propertyBasicInfo": { "landType": { "comment": [], "status": "UNSELECTED", "value": propertyBasicInfo(a) },
                  "accessibility": { "comment": [], "status": "UNSELECTED", "value": accessibility(a) },
                  "distanceToNearestRoad": { "comment": [], "status": "UNSELECTED", "value": 0 },
                  "frontRoadWidth": { "comment": [], "status": "UNSELECTED", "value": 0 },
                  "address": { "comment": [], "status": "UNSELECTED", "value": address_full[0] },
                  "description": { "comment": [], "status": "UNSELECTED", "value": description(a) },
                  "geolocation": { "comment": [], "status": "UNSELECTED", "value":{
                        "latitude": { "comment": [], "status": "UNSELECTED", "value": address_full[1][0] },
                        "longitude": { "comment": [], "status": "UNSELECTED", "value": address_full[1][1] }}},
                  "typeOfRealEstate": { "comment": [], "status": "UNSELECTED", "value": typeOfRealEstate(a) },
                  "frontWidth": { "comment": [], "status": "UNSELECTED", "value": frontWidth(a) },
                  "endWidth": { "comment": [], "status": "UNSELECTED", "value": frontWidth(a) },
                  "facade": { "comment": [], "status": "UNSELECTED", "value": facade(a) },
                  "houseDirection": { "comment": [], "status": "UNSELECTED", "value": houseDirection_ },
                  "landSize": { "comment": [], "status": "UNSELECTED", "value": landSize(a) },
                  "contact": { "name": { "comment": [], "status": "UNSELECTED", "value": a['contact_name'] },
                     "role": "houseOwner",
                     "phoneNumber": { "comment": [], "status": "UNSELECTED", "value": a['phone'] },
                     "avatarUrl": { "comment": [], "status": "UNSELECTED", "value": ' ' }},
                  "price": { "comment": [], "status": "UNSELECTED", "value": a['price']/1000000000 },
                  "unitPrice": { "comment": [], "status": "UNSELECTED", "value": "billion" },
                  "yearOfConstruction": { "comment": [], "status": "UNSELECTED", "value": None },
                  "saleHistory": { "time": { "comment": [], "status": "UNSELECTED", "value": 0 },
                     "price": { "comment": [], "status": "UNSELECTED", "value": 0 }},
                  "amenities": { "comment": [], "status": "UNSELECTED", "value": {
                        "amenityStatus": False, "bathRoom": [], "bedRoomAndLaundry": [], "kitchenAndDining": [], "others": [], }},
                  "peopleCulturalStandard": { "comment": [], "status": "UNSELECTED", "value": "high" },
                  "publicFacilities": { "comment": [], "status": "UNSELECTED", "value": [] },
                  "downside": { "comment": [], "status": "UNSELECTED", "value": [] }},
               "saleInfo": { "potentialAnalysis": { # ///
                     "marketPrice": { "comment": [], "status": "UNSELECTED", "value": 5000000000 },
                     "expensesForRenovationOrExpansion": { "comment": [],"status": "UNSELECTED","value": 0 },
                     "estimatedProfit": {"comment": [],"status": "UNSELECTED","value": 0 },
                     "threeYearFuturePricePotential": { "comment": [],"status": "UNSELECTED","value": 0 },
                     "fiveYearFuturePricePotential": {"comment": [],"status": "UNSELECTED","value": 0  },
                     "cashFlow": {"comment": [], "status": "UNSELECTED", "value": 0  },
                     "monthlyCashFlow": {"comment": [],"status": "UNSELECTED", "value": 0 },
                     "financialLeverage": {"comment": [],"status": "UNSELECTED","value": 0 },
                     "liquidity": { "comment": [],"status": "UNSELECTED","value": "high" }}},
               "crawlInfo": { "id": str(a['id']), "source" : 's2','time': time(a)}}


   return data_merge