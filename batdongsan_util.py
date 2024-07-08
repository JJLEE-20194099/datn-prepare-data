from bs4 import BeautifulSoup
import json
import datetime
from unidecode import unidecode
import re
import time
import hashlib

import re
from bs4 import BeautifulSoup
import json
from unidecode import unidecode

from tqdm import tqdm
import regex as re

uniChars = "àáảãạâầấẩẫậăằắẳẵặèéẻẽẹêềếểễệđìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵÀÁẢÃẠÂẦẤẨẪẬĂẰẮẲẴẶÈÉẺẼẸÊỀẾỂỄỆĐÌÍỈĨỊÒÓỎÕỌÔỒỐỔỖỘƠỜỚỞỠỢÙÚỦŨỤƯỪỨỬỮỰỲÝỶỸỴÂĂĐÔƠƯ"
unsignChars = "aaaaaaaaaaaaaaaaaeeeeeeeeeeediiiiiooooooooooooooooouuuuuuuuuuuyyyyyAAAAAAAAAAAAAAAAAEEEEEEEEEEEDIIIOOOOOOOOOOOOOOOOOOOUUUUUUUUUUUYYYYYAADOOU"


def loaddicchar():
    dic = {}
    char1252 = 'à|á|ả|ã|ạ|ầ|ấ|ẩ|ẫ|ậ|ằ|ắ|ẳ|ẵ|ặ|è|é|ẻ|ẽ|ẹ|ề|ế|ể|ễ|ệ|ì|í|ỉ|ĩ|ị|ò|ó|ỏ|õ|ọ|ồ|ố|ổ|ỗ|ộ|ờ|ớ|ở|ỡ|ợ|ù|ú|ủ|ũ|ụ|ừ|ứ|ử|ữ|ự|ỳ|ý|ỷ|ỹ|ỵ|À|Á|Ả|Ã|Ạ|Ầ|Ấ|Ẩ|Ẫ|Ậ|Ằ|Ắ|Ẳ|Ẵ|Ặ|È|É|Ẻ|Ẽ|Ẹ|Ề|Ế|Ể|Ễ|Ệ|Ì|Í|Ỉ|Ĩ|Ị|Ò|Ó|Ỏ|Õ|Ọ|Ồ|Ố|Ổ|Ỗ|Ộ|Ờ|Ớ|Ở|Ỡ|Ợ|Ù|Ú|Ủ|Ũ|Ụ|Ừ|Ứ|Ử|Ữ|Ự|Ỳ|Ý|Ỷ|Ỹ|Ỵ'.split(
        '|')
    charutf8 = "à|á|ả|ã|ạ|ầ|ấ|ẩ|ẫ|ậ|ằ|ắ|ẳ|ẵ|ặ|è|é|ẻ|ẽ|ẹ|ề|ế|ể|ễ|ệ|ì|í|ỉ|ĩ|ị|ò|ó|ỏ|õ|ọ|ồ|ố|ổ|ỗ|ộ|ờ|ớ|ở|ỡ|ợ|ù|ú|ủ|ũ|ụ|ừ|ứ|ử|ữ|ự|ỳ|ý|ỷ|ỹ|ỵ|À|Á|Ả|Ã|Ạ|Ầ|Ấ|Ẩ|Ẫ|Ậ|Ằ|Ắ|Ẳ|Ẵ|Ặ|È|É|Ẻ|Ẽ|Ẹ|Ề|Ế|Ể|Ễ|Ệ|Ì|Í|Ỉ|Ĩ|Ị|Ò|Ó|Ỏ|Õ|Ọ|Ồ|Ố|Ổ|Ỗ|Ộ|Ờ|Ớ|Ở|Ỡ|Ợ|Ù|Ú|Ủ|Ũ|Ụ|Ừ|Ứ|Ử|Ữ|Ự|Ỳ|Ý|Ỷ|Ỹ|Ỵ".split(
        '|')
    for i in range(len(char1252)):
        dic[char1252[i]] = charutf8[i]
    return dic
bang_nguyen_am = [['a', 'à', 'á', 'ả', 'ã', 'ạ', 'a'],
                  ['ă', 'ằ', 'ắ', 'ẳ', 'ẵ', 'ặ', 'aw'],
                  ['â', 'ầ', 'ấ', 'ẩ', 'ẫ', 'ậ', 'aa'],
                  ['e', 'è', 'é', 'ẻ', 'ẽ', 'ẹ', 'e'],
                  ['ê', 'ề', 'ế', 'ể', 'ễ', 'ệ', 'ee'],
                  ['i', 'ì', 'í', 'ỉ', 'ĩ', 'ị', 'i'],
                  ['o', 'ò', 'ó', 'ỏ', 'õ', 'ọ', 'o'],
                  ['ô', 'ồ', 'ố', 'ổ', 'ỗ', 'ộ', 'oo'],
                  ['ơ', 'ờ', 'ớ', 'ở', 'ỡ', 'ợ', 'ow'],
                  ['u', 'ù', 'ú', 'ủ', 'ũ', 'ụ', 'u'],
                  ['ư', 'ừ', 'ứ', 'ử', 'ữ', 'ự', 'uw'],
                  ['y', 'ỳ', 'ý', 'ỷ', 'ỹ', 'ỵ', 'y']]
bang_ky_tu_dau = ['', 'f', 's', 'r', 'x', 'j']

nguyen_am_to_ids = {}

for i in range(len(bang_nguyen_am)):
    for j in range(len(bang_nguyen_am[i]) - 1):
        nguyen_am_to_ids[bang_nguyen_am[i][j]] = (i, j)

dicchar = loaddicchar()
def chuan_hoa_dau_tu_tieng_viet(word):
    if not is_valid_vietnam_word(word):
        return word

    chars = list(word)
    dau_cau = 0
    nguyen_am_index = []
    qu_or_gi = False
    for index, char in enumerate(chars):
        x, y = nguyen_am_to_ids.get(char, (-1, -1))
        if x == -1:
            continue
        elif x == 9:  # check qu
            if index != 0 and chars[index - 1] == 'q':
                chars[index] = 'u'
                qu_or_gi = True
        elif x == 5:  # check gi
            if index != 0 and chars[index - 1] == 'g':
                chars[index] = 'i'
                qu_or_gi = True
        if y != 0:
            dau_cau = y
            chars[index] = bang_nguyen_am[x][0]
        if not qu_or_gi or index != 1:
            nguyen_am_index.append(index)
    if len(nguyen_am_index) < 2:
        if qu_or_gi:
            if len(chars) == 2:
                x, y = nguyen_am_to_ids.get(chars[1])
                chars[1] = bang_nguyen_am[x][dau_cau]
            else:
                x, y = nguyen_am_to_ids.get(chars[2], (-1, -1))
                if x != -1:
                    chars[2] = bang_nguyen_am[x][dau_cau]
                else:
                    chars[1] = bang_nguyen_am[5][dau_cau] if chars[1] == 'i' else bang_nguyen_am[9][dau_cau]
            return ''.join(chars)
        return word

    for index in nguyen_am_index:
        x, y = nguyen_am_to_ids[chars[index]]
        if x == 4 or x == 8:  # ê, ơ
            chars[index] = bang_nguyen_am[x][dau_cau]
            # for index2 in nguyen_am_index:
            #     if index2 != index:
            #         x, y = nguyen_am_to_ids[chars[index]]
            #         chars[index2] = bang_nguyen_am[x][0]
            return ''.join(chars)

    if len(nguyen_am_index) == 2:
        if nguyen_am_index[-1] == len(chars) - 1:
            x, y = nguyen_am_to_ids[chars[nguyen_am_index[0]]]
            chars[nguyen_am_index[0]] = bang_nguyen_am[x][dau_cau]
            # x, y = nguyen_am_to_ids[chars[nguyen_am_index[1]]]
            # chars[nguyen_am_index[1]] = bang_nguyen_am[x][0]
        else:
            # x, y = nguyen_am_to_ids[chars[nguyen_am_index[0]]]
            # chars[nguyen_am_index[0]] = bang_nguyen_am[x][0]
            x, y = nguyen_am_to_ids[chars[nguyen_am_index[1]]]
            chars[nguyen_am_index[1]] = bang_nguyen_am[x][dau_cau]
    else:
        # x, y = nguyen_am_to_ids[chars[nguyen_am_index[0]]]
        # chars[nguyen_am_index[0]] = bang_nguyen_am[x][0]
        x, y = nguyen_am_to_ids[chars[nguyen_am_index[1]]]
        chars[nguyen_am_index[1]] = bang_nguyen_am[x][dau_cau]
        # x, y = nguyen_am_to_ids[chars[nguyen_am_index[2]]]
        # chars[nguyen_am_index[2]] = bang_nguyen_am[x][0]
    return ''.join(chars)


def is_valid_vietnam_word(word):
    chars = list(word)
    nguyen_am_index = -1
    for index, char in enumerate(chars):
        x, y = nguyen_am_to_ids.get(char, (-1, -1))
        if x != -1:
            if nguyen_am_index == -1:
                nguyen_am_index = index
            else:
                if index - nguyen_am_index != 1:
                    return False
                nguyen_am_index = index
    return True


def chuan_hoa_dau_cau_tieng_viet(sentence):

    sentence = sentence.lower()
    words = sentence.split()
    for index, word in enumerate(words):
        cw = re.sub(r'(^\p{P}*)([p{L}.]*\p{L}+)(\p{P}*$)', r'\1/\2/\3', word).split('/')
        # print(cw)
        if len(cw) == 3:
            cw[1] = chuan_hoa_dau_tu_tieng_viet(cw[1])
        words[index] = ''.join(cw)
    return ' '.join(words)
def covert_unicode(txt):
    return re.sub(
        r'à|á|ả|ã|ạ|ầ|ấ|ẩ|ẫ|ậ|ằ|ắ|ẳ|ẵ|ặ|è|é|ẻ|ẽ|ẹ|ề|ế|ể|ễ|ệ|ì|í|ỉ|ĩ|ị|ò|ó|ỏ|õ|ọ|ồ|ố|ổ|ỗ|ộ|ờ|ớ|ở|ỡ|ợ|ù|ú|ủ|ũ|ụ|ừ|ứ|ử|ữ|ự|ỳ|ý|ỷ|ỹ|ỵ|À|Á|Ả|Ã|Ạ|Ầ|Ấ|Ẩ|Ẫ|Ậ|Ằ|Ắ|Ẳ|Ẵ|Ặ|È|É|Ẻ|Ẽ|Ẹ|Ề|Ế|Ể|Ễ|Ệ|Ì|Í|Ỉ|Ĩ|Ị|Ò|Ó|Ỏ|Õ|Ọ|Ồ|Ố|Ổ|Ỗ|Ộ|Ờ|Ớ|Ở|Ỡ|Ợ|Ù|Ú|Ủ|Ũ|Ụ|Ừ|Ứ|Ử|Ữ|Ự|Ỳ|Ý|Ỷ|Ỹ|Ỵ',
        lambda x: dicchar[x.group()], txt)

def preprocess_text(text):
    try:
        text = text.lower()
        text = covert_unicode(text)
        text = chuan_hoa_dau_cau_tieng_viet(text)
        return text
    except:
        return None


hcm_word = preprocess_text('hồ chí minh')
hn_word = preprocess_text('hà nội')

import difflib

def search_street(query, streets, locationql):
    print(query)
    best_matches = difflib.get_close_matches(query, locationql, n=1, cutoff=0.6)
    print(locationql[0])
    if best_matches:
        best_match = best_matches[0]
        best_match_index = locationql.index(best_match)
        best_location = streets[best_match_index]

      #   print(best_location)

        if (hcm_word in query or 'hcm' in query) and best_location['CITY'] == 'Hồ Chí Minh':
            return best_location

        if (hn_word in query or 'hn' in query) and best_location['CITY'] == "Hà Nội":
            return best_location

    else:
        return None

def text_to_slug(text):
   return unidecode(text).replace(' ', '_').lower()
def details_batdongsan(a):
   soup = BeautifulSoup(a, 'html.parser')
   details_ = soup.find_all('div', class_='re__pr-specs-content-item')
   details_ = [item.text.strip() for item in details_]
   json_ = {}
   for item in details_:
      key, value = item.split('\n')
      json_[text_to_slug(key)] = value
   return json_


def certificateOfLandUseRight(a):
   details_ = details_batdongsan(a)
   if 'phap_ly' in details_:
      if 'đỏ' in details_['phap_ly'] or 'hồng' in details_['phap_ly']:
         return { "certificateStatus": "yes","media": [] }
   return { "certificateStatus": "no", "media": [] }

def houseInfo(a):
   value = {   "numberOfFloors": 1,
               "numberOfBedRooms": 0,
               "numberOfBathRooms": 0,
               "numberOfKitchens": 0,
               "numberOfLivingRooms": 0,
               "numberOfGarages": 0 }
   details_ = details_batdongsan(a)
   if 'so_tang' in details_:
      value['numberOfFloors'] = int(details_['so_tang'].replace(' tầng', ''))
   if 'so_phong_ngu' in details_:
      value['numberOfBedRooms'] = int(details_['so_phong_ngu'].replace(' phòng', ''))
   if 'so_toilet' in details_:
      value['numberOfBathRooms'] = int(details_['so_toilet'].replace(' phòng', ''))
   return value

def accessibility(a):
   street_in = details_batdongsan(a)
   if 'duong_vao' in street_in:
      street_meter = float(street_in['duong_vao'].replace('m', '').replace(',', '.'))
      if street_meter <= 2.5:
         return 'theBottleNeckPoint'
      elif street_meter > 2.5 and street_meter <= 3:
         return 'narrorRoad'
      elif street_meter > 3 and street_meter <= 4:
         return 'fitOneCarAndOneMotorbike'
      elif street_meter > 4 and street_meter <= 5:
         return 'parkCar'
      elif street_meter > 5 and street_meter <= 7:
         return 'fitTwoCars'
      elif street_meter > 7:
         return 'fitThreeCars'
   else:
      if 'ba gác' in description(a).lower():
         return 'narrorRoad'

      return 'notInTheAlley'



def address(a, streets, locationql):
   data = {
   "addressDetails": '',
   "street": '',
   "ward": '',
   "district": '',
   "city": '',
   "country": "Việt Nam"
   }
   soup = BeautifulSoup(a, 'html.parser')
   address_ = soup.find('span', class_='re__pr-short-description js__pr-address')
   # print("address_:", address_)
   if address_ == None:
      return None
   address_ = address_.text.strip().lower()
   # print("address_:", address_)
   address_search = search_street(address_, streets, locationql)
   if address_search == None:
         return None
   else:
      data['street'] = address_search['STREET']
      data['ward'] = address_search['WARD']
      data['district'] = address_search['DISTRICT']
      data['city'] = address_search['CITY']
      lat = address_search['LAT']
      lng = address_search['LNG']
      return [data, lat, lng]


def description(a):
   soup = BeautifulSoup(a, 'html.parser')
   bio_ = soup.find('div', class_='re__section-body re__detail-content js__section-body js__pr-description js__tracking')
   if bio_ == None:
      return ''
   # kiem tra co bao nhieu the <br> trong description

   return bio_.get_text('\n').strip()

def title(a):
    try:
        soup = BeautifulSoup(a, 'html.parser')

        title_ = soup.title.string
        return title_
    except:
        return ""
def link(a):
   soup = BeautifulSoup(a, 'html.parser')
   link_ = soup.find('meta', property="og:url")
   if link_ == None:
      return ''
   return link_['content']

def typeOfRealEstate(a):
   soup = BeautifulSoup(a, 'html.parser')
   property_ = soup.find('a', class_='re__link-se')
   property_ = property_['href']
   if 'ban-nha-rieng' in property_:
      return 'privateProperty'
   if 'ban-can-ho-chung-cu' in property_:
      return 'condominium'
   if 'ban-dat' in property_:
      return 'privateLand'
   if 'ban-nha-biet-thu-lien-ke' in property_:
      return 'semiDetachedVilla'
   if 'ban-nha-mat-pho' in property_:
      return 'townhouse'
   if 'trang-trai' in property_:
      return 'resort'
   if 'ban-shophouse' in property_:
      return 'shophouse'
   else:
      return 'otherTypesOfProperty'

def frontWidth(a):
   details_ = details_batdongsan(a)
   if 'mat_tien' in details_:
      return float(details_['mat_tien'].replace('m', '').replace(',', '.'))
   else:
      return 0


def facade(a):
   bio_ = description(a)
   if '2 mặt' in bio_.lower():
      return 'twoSideOpen'
   elif '3 mặt' in bio_.lower():
      return 'threeSideOpen'
   elif '4 mặt' in bio_.lower():
      return 'fourSideOpen'
   elif typeOfRealEstate(a) == 'townhouse':
      return 'twoSideOpen'
   elif typeOfRealEstate(a) == 'semiDetachedVilla':
      return 'twoSideOpen'
   elif typeOfRealEstate(a) == 'shophouse':
      return 'twoSideOpen'
   else:
      return 'oneSideOpen'

def houseDirection(a):
   details_ = details_batdongsan(a)
   if 'huong_nha' in details_:
      if 'Tây - Bắc' in details_['huong_nha']:
         return 'northwest'
      elif 'Tây - Nam' in details_['huong_nha']:
         return 'southwest'
      elif 'Đông - Nam' in details_['huong_nha']:
         return 'southeast'
      elif 'Đông - Bắc' in details_['huong_nha']:
         return 'northeast'
      elif 'Tây' in details_['huong_nha']:
         return 'west'
      elif 'Đông' in details_['huong_nha']:
         return 'east'
      elif 'Nam' in details_['huong_nha']:
         return 'south'
      elif 'Bắc' in details_['huong_nha']:
         return 'north'
      else:
         return None
   else:
      return None

def landSize(a):
   details_ = details_batdongsan(a)
   if 'dien_tich' in details_:
      try:
         return float(details_['dien_tich'].replace('m²', '').replace('.', '').replace(',', '.'))
      except:
         return None

def name(a):
   soup = BeautifulSoup(a, 'html.parser')
   name_ = soup.find('div', class_='re__contact-name js_contact-name')
   if name_ == None:
      return None
   else:
      name_ = name_.text.strip()
   return name_

def numberPhone(a):
   soup = BeautifulSoup(a, 'html.parser')
   phone = soup.find('div', class_='re__btn re__btn-cyan-solid--md phone js__phone phoneEvent js__phone-event showHotline tooltip')
   if phone == None:
      phone = soup.find('div', class_='re__btn re__btn-cyan-solid--md phone js__phone phoneEvent js__phone-event')
      if phone == None:
         return None
      phone = phone.text.strip()
      phone = phone.replace(' ', '')
      phone = phone.replace('·Hiệnsố', '')
      return phone
   else:
      return phone['mobile'].replace(' ', '')

def avatarUrl(a):
   soup = BeautifulSoup(a, 'html.parser')
   avatar = soup.find('img', class_='re__contact-avatar')
   if avatar == None:
      return None
   else:
      return avatar['src']

def price(a):
   try:
      soup = BeautifulSoup(a, 'html.parser')
      price_ = soup.find('div', class_='re__pr-short-info-item js__pr-short-info-item')
      price_ = price_.find('span', class_='value').text.strip()
      if 'tỷ' in price_:
         price_ = float(price_.replace(' tỷ', '').replace(',', '.'))
      elif 'triệu' in price_:
         price_ = float(price_.replace(' triệu', '').replace(',', '.'))/1000
      return float(price_)
   except:
      soup = BeautifulSoup(a, 'html.parser')
      price_ = soup.find('div', class_='re__pr-short-info-item js__pr-short-info-item')
      price_ = price_.find('span', class_='value').text.strip()
      if 'thuận' in price_.lower():
         return 0
      if '/m²' in price_:
         price_ = price_.replace('/m²', '')
         if 'tỷ' in price_:
            price_ = float(price_.replace(' tỷ', '').replace(',', '.'))*landSize(a)
         elif 'triệu' in price_:
            price_ = float(price_.replace(' triệu', '').replace(',', '.'))*landSize(a)/1000
         return float(price_)
      return 0

def amenities(a):
   details_ = details_batdongsan(a)
   if 'noi_that' in details_:
      return True
   else:
      return False

def id(a):
   soup = BeautifulSoup(a, 'html.parser')
   id_ = soup.find_all('div', class_='re__pr-short-info-item js__pr-config-item')
   for idz in id_:
      if 'Mã tin' in idz.text:
         return idz.text.replace('Mã tin', '').strip()

def time(a):
   soup = BeautifulSoup(a, 'html.parser')
   date = soup.find_all('div', class_='re__pr-short-info-item js__pr-config-item')
   date = [item.text.strip() for item in date]
   date = date[0].replace('Ngày đăng', '')
   date = datetime.datetime.strptime(date, '%d/%m/%Y')
   return date.strftime('%Y-%m-%dT%H:%M:%S')


def transferBatdongsan(a, streets, locationql, get_all = False):

   #  print("LOCALTON_QL:", locationql[0])
    address_ = address(a, streets, locationql)
   #  print(address_)
    if address_ == None:
        return None

    city = address_[0]['city']
    if get_all == False and city not in ["Hà Nội", "Hồ Chí Minh"]:
        return None

    houseDirection_ = houseDirection(a)
    price_ = price(a)
    if price_ == None:
        price_ = 0
    landSize_ = landSize(a)
    if landSize_ == None:
        return None


    data_merge = {
                "propertyType": 'houseForSale',
                "propertyStatus": "SURVEYING",
                "mediaInfo": {
                                "certificateOfLandUseRight": certificateOfLandUseRight(a),
                                },
                "houseInfo": { "value": houseInfo(a), "status": "UNSELECTED", "comment": [] },
                "propertyBasicInfo": { "landType": {  "value": 'residentialLand' },
                    "accessibility": {  "value": accessibility(a) },
                    "distanceToNearestRoad": {  "value": 0 },
                    "frontRoadWidth": {  "value": 0 },
                    "address": {  "value": address_[0] },
                    "description": {  "value": description(a) },
                    "title": {  "value": title(a) },
                    "geolocation": {  "value":{
                            "latitude": {  "value": address_[1] },
                            "longitude": {  "value": address_[2] }}},
                    "typeOfRealEstate": {  "value": typeOfRealEstate(a) },
                    "frontWidth": {  "value": frontWidth(a) },
                    "endWidth": {  "value": 0 },
                    "facade": {  "value": facade(a) },
                    "houseDirection": {  "value": houseDirection_ },
                    "landSize": {  "value": landSize_ },

                    "price": {  "value": price_ },
                    "unitPrice": {  "value": "billion" },
                    },
                "saleInfo": { },
                "crawlInfo": { "id": id(a), "source" : 'batdongsan','time' : time(a),'sourceUrl': link(a)}}


    return data_merge