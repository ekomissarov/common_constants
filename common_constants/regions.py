import re
import requests
from common_constants import constants
from urllib3.exceptions import ProtocolError
from http.client import RemoteDisconnected
from socket import timeout
from time import sleep


ENVI = constants.EnviVar(
    main_dir="/home/eugene/Yandex.Disk/localsource/common_constants/common_constants/",
    cred_dir="/home/eugene/Yandex.Disk/localsource/credentials/"
)
logger = constants.logging.getLogger(__name__)


class IntegrityDataError(constants.PySeaError): pass
class LimitOfRetryError(constants.PySeaError): pass


def connection_attempts(n=12, t=10):  # конструктор декоратора (N,T залипает в замыкании)
    """
    Декоратор задает n попыток для соединения с сервером в случае ряда исключений
    с задержкой t*2^i секунд

    :param n: количество попыток соединения с сервером [1, 15]
    :param t: количество секунд задержки на первой попытке попытке (на i'ом шаге t*2^i)
    :return:
    """
    def deco_connect(f):  # собственно декоратор принимающий функцию для декорирования
        def constructed_function(*argp, **argn):  # конструируемая функция
            retry_flag, pause_seconds = n, t
            try_number = 0

            if retry_flag < 0 or retry_flag > 15:
                retry_flag = 8
            if pause_seconds < 1 or pause_seconds > 30:
                pause_seconds = 10

            while True:
                try:
                    result = f(*argp, **argn)
                    # Обработка ошибки, если не удалось соединиться с сервером
                except (ConnectionError,
                        requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError,
                        ProtocolError, RemoteDisconnected, timeout) as err:
                    logger.error(f"Ошибка соединения с сервером {err}. Осталось попыток {retry_flag - try_number}")
                    if try_number >= retry_flag:
                        raise LimitOfRetryError
                    sleep(pause_seconds * 2 ** try_number)
                    try_number += 1
                    continue
                else:
                    return result

            return None
        return constructed_function
    return deco_connect


class CianRegions:
    ptr_domain = re.compile('(^|http://|https://)([a-zA-Z\-]+\.cian\.ru)')  # RE_DOMAIN
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36 ekom@cian.ru'}
    fields = ['reg', 'campaign_name_prefix', 'city', 'in_city', 'obl', 'in_obl', 'domain', 'geo_map',
              'yandex_geo', 'region_id', 'obl_id', 'analytics_geo_regex', 'obl_seourl_postfix', 'obl_seourl_domain']
    data = [
        {
            'reg': 'ekb',
            'campaign_name_prefix': ['ekb'],
            'city': 'Екатеринбург',
            'in_city': 'в Екатеринбурге',
            'obl': 'Свердловская область',
            'in_obl': 'в Свердловской области',
            'domain': 'ekb.cian.ru',
            'geo_map': '&center=56.761342,60.574555&zoom=9',
            'yandex_geo': 'Свердловская область',
            'region_id': '4743',
            'obl_id': '4612',
            'analytics_geo_regex': '^Sverdlovsk Oblast$',
            'obl_seourl_postfix': 'sverdlovskaya-oblast/',
            'obl_seourl_domain': 'ekb.cian.ru',
        },
        {
            'reg': 'novosibirsk',
            'campaign_name_prefix': ['novosibirsk'],
            'city': 'Новосибирск',
            'in_city': 'в Новосибирске',
            'obl': 'Новосибирская область',
            'in_obl': 'в Новосибирской области',
            'domain': 'novosibirsk.cian.ru',
            'geo_map': '&center=55.000683,82.956277&zoom=10',
            'yandex_geo': 'Новосибирская область',
            'region_id': '4897',
            'obl_id': '4598',
            'analytics_geo_regex': '^Novosibirsk Oblast$',
            'obl_seourl_postfix': 'novosibirskaya-oblast/',
            'obl_seourl_domain': 'novosibirsk.cian.ru',
        },
        {
            'reg': 'voronezh',
            'campaign_name_prefix': ['voronezh'],
            'city': 'Воронеж',
            'in_city': 'в Воронеже',
            'obl': 'Воронежская область',
            'in_obl': 'в Воронежской области',
            'domain': 'voronezh.cian.ru',
            'geo_map': '&center=51.694273,39.335955&zoom=10',
            'yandex_geo': 'Воронежская область',
            'region_id': '4713',
            'obl_id': '4567',
            'analytics_geo_regex': '^Voronezh Oblast$',
            'obl_seourl_postfix': 'voronezhskaya-oblast/',
            'obl_seourl_domain': 'voronezh.cian.ru',
        },
        {
            'reg': 'chelyabinsk',
            'campaign_name_prefix': ['chelyabinsk'],
            'city': 'Челябинск',
            'in_city': 'в Челябинске',
            'obl': 'Челябинская область',
            'in_obl': 'в Челябинской области',
            'domain': 'chelyabinsk.cian.ru',
            'geo_map': '&center=55.153362,61.391698&zoom=10',
            'yandex_geo': 'Челябинская область',
            'region_id': '5048',
            'obl_id': '4630',
            'analytics_geo_regex': '^Chelyabinsk Oblast$',
            'obl_seourl_postfix': 'chelyabinskaya-oblast/',
            'obl_seourl_domain': 'chelyabinsk.cian.ru',
        },
        {
            'reg': 'krasnoyarsk',
            'campaign_name_prefix': ['krasnoyarsk'],
            'city': 'Красноярск',
            'in_city': 'в Красноярске',
            'obl': 'Красноярский край',
            'in_obl': 'в Красноярском крае',
            'domain': 'krasnoyarsk.cian.ru',
            'geo_map': '&center=56.022798,92.897429&zoom=11',
            'yandex_geo': 'Красноярский край',
            'region_id': '4827',
            'obl_id': '4585',
            'analytics_geo_regex': '^Krasnoyarsk Krai$',
            'obl_seourl_postfix': 'krasnoyarskiy-kray/',
            'obl_seourl_domain': 'krasnoyarsk.cian.ru',
        },
        {
            'reg': 'perm',
            'campaign_name_prefix': ['perm'],
            'city': 'Пермь',
            'in_city': 'в Перми',
            'obl': 'Пермский край',
            'in_obl': 'в Пермском крае',
            'domain': 'perm.cian.ru',
            'geo_map': '&center=58.022833,56.229420&zoom=10',
            'yandex_geo': 'Пермский край',
            'region_id': '4927',
            'obl_id': '4603',
            'analytics_geo_regex': '^Perm Krai$',
            'obl_seourl_postfix': 'permskiy-kray/',
            'obl_seourl_domain': 'perm.cian.ru',
        },
        {
            'reg': 'omsk',
            'campaign_name_prefix': ['omsk'],
            'city': 'Омск',
            'in_city': 'в Омске',
            'obl': 'Омская область',
            'in_obl': 'в Омской области',
            'domain': 'omsk.cian.ru',
            'geo_map': '&center=55.012435,73.372937&zoom=9',
            'yandex_geo': 'Омская область',
            'region_id': '4914',
            'obl_id': '4599',
            'analytics_geo_regex': '^Omsk Oblast$',
            'obl_seourl_postfix': 'omskaya-oblast/',
            'obl_seourl_domain': 'omsk.cian.ru',
        },
        {
            'reg': 'saratov',
            'campaign_name_prefix': ['saratov'],
            'city': 'Саратов',
            'in_city': 'в Саратове',
            'obl': 'Саратовская область',
            'in_obl': 'в Саратовской области',
            'domain': 'saratov.cian.ru',
            'geo_map': '&center=51.540041,46.004441&zoom=10',
            'yandex_geo': 'Саратовская область',
            'region_id': '4969',
            'obl_id': '4609',
            'analytics_geo_regex': '^Saratov Oblast$',
            'obl_seourl_postfix': 'saratovskaya-oblast/',
            'obl_seourl_domain': 'saratov.cian.ru',
        },
        {
            'reg': 'tyumen',
            'campaign_name_prefix': ['tyumen'],
            'city': 'Тюмень',
            'in_city': 'в Тюмени',
            'obl': 'Тюменская область',
            'in_obl': 'в Тюменской области',
            'domain': 'tyumen.cian.ru',
            'geo_map': '&center=57.137405,65.544996&zoom=10',
            'yandex_geo': 'Тюменская область',
            'region_id': '5024',
            'obl_id': '4623',
            'analytics_geo_regex': '^Tyumen Oblast$',
            'obl_seourl_postfix': 'tyumenskaya-oblast/',
            'obl_seourl_domain': 'tyumen.cian.ru',
        },
        {
            'reg': 'volgograd',
            'campaign_name_prefix': ['volgograd'],
            'city': 'Волгоград',
            'in_city': 'в Волгограде',
            'obl': 'Волгоградская область',
            'in_obl': 'в Волгоградской области',
            'domain': 'volgograd.cian.ru',
            'geo_map': '&center=48.650812,44.399175&zoom=10',
            'yandex_geo': 'Волгоградская область',
            'region_id': '4704',
            'obl_id': '4565',
            'analytics_geo_regex': '^Volgograd Oblast$',
            'obl_seourl_postfix': 'volgogradskaya-oblast/',
            'obl_seourl_domain': 'volgograd.cian.ru',
        },
        {
            'reg': 'irkutsk',
            'campaign_name_prefix': ['irkutsk'],
            'city': 'Иркутск',
            'in_city': 'в Иркутске',
            'obl': 'Иркутская область',
            'in_obl': 'в Иркутской области',
            'domain': 'irkutsk.cian.ru',
            'geo_map': '&center=52.287141,104.279197&zoom=11',
            'yandex_geo': 'Иркутская область',
            'region_id': '4774',
            'obl_id': '4572',
            'analytics_geo_regex': '^Irkutsk Oblast$',
            'obl_seourl_postfix': 'irkutskaya-oblast/',
            'obl_seourl_domain': 'irkutsk.cian.ru',
        },
        {
            'reg': 'stavropol',
            'campaign_name_prefix': ['stavropol'],
            'city': 'Ставрополь',
            'in_city': 'в Ставрополе',
            'obl': 'Ставропольский край',
            'in_obl': 'в Ставропольском крае',
            'domain': 'stavropol.cian.ru',
            'geo_map': '&center=45.063966,41.974589&zoom=11',
            'yandex_geo': 'Ставропольский край',
            'region_id': '5001',
            'obl_id': '4615',
            'analytics_geo_regex': '^Stavropol Krai$',
            'obl_seourl_postfix': 'stavropolskiy-kray/',
            'obl_seourl_domain': 'stavropol.cian.ru',
        },
        {
            'reg': 'habarovsk',
            'campaign_name_prefix': ['habarovsk'],
            'city': 'Хабаровск',
            'in_city': 'в Хабаровске',
            'obl': 'Хабаровский край',
            'in_obl': 'в Хабаровском крае',
            'domain': 'habarovsk.cian.ru',
            'geo_map': '&center=48.468976,135.112974&zoom=11',
            'yandex_geo': 'Хабаровский край',
            'region_id': '5039',
            'obl_id': '4627',
            'analytics_geo_regex': '^Khabarovsk Krai$',
            'obl_seourl_postfix': 'habarovskiy-kray/',
            'obl_seourl_domain': 'habarovsk.cian.ru',
        },
        {
            'reg': 'tula',
            'campaign_name_prefix': ['tula'],
            'city': 'Тула',
            'in_city': 'в Туле',
            'obl': 'Тульская область',
            'in_obl': 'в Тульской области',
            'domain': 'tula.cian.ru',
            'geo_map': '&center=54.182217,37.619027&zoom=10',
            'yandex_geo': 'Тульская область',
            'region_id': '5020',
            'obl_id': '4621',
            'analytics_geo_regex': '^Tula Oblast$',
            'obl_seourl_postfix': 'tulskaya-oblast/',
            'obl_seourl_domain': 'tula.cian.ru',
        },
        {
            'reg': 'barnaul',
            'campaign_name_prefix': ['barnaul'],
            'city': 'Барнаул',
            'in_city': 'в Барнауле',
            'obl': 'Алтайский край',
            'in_obl': 'в Алтайском крае',
            'domain': 'barnaul.cian.ru',
            'geo_map': '&center=53.338519,83.704605&zoom=11',
            'yandex_geo': 'Алтайский край',
            'region_id': '4668',
            'obl_id': '4555',
            'analytics_geo_regex': '^Altai Krai$',
            'obl_seourl_postfix': 'altayskiy-kray/',
            'obl_seourl_domain': 'barnaul.cian.ru',
        },
        {
            'reg': 'ryazan',
            'campaign_name_prefix': ['ryazan'],
            'city': 'Рязань',
            'in_city': 'в Рязани',
            'obl': 'Рязанская область',
            'in_obl': 'в Рязанской области',
            'domain': 'ryazan.cian.ru',
            'geo_map': '&center=54.654800,39.723733&zoom=10',
            'yandex_geo': 'Рязанская область',
            'region_id': '4963',
            'obl_id': '4607',
            'analytics_geo_regex': '^Ryazan Oblast$',
            'obl_seourl_postfix': 'ryazanskaya-oblast/',
            'obl_seourl_domain': 'ryazan.cian.ru',
        },
        {
            'reg': 'penza',
            'campaign_name_prefix': ['penza'],
            'city': 'Пенза',
            'in_city': 'в Пензе',
            'obl': 'Пензенская область',
            'in_obl': 'в Пензенской области',
            'domain': 'penza.cian.ru',
            'geo_map': '&center=53.194055,45.029882&zoom=11',
            'yandex_geo': 'Пензенская область',
            'region_id': '4923',
            'obl_id': '4602',
            'analytics_geo_regex': '^Penza Oblast$',
            'obl_seourl_postfix': 'penzenskaya-oblast/',
            'obl_seourl_domain': 'penza.cian.ru',
        },
        {
            'reg': 'yaroslavl',
            'campaign_name_prefix': ['yaroslavl'],
            'city': 'Ярославль',
            'in_city': 'в Ярославле',
            'obl': 'Ярославская область',
            'in_obl': 'в Ярославской области',
            'domain': 'yaroslavl.cian.ru',
            'geo_map': '&center=57.650721,39.866922&zoom=10',
            'yandex_geo': 'Ярославская область',
            'region_id': '5075',
            'obl_id': '4636',
            'analytics_geo_regex': '^Yaroslavl Oblast$',
            'obl_seourl_postfix': 'yaroslavskaya-oblast/',
            'obl_seourl_domain': 'yaroslavl.cian.ru',
        },
        {
            'reg': 'orenburg',
            'campaign_name_prefix': ['orenburg'],
            'city': 'Оренбург',
            'in_city': 'в Оренбурге',
            'obl': 'Оренбургская область',
            'in_obl': 'в Оренбургской области',
            'domain': 'orenburg.cian.ru',
            'geo_map': '&center=51.793240,55.197472&zoom=11',
            'yandex_geo': 'Оренбургская область',
            'region_id': '4915',
            'obl_id': '4600',
            'analytics_geo_regex': '^Orenburg Oblast$',
            'obl_seourl_postfix': 'orenburgskaya-oblast/',
            'obl_seourl_domain': 'orenburg.cian.ru',
        },
        {
            'reg': 'izhevsk',
            'campaign_name_prefix': ['izhevsk'],
            'city': 'Ижевск',
            'in_city': 'в Ижевске',
            'obl': 'Удмуртская Республика',
            'in_obl': 'в Республике Удмуртия',
            'domain': 'izhevsk.cian.ru',
            'geo_map': '&center=56.852379,53.202749&zoom=10',
            'yandex_geo': 'Удмуртская Республика',
            'region_id': '4770',
            'obl_id': '4624',
            'analytics_geo_regex': '^Udmurt Republic$',
            'obl_seourl_postfix': 'udmurtskaya/',
            'obl_seourl_domain': 'izhevsk.cian.ru',
        },
        {
            'reg': 'kemerovo',
            'campaign_name_prefix': ['kemerovo'],
            'city': 'Кемерово',
            'in_city': 'в Кемерове',
            'obl': 'Кемеровская область',
            'in_obl': 'в Кемеровской области',
            'domain': 'kemerovo.cian.ru',
            'geo_map': '&center=55.366913,86.109759&zoom=10',
            'yandex_geo': 'Кемеровская область (Кузбасс)',
            'region_id': '4795',
            'obl_id': '4580',
            'analytics_geo_regex': '^Kemerovo Oblast$',
            'obl_seourl_postfix': 'kemerovskaya-oblast/',
            'obl_seourl_domain': 'kemerovo.cian.ru',
        },
        {
            'reg': 'kirov',
            'campaign_name_prefix': ['kirov'],
            'city': 'Киров',
            'in_city': 'в Кирове',
            'obl': 'Кировская область',
            'in_obl': 'в Кировской области',
            'domain': 'kirov.cian.ru',
            'geo_map': '&center=58.593995,49.625031&zoom=11',
            'yandex_geo': 'Кировская область',
            'region_id': '4800',
            'obl_id': '4581',
            'analytics_geo_regex': '^Kirov Oblast$',
            'obl_seourl_postfix': 'kirovskaya-oblast/',
            'obl_seourl_domain': 'kirov.cian.ru',
        },
        {
            'reg': 'bryansk',
            'campaign_name_prefix': ['bryansk'],
            'city': 'Брянск',
            'in_city': 'в Брянске',
            'obl': 'Брянская область',
            'in_obl': 'в Брянской области',
            'domain': 'bryansk.cian.ru',
            'geo_map': '&center=53.279268,34.374304&zoom=11',
            'yandex_geo': 'Брянская область',
            'region_id': '4691',
            'obl_id': '4562',
            'analytics_geo_regex': '^Bryansk Oblast$',
            'obl_seourl_postfix': 'bryanskaya-oblast/',
            'obl_seourl_domain': 'bryansk.cian.ru',
        },
        {
            'reg': 'tver',
            'campaign_name_prefix': ['tver'],
            'city': 'Тверь',
            'in_city': 'в Твери',
            'obl': 'Тверская область',
            'in_obl': 'в Тверской области',
            'domain': 'tver.cian.ru',
            'geo_map': '&center=56.859633,35.893548&zoom=11',
            'yandex_geo': 'Тверская область',
            'region_id': '176083',
            'obl_id': '4619',
            'analytics_geo_regex': '^Tver Oblast$',
            'obl_seourl_postfix': 'tverskaya-oblast/',
            'obl_seourl_domain': 'tver.cian.ru',
        },
        {
            'reg': 'lipetsk',
            'campaign_name_prefix': ['lipetsk'],
            'city': 'Липецк',
            'in_city': 'в Липецке',
            'obl': 'Липецкая область',
            'in_obl': 'в Липецкой области',
            'domain': 'lipetsk.cian.ru',
            'geo_map': '&center=52.603584,39.596237&zoom=11',
            'yandex_geo': 'Липецкая область',
            'region_id': '4847',
            'obl_id': '4589',
            'analytics_geo_regex': '^Lipetsk Oblast$',
            'obl_seourl_postfix': 'lipeckaya-oblast/',
            'obl_seourl_domain': 'lipetsk.cian.ru',
        },
        {
            'reg': 'tomsk',
            'campaign_name_prefix': ['tomsk'],
            'city': 'Томск',
            'in_city': 'в Томске',
            'obl': 'Томская область',
            'in_obl': 'в Томской области',
            'domain': 'tomsk.cian.ru',
            'geo_map': '&center=56.506804,84.981713&zoom=10',
            'yandex_geo': 'Томская область',
            'region_id': '5016',
            'obl_id': '4620',
            'analytics_geo_regex': '^Tomsk Oblast$',
            'obl_seourl_postfix': 'tomskaya-oblast/',
            'obl_seourl_domain': 'tomsk.cian.ru',
        },
        {
            'reg': 'vladimir',
            'campaign_name_prefix': ['vladimir'],
            'city': 'Владимир',
            'in_city': 'во Владимире',
            'obl': 'Владимирская область',
            'in_obl': 'в Владимирской области',
            'domain': 'vladimir.cian.ru',
            'geo_map': '&center=56.138330,40.421470&zoom=11',
            'yandex_geo': 'Владимирская область',
            'region_id': '4703',
            'obl_id': '4564',
            'analytics_geo_regex': '^Vladimir Oblast$',
            'obl_seourl_postfix': 'vladimirskaya-oblast/',
            'obl_seourl_domain': 'vladimir.cian.ru',
        },
        {
            'reg': 'kursk',
            'campaign_name_prefix': ['kursk'],
            'city': 'Курск',
            'in_city': 'в Курске',
            'obl': 'Курская область',
            'in_obl': 'в Курской области',
            'domain': 'kursk.cian.ru',
            'geo_map': '&center=51.737207,36.185245&zoom=11',
            'yandex_geo': 'Курская область',
            'region_id': '4835',
            'obl_id': '4587',
            'analytics_geo_regex': '^Kursk Oblast$',
            'obl_seourl_postfix': 'kurskaya-oblast/',
            'obl_seourl_domain': 'kursk.cian.ru',
        },
        {
            'reg': 'belgorod',
            'campaign_name_prefix': ['belgorod'],
            'city': 'Белгород',
            'in_city': 'в Белгороде',
            'obl': 'Белгородская область',
            'in_obl': 'в Белгородской области',
            'domain': 'belgorod.cian.ru',
            'geo_map': '&center=50.594166,36.572280&zoom=11',
            'yandex_geo': 'Белгородская область',
            'region_id': '4671',
            'obl_id': '4561',
            'analytics_geo_regex': '^Belgorod Oblast$',
            'obl_seourl_postfix': 'belgorodskaya-oblast/',
            'obl_seourl_domain': 'belgorod.cian.ru',
        },
        {
            'reg': 'ulyanovsk',
            'campaign_name_prefix': ['ulyanovsk'],
            'city': 'Ульяновск',
            'in_city': 'в Ульяновске',
            'obl': 'Ульяновская область',
            'in_obl': 'в Ульяновской области',
            'domain': 'ulyanovsk.cian.ru',
            'geo_map': '&center=54.302987,48.434159&zoom=10',
            'yandex_geo': 'Ульяновская область',
            'region_id': '5027',
            'obl_id': '4625',
            'analytics_geo_regex': '^Ulyanovsk Oblast$',
            'obl_seourl_postfix': 'ulyanovskaya-oblast/',
            'obl_seourl_domain': 'ulyanovsk.cian.ru',
        },
        {
            'reg': 'vladivostok',
            'campaign_name_prefix': ['vladivostok'],
            'city': 'Владивосток',
            'in_city': 'во Владивостоке',
            'obl': 'Приморский край',
            'in_obl': 'в Приморском крае',
            'domain': 'vladivostok.cian.ru',
            'geo_map': '&center=43.142304,131.962731&zoom=11',
            'yandex_geo': 'Приморский край',
            'region_id': '4701',
            'obl_id': '4604',
            'analytics_geo_regex': '^Primorsky Krai$',
            'obl_seourl_postfix': 'primorskiy-kray/',
            'obl_seourl_domain': 'vladivostok.cian.ru',
        },
        {
            'reg': 'kaluga',
            'campaign_name_prefix': ['kaluga'],
            'city': 'Калуга',
            'in_city': 'в Калуге',
            'obl': 'Калужская область',
            'in_obl': 'в Калужской области',
            'domain': 'kaluga.cian.ru',
            'geo_map': '&center=54.540883,36.233286&zoom=11',
            'yandex_geo': 'Калужская область',
            'region_id': '4780',
            'obl_id': '4576',
            'analytics_geo_regex': '^Kaluga Oblast$',
            'obl_seourl_postfix': 'kaluzhskaya-oblast/',
            'obl_seourl_domain': 'kaluga.cian.ru',
        },
        {
            'reg': 'cheboksary',
            'campaign_name_prefix': ['cheboksary'],
            'city': 'Чебоксары',
            'in_city': 'в Чебоксарах',
            'obl': 'Чувашская республика',
            'in_obl': 'в Республике Чувашия',
            'domain': 'cheboksary.cian.ru',
            'geo_map': '&center=56.093060,47.313913&zoom=11',
            'yandex_geo': 'Чувашская республика',
            'region_id': '5047',
            'obl_id': '4633',
            'analytics_geo_regex': '^Chuvashia Republic$',
            'obl_seourl_postfix': 'chuvashskaya/',
            'obl_seourl_domain': 'cheboksary.cian.ru',
        },
        {
            'reg': 'kaliningrad',
            'campaign_name_prefix': ['kaliningrad'],
            'city': 'Калининград',
            'in_city': 'в Калининграде',
            'obl': 'Калининградская область',
            'in_obl': 'в Калининградской области',
            'domain': 'kaliningrad.cian.ru',
            'geo_map': '&center=54.707720,20.462805&zoom=11',
            'yandex_geo': 'Калининградская область',
            'region_id': '4778',
            'obl_id': '4574',
            'analytics_geo_regex': '^Kaliningrad Oblast$',
            'obl_seourl_postfix': 'kaliningradskaya-oblast/',
            'obl_seourl_domain': 'kaliningrad.cian.ru',
        },
        {
            'reg': 'smolensk',
            'campaign_name_prefix': ['smolensk'],
            'city': 'Смоленск',
            'in_city': 'в Смоленске',
            'obl': 'Смоленская область',
            'in_obl': 'в Смоленской области',
            'domain': 'smolensk.cian.ru',
            'geo_map': '&center=54.85078725965436,32.0642520440742&zoom=11',
            'yandex_geo': 'Смоленская область',
            'region_id': '4987',
            'obl_id': '4614',
            'analytics_geo_regex': '^Smolensk Oblast$',
            'obl_seourl_postfix': 'smolenskaya-oblast/',
            'obl_seourl_domain': 'smolensk.cian.ru',
        },
        {
            'reg': 'orel',
            'campaign_name_prefix': ['orel'],
            'city': 'Орёл',
            'in_city': 'в Орле',
            'obl': 'Орловская область',
            'in_obl': 'в Орловской области',
            'domain': 'orel.cian.ru',
            'geo_map': '&center=52.967993,36.096365&zoom=11',
            'yandex_geo': 'Орловская область',
            'region_id': '175604',
            'obl_id': '4601',
            'analytics_geo_regex': '^Oryol Oblast$',
            'obl_seourl_postfix': 'orlovskaya-oblast/',
            'obl_seourl_domain': 'orel.cian.ru',
        },
        {
            'reg': 'ivanovo',
            'campaign_name_prefix': ['ivanovo'],
            'city': 'Иваново',
            'in_city': 'в Иванове',
            'obl': 'Ивановская область',
            'in_obl': 'в Ивановской области',
            'domain': 'ivanovo.cian.ru',
            'geo_map': '&center=56.997342,40.999440&zoom=11',
            'yandex_geo': 'Ивановская область',
            'region_id': '4767',
            'obl_id': '4570',
            'analytics_geo_regex': '^Ivanovo Oblast$',
            'obl_seourl_postfix': 'ivanovskaya-oblast/',
            'obl_seourl_domain': 'ivanovo.cian.ru',
        },
        {
            'reg': 'tambov',
            'campaign_name_prefix': ['tambov'],
            'city': 'Тамбов',
            'in_city': 'в Тамбове',
            'obl': 'Тамбовская область',
            'in_obl': 'в Тамбовской области',
            'domain': 'tambov.cian.ru',
            'geo_map': '&center=52.732522,41.421759&zoom=11',
            'yandex_geo': 'Тамбовская область',
            'region_id': '5011',
            'obl_id': '4617',
            'analytics_geo_regex': '^Tambov Oblast$',
            'obl_seourl_postfix': 'tambovskaya-oblast/',
            'obl_seourl_domain': 'tambov.cian.ru',
        },
        {
            'reg': 'astrahan',
            'campaign_name_prefix': ['astrahan'],
            'city': 'Астрахань',
            'in_city': 'в Астрахани',
            'obl': 'Астраханская область',
            'in_obl': 'в Астраханской области',
            'domain': 'astrahan.cian.ru',
            'geo_map': '&center=46.359311,48.037508&zoom=11',
            'yandex_geo': 'Астраханская область',
            'region_id': '4660',
            'obl_id': '4558',
            'analytics_geo_regex': '^Astrakhan Oblast$',
            'obl_seourl_postfix': 'astrahanskaya-oblast/',
            'obl_seourl_domain': 'astrahan.cian.ru',
        },
        {
            'reg': 'kurgan',
            'campaign_name_prefix': ['kurgan'],
            'city': 'Курган',
            'in_city': 'в Кургане',
            'obl': 'Курганская область',
            'in_obl': 'в Курганской области',
            'domain': 'kurgan.cian.ru',
            'geo_map': '&center=55.424635,65.393656&zoom=10',
            'yandex_geo': 'Курганская область',
            'region_id': '4834',
            'obl_id': '4586',
            'analytics_geo_regex': '^Kurgan Oblast$',
            'obl_seourl_postfix': 'kurganskaya-oblast/',
            'obl_seourl_domain': 'kurgan.cian.ru',
        },
        {
            'reg': 'kostroma',
            'campaign_name_prefix': ['kostroma'],
            'city': 'Кострома',
            'in_city': 'в Костроме',
            'obl': 'Костромская область',
            'in_obl': 'в Костромской области',
            'domain': 'kostroma.cian.ru',
            'geo_map': '&center=57.762013,40.948100&zoom=11',
            'yandex_geo': 'Костромская область',
            'region_id': '175050',
            'obl_id': '4583',
            'analytics_geo_regex': '^Kostroma Oblast$',
            'obl_seourl_postfix': 'kostromskaya-oblast/',
            'obl_seourl_domain': 'kostroma.cian.ru',
        },
        {
            'reg': 'surgut',
            'campaign_name_prefix': ['surgut'],
            'city': 'Сургут',
            'in_city': 'в Сургуте',
            'obl': 'Ханты-Мансийский автономный округ',
            'in_obl': 'в Ханты-Мансийском автономном округе',
            'domain': 'surgut.cian.ru',
            'geo_map': '&center=61.284432,73.421297&zoom=11',
            'yandex_geo': 'Ханты-Мансийский автономный округ - Югра',
            'region_id': '5003',
            'obl_id': '4629',
            'analytics_geo_regex': '^Khanty-Mansi Autonomous Okrug$',
            'obl_seourl_postfix': None,
            'obl_seourl_domain': 'hmao.cian.ru',
        },
        {
            'reg': 'novgorod',
            'campaign_name_prefix': ['novgorod'],
            'city': 'Великий Новгород',
            'in_city': 'в Великом Новгороде',
            'obl': 'Новгородская область',
            'in_obl': 'в Новгородской области',
            'domain': 'novgorod.cian.ru',
            'geo_map': '&center=58.550906,31.305950&zoom=11',
            'yandex_geo': 'Новгородская область',
            'region_id': '4694',
            'obl_id': '4597',
            'analytics_geo_regex': '^Novgorod Oblast$',
            'obl_seourl_postfix': 'novgorodskaya-oblast/',
            'obl_seourl_domain': 'novgorod.cian.ru',
        },
        {
            'reg': 'ulan-ude',
            'campaign_name_prefix': ['ulanude'],
            'city': 'Улан-Удэ',
            'in_city': 'в Улан-Удэ',
            'obl': 'Республика Бурятия',
            'in_obl': 'в Республике Бурятия',
            'domain': 'ulan-ude.cian.ru',
            'geo_map': '&center=51.843010,107.627797&zoom=10',
            'yandex_geo': 'Республика Бурятия',
            'region_id': '5026',
            'obl_id': '4563',
            'analytics_geo_regex': '^Buryatia$',
            'obl_seourl_postfix': 'buryatiya/',
            'obl_seourl_domain': 'ulan-ude.cian.ru',
        },
        {
            'reg': 'mahachkala',
            'campaign_name_prefix': ['mahachkala'],
            'city': 'Махачкала',
            'in_city': 'в Махачкале',
            'obl': 'Республика Дагестан',
            'in_obl': 'в Республике Дагестан',
            'domain': 'mahachkala.cian.ru',
            'geo_map': '&center=42.964583,47.478279&zoom=12',
            'yandex_geo': 'Республика Дагестан',
            'region_id': '4857',
            'obl_id': '4568',
            'analytics_geo_regex': '^Republic of Dagestan$',
            'obl_seourl_postfix': 'dagestan/',
            'obl_seourl_domain': 'mahachkala.cian.ru',
        },
        {
            'reg': 'pskov',
            'campaign_name_prefix': ['pskov'],
            'city': 'Псков',
            'in_city': 'в Пскове',
            'obl': 'Псковская область',
            'in_obl': 'в Псковской области',
            'domain': 'pskov.cian.ru',
            'geo_map': '&center=57.802826,28.357873&zoom=11',
            'yandex_geo': 'Псковская область',
            'region_id': '4946',
            'obl_id': '4605',
            'analytics_geo_regex': '^Pskov Oblast$',
            'obl_seourl_postfix': 'pskovskaya-oblast/',
            'obl_seourl_domain': 'pskov.cian.ru',
        },
        {
            'reg': 'rostov',
            'campaign_name_prefix': ['rostov'],
            'city': 'Ростов-на-Дону',
            'in_city': 'в Ростове-на-Дону',
            'obl': 'Ростовская область',
            'in_obl': 'в Ростовской области',
            'domain': 'rostov.cian.ru',
            'geo_map': '&center=47.312145347649874,39.72567975986745&zoom=10',
            'yandex_geo': 'Ростовская область',
            'region_id': '4959',
            'obl_id': '4606',
            'analytics_geo_regex': '^Rostov Oblast$',
            'obl_seourl_postfix': 'rostovskaya-oblast/',
            'obl_seourl_domain': 'rostov.cian.ru',
        },
        {
            'reg': 'samara',
            'campaign_name_prefix': ['samara'],
            'city': 'Самара',
            'in_city': 'в Самаре',
            'obl': 'Самарская область',
            'in_obl': 'в Самарской области',
            'domain': 'samara.cian.ru',
            'geo_map': '&center=53.431089199469305,49.989416124299105&zoom=9',
            'yandex_geo': 'Самарская область',
            'region_id': '4966',
            'obl_id': '4608',
            'analytics_geo_regex': '^Samara Oblast$',
            'obl_seourl_postfix': 'samarskaya-oblast/',
            'obl_seourl_domain': 'samara.cian.ru',
        },
        {
            'reg': 'kazan',
            'campaign_name_prefix': ['kazan'],
            'city': 'Казань',
            'in_city': 'в Казани',
            'obl': 'Республика Татарстан',
            'in_obl': 'в Республике Татарстан',
            'domain': 'kazan.cian.ru',
            'geo_map': '&center=55.84636608781986,49.067939487285884&zoom=11',
            'yandex_geo': 'Республика Татарстан',
            'region_id': '4777',
            'obl_id': '4618',
            'analytics_geo_regex': '^Tatarstan$',
            'obl_seourl_postfix': 'tatarstan/',
            'obl_seourl_domain': 'kazan.cian.ru',
        },
        {
            'reg': 'ufa',
            'campaign_name_prefix': ['ufa'],
            'city': 'Уфа',
            'in_city': 'в Уфе',
            'obl': 'Республика Башкортостан',
            'in_obl': 'в Республике Башкортостан',
            'domain': 'ufa.cian.ru',
            'geo_map': '&center=54.815804448482524,56.01886948104949&zoom=11',
            'yandex_geo': 'Республика Башкортостан',
            'region_id': '176245',
            'obl_id': '4560',
            'analytics_geo_regex': '^Republic of Bashkortostan$',
            'obl_seourl_postfix': 'bashkortostan/',
            'obl_seourl_domain': 'ufa.cian.ru',
        },
        {
            'reg': 'nn',
            'campaign_name_prefix': ['nn'],
            'city': 'Нижний Новгород',
            'in_city': 'в Нижнем Новгороде',
            'obl': 'Нижегородская область',
            'in_obl': 'в Нижегородской области',
            'domain': 'nn.cian.ru',
            'geo_map': '&center=56.345465786182544,43.90993257053197&zoom=10',
            'yandex_geo': 'Нижегородская область',
            'region_id': '4885',
            'obl_id': '4596',
            'analytics_geo_regex': '^Nizhny Novgorod Oblast$',
            'obl_seourl_postfix': 'nizhegorodskaya-oblast/',
            'obl_seourl_domain': 'nn.cian.ru',
        },
        {
            'reg': 'krasnodar',
            'campaign_name_prefix': ['krasnodar'],
            'city': 'Краснодар',
            'in_city': 'в Краснодаре',
            'obl': 'Краснодарский край',
            'in_obl': 'в Краснодарском крае',
            'domain': 'krasnodar.cian.ru',
            'geo_map': '&center=45.7275197009489,41.240933937951915&zoom=7',
            'yandex_geo': 'Краснодарский край',
            'region_id': '4820',
            'obl_id': '4584',
            'analytics_geo_regex': '^Krasnodar Krai$',
            'obl_seourl_postfix': 'krasnodarskiy-kray/',
            'obl_seourl_domain': 'krasnodar.cian.ru',
        },
        {
            'reg': 'sevastopol',
            'campaign_name_prefix': ['sevastopol'],
            'city': 'Севастополь',
            'in_city': 'в Севастополе',
            'obl': 'Севастополь',
            'in_obl': 'в Севастополе',
            'domain': 'sevastopol.cian.ru',
            'geo_map': '&center=44.59047823811956,33.53976803831757&zoom=12',
            'yandex_geo': 'Севастополь',
            'region_id': '184723',
            'obl_id': '184723',
            'analytics_geo_regex': '^n/a$',
            'obl_seourl_postfix': None,
            'obl_seourl_domain': 'sevastopol.cian.ru',
        },
        {
            'reg': 'arhangelsk',
            'campaign_name_prefix': ['arhangelsk'],
            'city': 'Архангельск',
            'in_city': 'в Архангельске',
            'obl': 'Архангельская область',
            'in_obl': 'в Архангельской области',
            'domain': 'arhangelsk.cian.ru',
            'geo_map': '&center=64.5590721963482,40.50051244441416&zoom=10',
            'yandex_geo': 'Архангельская область',
            'region_id': '4658',
            'obl_id': '4557',
            'analytics_geo_regex': '^Arkhangelsk Oblast$',
            'obl_seourl_postfix': 'arhangelskaya-oblast/',
            'obl_seourl_domain': 'arhangelsk.cian.ru',
        },
        {
            'reg': 'spb',
            'campaign_name_prefix': ['spb', 'spblo'],
            'city': 'Санкт-Петербург',
            'in_city': 'в Санкт-Петербурге',
            'obl': 'Ленинградская область',
            'in_obl': 'в Ленинградской области',
            'domain': 'spb.cian.ru',
            'geo_map': '&center=60.087721113782614,29.872359889559405&zoom=9',
            'yandex_geo': 'Санкт-Петербург и Ленинградская область',
            'region_id': '-2',
            'obl_id': '4588',
            'analytics_geo_regex': '(Saint Petersburg)|(Leningrad Oblast)',
            'obl_seourl_postfix': 'leningradskaya-oblast/',
            'obl_seourl_domain': 'spb.cian.ru',
        },
        {
            'reg': 'www',
            'campaign_name_prefix': ['msk', 'mo', 'dmo', 'bmo', 'mskmo'],
            'city': 'Москва',
            'in_city': 'в Москве',
            'obl': 'Московская область',
            'in_obl': 'в Московской области',
            'domain': 'www.cian.ru',
            'geo_map': '&center=55.817955829724696,37.41757520008833&zoom=9',
            'yandex_geo': 'Москва и область',
            'region_id': '-1',
            'obl_id': '4593',
            'analytics_geo_regex': '(Moscow)|(Moscow Oblast)',
            'obl_seourl_postfix': 'moskovskaya-oblast/',
            'obl_seourl_domain': 'www.cian.ru',
        },
        {
            'reg': 'sochi',
            'campaign_name_prefix': ['sochi'],
            'city': 'Сочи',
            'in_city': 'в Сочи',
            'obl': 'Краснодарский край',
            'in_obl': 'в Краснодарском крае',
            'domain': 'sochi.cian.ru',
            'geo_map': '&center=43.81084805887988,39.88346680448195&zoom=9',
            'yandex_geo': 'Краснодарский край',
            'region_id': '4998',
            'obl_id': '4998',
            'analytics_geo_regex': '^Krasnodar Krai$',
            'obl_seourl_postfix': None,
            'obl_seourl_domain': 'sochi.cian.ru',
        },
    ]

    def __init__(self, run_tests=False):
        if run_tests:
            fields = set(self.fields)

            for i in self.data:
                flds = {j for j in i.keys()}
                if fields ^ flds:
                    logger.critical(f"Не соблюдена целостность данных в {type(self)}", exc_info=True)
                    raise IntegrityDataError

    def __contains__(self, item):
        return self.region_search(item)

    def __getitem__(self, item):
        return self.data[item]

    def __len__(self):
        return len(self.data)

    def __str__(self):
        fields = self.fields[:]
        #fields.remove('analytics_geo_regex')
        #fields.remove('obl_seourl_postfix')
        res = ""
        for i in self.data:
            line = [str(i[j]) for j in fields]
            res = f"{res}{';'.join(line)};\n"

        return res

    @connection_attempts()
    def get_region_id_by_url(self, url):
        patterns = ['(<a href=\"\/newobjects\/list\/\?deal_type=sale&engine_version=2&offer_type=newobject&p=2)(&region=[\-0-9]+)(\">2<\/a>)',
                    '(map.cian.ru.*)(&region=[\-0-9]+)(&with_newobject)',
                    '(href=\"/newobjects/list\?.*)(region=[\-0-9]+)(&.*p=2\")',
                    '(href=\".*/map/\?newbuildings_search=true.*)(region=[\-0-9]+)(.*На карте)'
                    ]
        patterns = [re.compile(i) for i in patterns]

        counter, region_id = 10, None
        while region_id is None:
            web_page = requests.get(url, headers=self.headers)

            for pattern in patterns:
                region_id = pattern.search(web_page.text)
                if region_id is not None:
                    break
            else:
                logger.warning(f"Не удалось получить id региона для {url}")

            counter -= 1
            if counter < 0:
                break

        return region_id

    def get_obl_id_by_domain(self, domain):
        url = f"https://{domain}/novostroyki/?p=1"
        url = self.update_obl_url_postfix(url, src="/novostroyki/", dest_oblsuffix="/novostroyki-oblsuffix/")
        obl_id = self.get_region_id_by_url(url)
        if obl_id is None:
            obl_id = self.region_search(domain)['obl_id']
            logger.error(f"Не удалось получить id области региона для {domain}\n"
                         f"используем стандартный obl_id= {obl_id}")
        else:
            obl_id = obl_id.groups()[1].split("=")[1]
            if obl_id != self.region_search(domain)['obl_id']:
                logger.error(f"ВНИМАНИЕ: в базе {type(self)} устарел id области региона для {domain}")

        return obl_id

    def get_region_id_by_domain(self, domain):
        url = f'https://{domain}/novostroyki/?p=1'
        region_id = self.get_region_id_by_url(url)
        if region_id is None:
            region_id = self.region_search(domain)['region_id']
            logger.error(f"Не удалось получить id региона для {domain}\n"
                         f"используем стандартный region_id= {region_id}")
        else:
            region_id = region_id.groups()[1].split("=")[1]
            if region_id != self.region_search(domain)['region_id']:
                logger.error(f"ВНИМАНИЕ: в базе {type(self)} устарел id региона для {domain}")

        return region_id

    def region_search(self, val):
        if val.find(".cian.ru") != -1:
            domain = self.ptr_domain.search(val)
            if domain is not None:
                val = domain.groups()[1]

        if val.find("_") != -1:  # при передачи имени кампании поиск по campaign_name_prefix
            val = val.split("_")[1]  # поле в названии кампании содержащее регион

        for i in self.data:
            if i["reg"] == val \
                    or i["city"].upper() == val.upper() \
                    or i["domain"] == val \
                    or i["obl_seourl_domain"] == val \
                    or val in i["campaign_name_prefix"]:
                return i

        return False

    def update_obl_url_postfix(self, url, src="/kupit-dom/", dest_oblsuffix="/kupit-dom-oblsuffix/"):
        if url.find(".cian.ru") != -1:
            domain = self.ptr_domain.search(url)
            if domain is not None:
                domain = domain.groups()[1]
        else:
            logger.warning(f"Ссылка не cian.ru: {url}")
            return url

        data = self.region_search(domain)
        if not data:
            logger.warning(f"Невозможно найти апдейт для obl_url_postfix {url}")
            return url

        if data['obl_seourl_postfix'] is None:
            url = url.replace(domain, data['obl_seourl_domain'])
        else:
            dest_oblsuffix = dest_oblsuffix.replace("-oblsuffix/", f"-{data['obl_seourl_postfix']}")
            url = url.replace(src, dest_oblsuffix)

        return url


if __name__ == '__main__':
    t = CianRegions()
    print(t.region_search("voronezh"))
    print(t.region_search("Воронеж"))
    print(t.region_search("voronezh.cian.ru"))
    print(t.region_search("b2c_voronezh_drtg_site_rentsub_mix_network"))
    print(t.region_search("https://voronezh.cian.ru"))

    print(t.update_obl_url_postfix("https://ulan-ude.cian.ru/tro-lo-lo/", "/tro-lo-lo/", "/kupit-dom-oblsuffix/"))

    n = len(t)
    for num, i in enumerate(t):
        print(f"{num} of {n} testing {i['domain']} id: {t.get_region_id_by_domain(i['domain'])}")


