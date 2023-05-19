import os
import re
import logging.config
from time import sleep
from http.client import RemoteDisconnected
from urllib3.exceptions import ProtocolError
from socket import timeout

class PySeaError(Exception): pass
class LimitOfRetryError(PySeaError): pass

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
                except (ConnectionError, ProtocolError, RemoteDisconnected, timeout) as err:
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


class EnviVar:
    credentials_is_ok = False

    def __init__(self, main_dir="/home/eugene/Yandex.Disk/localsource/MyProj/",
                 cred_dir="/home/eugene/Yandex.Disk/localsource/credentials/"):
        self._environment_variables = dict()
        self.__set_env("MAIN_PYSEA_DIR", main_dir)
        self.__set_env("LOGGING_PYSEA_CONFIG", "{}logging.conf".format(cred_dir))
        self.__set_env("CREDENTIALS_DIR", cred_dir)

        if not EnviVar.credentials_is_ok:
            env_file = f"{cred_dir}environment_variables.txt"
            if os.path.isfile(env_file):
                if os.path.isfile(self._environment_variables["LOGGING_PYSEA_CONFIG"]):
                    logging.config.fileConfig(fname=self._environment_variables["LOGGING_PYSEA_CONFIG"],
                                              disable_existing_loggers=False)

                with open(env_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        line = list(re.search('export\s+(\w+)=\"(.*)\"', line).groups())
                        self.__set_env(*line)
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f"{cred_dir}{os.environ['GOOGLE_APPLICATION_CREDENTIALS']}"
                EnviVar.credentials_is_ok = True

    def __set_env(self, key, value):
        os.environ[key] = value
        self._environment_variables.update({key: value})

    def __getitem__(self, item):
        res = os.environ.get(item, None)
        if not res:
            res = self._environment_variables.get(item, None)
        return res

    def __iter__(self):
        return iter(self._environment_variables.items())

    def __str__(self):
        return str(self._environment_variables)

    var = __getitem__


regex = {
    "TARGET_JK_EVENT_LABEL": '((/sale/flat/)|(/jk/))(.*)(object_type=2)(.*)(from_developer=1)',
}

if __name__ == '__main__':
    ENVI = EnviVar(main_dir="/home/eugene/Yandex.Disk/localsource/common_constants/",
                   cred_dir="/home/eugene/Yandex.Disk/localsource/credentials/")
    logger = logging.getLogger(__name__)

    print(ENVI)

    logger.debug('This is a debug message')
    logger.info('This is an info message')
    logger.warning('This is a warning message')
    logger.error('This is an error message')
    logger.critical('This is a critical message')
