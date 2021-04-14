import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


def get_requests_session(retries=3, backoff=1, status_forcelist=(500, 502, 503, 504)):
    """
    Returns a requests session with retry enabled.

    :param retries: Total request retries
    :type retries: int
    :param backoff: Backup factor
    :type backoff: int
    :param status_forcelist: Status codes to retry to
    :type status_forcelist: tuple

    :return: Request session
    """

    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff,
        status_forcelist=status_forcelist,
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session
