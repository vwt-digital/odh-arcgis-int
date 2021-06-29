import logging
import mimetypes
from urllib.parse import unquote_plus, urlparse

import google.auth
import google.auth.transport.requests
import requests
import validators


class AttachmentService:
    def __init__(self):
        self.credentials, self.project = google.auth.default()
        self.auth_req = google.auth.transport.requests.Request()

    def get(self, attachment_url):
        """
        Get an attachment

        :param attachment_url: Attachment URL
        :type attachment_url: str

        :return: File content-type, File name, File content
        :rtype: (str, str, str)
        """

        # Check if attachment URL is valid URL
        if not is_valid_url(attachment_url):
            logging.info(
                f"Attachment '{attachment_url}' cannot be downloaded, not a valid url"
            )
            return None, None, None

        # Parse url into file name and type
        file_name = unquote_plus(urlparse(attachment_url).path).split("/")[-1]
        file_type = mimetypes.guess_type(file_name)[0]

        # Refresh authentication token
        self.credentials.refresh(self.auth_req)

        request_headers = {"Authorization": f"Bearer {self.credentials.token}"}

        # Get bucket
        try:
            response = requests.get(attachment_url, headers=request_headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error(
                f"Attachment '{attachment_url}' cannot be downloaded, skipping upload: {str(e)}"
            )
            return None, None, None
        else:
            logging.debug(f"Successfully downloaded attachment '{attachment_url}'")
            return file_type, file_name, response.content


def is_valid_url(url_string: str) -> bool:
    """
    Check whether input is a valid URL

    :param url_string: URL
    :type url_string: str

    :return: Is valid URL
    :rtype: boolean
    """

    result = validators.url(url_string)

    if isinstance(result, validators.utils.ValidationFailure):
        return False

    return result
