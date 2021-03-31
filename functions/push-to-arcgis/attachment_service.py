import logging
import mimetypes

import google.auth
import google.auth.transport.requests
import requests


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

        file_name = attachment_url.split("/")[-1]
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
            logging.info(f"Successfully downloaded attachment '{attachment_url}'")
            return file_type, file_name, response.content
