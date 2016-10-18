import requests


class CloudbrainAuth(object):

    def __init__(self, base_url=None, token=None):
        self.base_url = base_url or 'http://dockerhost:3000'
        self.token = token or ''

    def token_info(self, token=None):
        token_url = self.base_url + '/oauth/token/info'
        token = token or self.token

        headers = {
          'Authorization': 'Bearer %s' % token
        }

        response = requests.get(token_url, headers=headers, verify=False)
        return response.json()

    def vhost_info(self, token=None):
        token_url = self.base_url + '/rabbitmq/vhost/info'
        token = token or self.token

        headers = {
          'Authorization': 'Bearer %s' % token
        }

        response = requests.get(token_url, headers=headers, verify=False)
        return response.json()

    def get_vhost(self, token):
        response = self.vhost_info(token)
        return response['vhost']