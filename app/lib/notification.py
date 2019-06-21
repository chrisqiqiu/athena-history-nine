import requests
import json


class SlackNotification(object):

    def __init__(self, module_name):
        self.base_uri = "http://slack.datascience.ec2/postMessage"
        self.warningTemplateMessage = {
            "text": "<!here> Error: Athena runner python version run {} from module : {}"}
        self.infoTemplateMessage = {
            "text": "Error: Athena runner python version run {} from module : {}"}
        self.headers = {'Content-Type': 'application/json'}
        self.module = module_name

    def info(self, message):
        infoMessage = {}
        infoMessage['text'] = self.infoTemplateMessage['text'].format(
            str(message), self.module)
        resp = requests.post(self.base_uri, headers=self.headers,
                             data=json.dumps(infoMessage))

        return resp

    def warn(self, message):

        warningMessage = {}
        warningMessage['text'] = self.warningTemplateMessage['text'].format(
            str(message), self.module)

        resp = requests.post(self.base_uri, headers=self.headers,
                             data=json.dumps(warningMessage))
        return resp


# SMTP host is pending ...

class EmailNotification(object):
    pass
