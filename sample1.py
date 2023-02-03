import requests
import base64
import urllib3
import json
import time

requests.packages.urllib3.disable_warnings()

class PhantomAPI(object):
    baseurl = f"https://devcheck/rest/decided_list/"
    auth_token = "abcdefghfhtitjtt"

    def __init__(self, base_url, auth_token):
        self.baseurl = base_url
        self.auth_token = {"ph-auth-token": auth_token}
        print(self.baseurl)

    def get_custom_lists(self):
        print("Base API URL is: " + self.baseurl)
        custom_url = self.baseurl + f"?page_size=0"
        print("Custom API URL is: " + custom_url)
        response = requests.request(method="GET", url=custom_url, headers=self.auth_token, verify=False)
        if response.status_code == 200:
            data_list = []
            for i in response.json().get("data"):
                new_url = self.baseurl+f"{i.get('id')}"
                print("The new Custom List URL is: " + new_url)
                response = requests.request(method="GET", url=new_url, headers=self.auth_token, verify=False)

                all_data = response.json()
                data = json.dumps(all_data)
                loaded_data = json.loads(data)

                if len(loaded_data.get("content")) > 1:
                    header = loaded_data.get("content")[0]
                    rows = loaded_data.get("content")[1:]
                    new_data = [dict(zip(header, row)) for row in rows]
                    print(new_data)
                    #data_lst.append(new_data)

if __name__ == "__main__":
    baseurl = f"https://devcheck/rest/decided_list/"
    auth_token = "abcdefghkvuigrouhtih"
    phantomApi = PhantomAPI(base_url=baseurl, auth_token=auth_token)
    phantomApi.get_custom_lists()
