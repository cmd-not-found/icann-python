import os
import sys
import cgi
import json
import boto3
import datetime
import requests
from multiprocessing import Pool, cpu_count

class ICANN:

    def __init__(self, access_token=None):
        self.access_token = access_token
        self.auth_url = "https://account-api.icann.org/api/authenticate"
        self.czds_base_url = "https://czds-api.icann.org"
        self.username = ''
        self.password = ''
        self.working_directory = '.'
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    def _http_req(self, method, url):

        headers = self.headers
        headers['Authorization'] = 'Bearer {0}'.format(self.access_token)

        resp = requests.request(
            method=method,
            url=url,
            headers=headers,
            stream=True
        )

        return resp


    def _get_aws_tokens(self):
        session = boto3.Session(region_name='us-east-2')
        ssm = session.client('ssm')
        self.password = ssm.get_parameter(Name='czds_password', WithDecryption=True)['Parameter']['Value']
        self.username = ssm.get_parameter(Name='czds_username')['Parameter']['Value']


    def _authenticate(self):

        self._get_aws_tokens()
        creds = {
            "username": self.username,
            "password": self.password
        }

        resp = requests.post(self.auth_url, data=json.dumps(creds), headers=self.headers)
        # Return the access_token on status code 200. Otherwise, terminate the program.
        if resp.status_code == 200:
            self.access_token = resp.json()['accessToken']
        elif resp.status_code == 404:
            sys.stderr.write("Invalid url " + self.auth_url)
            exit(1)
        elif resp.status_code == 401:
            sys.stderr.write("Invalid username/password. Please reset your password via web")
            exit(1)
        elif resp.status_code == 500:
            sys.stderr.write("Internal server error. Please try again later")
            exit(1)
        else:
            sys.stderr.write("Failed to authenticate user {0} with error code {1}".format(self.username, resp.status_code))
            exit(1)


    def get_zone_links(self):

        links_url = self.czds_base_url + "/czds/downloads/links"
        resp = self._http_req('GET', links_url)

        if resp.status_code == 200:
            zone_links = resp.json()
            print("{0}: The number of zone files to be downloaded is {1}".format(datetime.datetime.now(),len(zone_links)))
            return zone_links
        elif resp.status_code == 401:
            print("The access_token has been expired. Re-authenticate user {0}".format(self.username))
            self._authenticate()
            self.get_zone_links()
        else:
            sys.stderr.write("Failed to get zone links from {0} with error code {1}\n".format(links_url, resp.status_code))
            return None

    def download_one_zone(self, url):

        output_directory = self.working_directory + "/zonefiles"
        print("{0}: Downloading zone file from {1}".format(str(datetime.datetime.now()), url))
        resp = self._http_req('GET', url)

        if resp.status_code == 200:
            # Try to get the filename from the header
            _,option = cgi.parse_header(resp.headers['content-disposition'])
            filename = option.get('filename')

            # If get a filename from the header, then makeup one like [tld].txt.gz
            if not filename:
                filename = url.rsplit('/', 1)[-1].rsplit('.')[-2] + '.txt.gz'

            # This is where the zone file will be saved
            path = '{0}/{1}'.format(output_directory, filename)

            with open(path, 'wb') as f:
                for chunk in resp.iter_content(1024):
                    f.write(chunk)

            print("{0}: Completed downloading zone to file {1}".format(str(datetime.datetime.now()), path))

        elif resp.status_code == 401:
            print("The access_token has been expired. Re-authenticate user {0}".format(username))
            self._authenticate()
            self.download_one_zone(url)
        elif resp.status_code == 404:
            print("No zone file found for {0}".format(url))
        else:
            sys.stderr.write('Failed to download zone from {0} with code {1}\n'.format(url, status_code))

    # Function definition for downloading all the zone files
    def download_zone_files(self, urls):

        # The zone files will be saved in a sub-directory
        output_directory = self.working_directory + "/zonefiles"

        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        # Download the zone files one by one
        #for link in urls:
            #self.download_one_zone(link, output_directory)
        
        pool = Pool(cpu_count())
        pool.map(self.download_one_zone, urls)
        pool.close()
        pool.join()



def main():

    client = ICANN()
    client._authenticate()
    links = client.get_zone_links()
    start_time = datetime.datetime.now()
    client.download_zone_files(links)
    end_time = datetime.datetime.now()

    print("{0}: DONE DONE. Completed downloading all zone files. Time spent: {1}".format(str(end_time), (end_time-start_time)))


if __name__ == "__main__":
    main()
