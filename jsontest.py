
import json
#import pdb; pdb.set_trace()


import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--verbose', action='store_true', help='increase output verbosity')
args = parser.parse_args()

if args.verbose:
    print("Verbosity is turned on.")






data = '{ "host": "127.0.0.1", "port": 60915, "workers": 4, "ssl": { "enabled": false, "certfile": "/etc/httpd/conf/vhosts/ssl/star-dot.hastek.net.crt", "keyfile": "/etc/httpd/conf/vhosts/ssl/star-dot.hastek.net.key", "ca_certs": "/etc/httpd/conf/vhosts/ssl/star-dot.hastek.net.rootchain", }, "auth": { "enabled": true, "realm": "Short Interest Tracker", "users": { "traderjoe": "pbkdf2:512b253bc4596a8d610073d53d1dbc4d:bc996e83a3b9bd20c6b7369bd9d554e00c801b4d2d13a63f14ef33690a1909e0" "admin": "pbkdf2:512b253bc4596a8d610073d53d1dbc4d:bc996e83a3b9bd20c6b7369bd9d554e00c801b4d2d13a63f14ef33690a1909e0" } }, "poll_schedule": { "hours": [10, 15], "days": "mon-fri" }, "keep_days": 10 }'




def parse_json(data):
    try:
        parsed_data = json.loads(data)
        print(parsed_data)  # This will print the JSON data
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")



