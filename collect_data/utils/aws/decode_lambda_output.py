from __future__ import print_function
import json,sys
from base64 import b64decode

#----
output = json.loads("".join(sys.argv[1:]))
log = b64decode(output["LogResult"])

#----
print("\tExited with Status Code:\t",output["StatusCode"],"\n")
print("Log output is:")
for line in log.split(b"\n"):
      print("\t>>> ",line)
