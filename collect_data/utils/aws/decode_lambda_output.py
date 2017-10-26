from __future__ import print_function
import json,sys
from base64 import b64decode

#----
output = json.loads("".join(sys.argv[1:]))
print("\tExited with Status Code:\t",output["StatusCode"],"\n")
try:
      log = b64decode(output["LogResult"])
      print("Log output is:")
      for line in log.split(b"\n"):
            print("\t>>> ",line)      
except:
      pass
