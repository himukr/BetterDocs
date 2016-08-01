#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This script is a helper script to upload spark generated output to elasticSearch.
# DISCLAIMER: There are no defensive checks please use it carefully.

echo "Clearing kodebeagle related indices from elasticsearch."
curl -XDELETE 'http://192.168.2.67:9200/sourcefile/'
curl -XDELETE 'http://192.168.2.67:9200/repository/'
curl -XDELETE 'http://192.168.2.67:9200/repotopic/'
curl -XDELETE 'http://192.168.2.67:9200/filemetadata/'
curl -XDELETE 'http://192.168.2.67:9200/typereferences/'


# Updating mappings and types for kodebeagle index.

curl -XPUT '192.168.2.67:9200/sourcefile' -d '{
 "mappings": {
    "typesourcefile" : {
        "properties" : {
           "repoId" : { "type" : "integer", "index" : "not_analyzed" },
           "fileName": { "type": "string", "index" : "not_analyzed" },
           "fileContent": { "type": "string", "index" : "no" }
        }
      }
    }
}'

curl -XPUT '192.168.2.67:9200/repotopic' -d '{
    "mappings" : {
      "typerepotopic" : {
        "properties" : {
          "defaultBranch" : {
            "type" : "string"
          },
          "fork" : {
            "type" : "boolean"
          },
          "id" : {
            "type" : "long"
          },
          "language" : {
            "type" : "string"
          },
          "login" : {
            "type" : "string"
          },
          "name" : {
            "type" : "string"
          },
	  "files":   { 
              "type":       "object",
              "properties": {
                "file":     { "type": "string" },
                "klscore":    { "type": "double" }
              }
          },
	  "topic":   { 
              "type":         "object",
              "properties": {
                "term":     { "type": "string" },
                "freq":    { "type": "long" }
              }
          }
        }
      }
    }
  }'

 curl -XPUT '192.168.2.67:9200/filemetadata' -d '{
 "mappings": {
   "typefilemetadata": {
     "properties": {
       "superTypes": {
        "type": "nested",
         "properties": {
            "typeName":{
              "index": "not_analyzed",
              "type": "string"
            },
            "superTypes": {
              "index": "not_analyzed",
               "type": "string"
            }
         }
       },
       "repoId": {
         "index": "no",
         "type": "long"
       },
       "fileName": {
         "index": "not_analyzed",
         "type": "string"
       },
       "methodTypeLocation": {
         "properties": {
           "argTypes": {
             "index": "no",
             "type": "string"
           },
           "loc": {
             "index": "no",
             "type": "string"
           },
           "method": {
             "index": "no",
             "type": "string"
           },
           "id": {
             "index": "no",
             "type": "long"
           }
         }
       },
       "methodDefinitionList": {
         "properties": {
           "argTypes": {
             "index": "no",
             "type": "string"
           },
           "loc": {
             "index": "no",
             "type": "string"
           },
           "method": {
             "index": "no",
             "type": "string"
           }
         }
       },
       "typeLocationList": {
         "properties": {
           "loc": {
             "index": "no",
             "type": "string"
           },
           "id": {
             "index": "no",
             "type": "long"
           }
         }
       },
       "internalRefList": {
         "properties": {
           "childLine": {
             "index": "no",
             "type": "string"
           },
           "parentLine": {
             "index": "no",
             "type": "string"
           }
         }
       },
       "externalRefList": {
         "properties": {
           "fqt": {
             "index": "no",
             "type": "string"
           },
           "id": {
             "index": "no",
             "type": "long"
           }
         }
       },
       "fileTypes": {
         "properties": {
           "loc": {
             "index": "no",
             "type": "string"
           },
           "underlying": {
             "properties": {
               "loc": {
                 "type": "string"
               },
               "fileType": {
                 "index": "not_analyzed",
                 "type": "string"
               }
             }
           },
           "fileType": {
             "index": "not_analyzed",
             "type": "string"
           }
         }
       }
     }
   }
 }
}'

curl -XPUT '192.168.2.67:9200/typereferences' -d '{
"mappings": {

    "javaexternal": {
        "properties": {
            "repoId": {
                "type": "long"
            },
            "score": {
                "type": "integer"
            },
            "types": {
                "type": "nested",
                "include_in_parent": true,
                "properties": {
                    "typeName": {
                        "index": "not_analyzed",
                        "type": "string"
                    },
                    "lines": {
                        "properties": {
                            "endColumn": {
                                "type": "long"
                            },
                            "startColumn": {
                                "type": "long"
                            },
                            "lineNumber": {
                                "type": "long"
                            }
                        }
                    },
                    "properties": {
                        "type": "nested",
                        "include_in_parent": true,
                        "properties": {
                            "propertyName": {
                                "index": "not_analyzed",
                                "type": "string"
                            },
                            "lines": {
                                "properties": {
                                    "endColumn": {
                                        "type": "long"
                                    },
                                    "startColumn": {
                                        "type": "long"
                                    },
                                    "lineNumber": {
                                        "type": "long"
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "file": {
                "index": "no",
                "type": "string"
            },
            "language": {
                "index": "not_analyzed",
                "type": "string"
            }
        }
    }
}
}'

for file in `find $1 -type f`
do
    echo "uploading $file to elasticsearch."
    curl -s -XPOST '192.168.2.67:9200/_bulk' --data-binary '@'$file >/dev/null
done

