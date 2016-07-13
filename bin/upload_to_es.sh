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
curl -XDELETE 'http://localhost:9200/kodebeagle/'
curl -XDELETE 'http://localhost:9200/sourcefile/'
curl -XDELETE 'http://localhost:9200/repository/'
curl -XDELETE 'http://localhost:9200/importsmethods/'
curl -XDELETE 'http://localhost:9200/statistics/'
curl -XDELETE 'http://localhost:9200/typereferences'
curl -XDELETE 'http://localhost:9200/filemetadata'
curl -XDELETE 'http://localhost:9200/repotopic'


# create a kodebeagle index
curl -XPUT 'http://localhost:9200/kodebeagle/'

# Updating mappings and types for kodebeagle index.

curl -XPUT 'localhost:9200/kodebeagle/custom/_mapping' -d '
{
    "custom" : {
        "properties" : {
                "file" : { "type" : "string", "index" : "no" },
                "tokens": {
                        "type": "nested",
                        "include_in_parent": true,
                        "properties": {
                            "importName": {
                                "type": "string",
                                "index" : "not_analyzed"
                            },
                            "importExactName": {
                                "type": "string",
                                "index" : "no"
                            },
                            "lineNumbers": {
                                "type": "long",
                                "index" : "no"
                            }
                        }
                    },
                "score" : { "type" : "integer", "index" : "not_analyzed" }
        }
    }
}'

curl -XPUT 'localhost:9200/sourcefile' -d '{
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

curl -XPUT 'localhost:9200/repotopic' -d '{
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


curl -XPUT 'http://localhost:9200/importsmethods/' -d '{
    "mappings": {
        "typeimportsmethods": {
            "properties": {
                "file": {
                    "type": "string",
                    "index": "no"
                },
                "tokens": {
                    "type": "nested",
                    "include_in_parent": true,
                    "properties": {
                        "importName": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "importExactName": {
                            "type": "string",
                            "index": "no"
                        },
                        "lineNumbers": {
                            "properties": {
                                "endColumn": {
                                    "type": "long"
                                },
                                "lineNumber": {
                                    "type": "long"
                                },
                                "startColumn": {
                                    "type": "long"
                                }
                            }
                        },
                        "methodAndLineNumbers": {
                            "type": "nested",
                            "include_in_parent": true,
                            "properties": {
                                "lineNumbers": {
                                    "properties": {
                                        "endColumn": {
                                            "type": "long"
                                        },
                                        "lineNumber": {
                                            "type": "long"
                                        },
                                        "startColumn": {
                                            "type": "long"
                                        }
                                    }
                                },
                                "methodName": {
                                    "type": "string",
                                    "index": "not_analyzed"
                                }
                            }
                        }
                    }
                },
                "repoId": {
                    "type": "long"
                },
                "language": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "score": {
                    "type": "integer",
                    "index": "not_analyzed"
                }
            }
        }
    }
}'

curl -XPUT 'http://localhost:9200/typereferences/' -d '{
    "mappings": {
        "javaexternal": {
            "properties": {
                "file": {
                    "type": "string",
                    "index": "no"
                },
                "types": {
                    "type": "nested",
                    "include_in_parent": true,
                    "properties": {
                        "typeName": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "lines": {
                            "properties": {
                                "endColumn": {
                                    "type": "long"
                                },
                                "lineNumber": {
                                    "type": "long"
                                },
                                "startColumn": {
                                    "type": "long"
                                }
                            }
                        },
                        "properties": {
                            "type": "nested",
                            "include_in_parent": true,
                            "properties": {
                                "lines": {
                                    "properties": {
                                        "endColumn": {
                                            "type": "long"
                                        },
                                        "lineNumber": {
                                            "type": "long"
                                        },
                                        "startColumn": {
                                            "type": "long"
                                        }
                                    }
                                },
                                "propertyName": {
                                    "type": "string",
                                    "index": "not_analyzed"
                                }
                            }
                        }
                    }
                },
                "repoId": {
                    "type": "long"
                },
                "language": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "score": {
                    "type": "integer",
                    "index": "not_analyzed"
                }
            }
        }
    }
}'


for f in `find $1 -name 'part*'`
do
    echo "uploading $f to elasticsearch."
    curl -s -XPOST 'localhost:9200/_bulk' --data-binary '@'$f >/dev/null
done
