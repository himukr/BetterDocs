#!/usr/bin/env bash

echo "started delete task"

for f in `find .. $1 -name 'deleteIndexFiles'`
do
    while IFS='' read -r line || [[ -n "$line" ]]; do
        echo "query is $line"
        curl -XDELETE 'http://localhost:9200/filemetadata/typefilemetadata/_query' --data "$line"


        replacedFileWith="\"fileName\":"
        changedLine="${line//\"file\":/$replacedFileWith}"
        echo "changed query is $changedLine"

        curl -XDELETE 'http://localhost:9200/sourcefile/typesourcefile/_query' --data "$changedLine"
        curl -XDELETE 'http://localhost:9200/typereferences/javaexternal/_query' --data "$changedLine"
        curl -XDELETE 'http://localhost:9200/typereferences/javainternal/_query' --data "$changedLine"

    done < "$f"
done