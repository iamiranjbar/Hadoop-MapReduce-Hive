#!/bin/bash
cat books.txt | 
while read a ; do
    address=$(echo $a | cut -d , -f 1)
    name=$(echo $a | cut -d , -f 2)
    curl --retry 10 --output "$name.txt" -L -H "User-Agent:Chrome/61.0" --compressed $address; 
    echo "$name.txt downloaded." ;
    hadoop fs -put "$name.txt" /data/books
    echo "$name.txt transfered to hadoop successfully." ;
done;
echo "Done!"
