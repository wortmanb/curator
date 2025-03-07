#!/bin/bash

curl -sku bret:2xqT2IO1OQ%tfMHP -XDELETE https://es-test.bwortman.us/deepfreeze-status
for repo in $(curl -sku bret:2xqT2IO1OQ%tfMHP -X GET "https://es-test.bwortman.us/_snapshot" | jq -r 'keys[] | select(test("^deepfreeze-"))'); do
  curl -sku bret:2xqT2IO1OQ%tfMHP -XDELETE "https://es-test.bwortman.us/_snapshot/$repo"
done


for bucket in $(aws s3api list-buckets --query "Buckets[?starts_with(Name, 'deepfreeze-')].Name" --output text); do
  aws s3 rb s3://$bucket --force
done


