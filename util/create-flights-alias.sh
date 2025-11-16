curl -X POST "http://localhost:9200/_aliases" \
  -H 'Content-Type: application/json' \
  -u elastic:elastic \
  -d '{
    "actions": [
      {
        "add": {
          "index": "flights-*",
          "alias": "flights"
        }
      }
    ]
  }'
