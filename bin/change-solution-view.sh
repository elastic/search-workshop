curl -X PUT "http://localhost:5601/api/spaces/space/default" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  -u elastic:$ES_LOCAL_PASSWORD \
  -d '{
    "id": "default",
    "name": "Default",
    "disabledFeatures": [],
    "solution": "es"
  }'
