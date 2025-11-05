curl -X PUT "http://localhost:5601/api/spaces/space/default" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  -u elastic:elastic \
  -d '{
    "id": "default",
    "name": "Default",
    "disabledFeatures": [],
    "solution": "es"
  }'
