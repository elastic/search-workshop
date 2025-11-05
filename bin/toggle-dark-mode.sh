#!/bin/bash
# Enable Dark mode
curl -X PUT "http://localhost:5601/api/saved_objects/config/9.2.0" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  -u elastic:$ES_LOCAL_PASSWORD \
  -d '{
    "attributes": {
      "theme:darkMode": true
    }
  }'
