#!/bin/bash
# Enable Dark mode
curl -X PUT "http://localhost:5601/api/saved_objects/config/9.2.1" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  -u elastic:elastic \
  -d '{
    "attributes": {
      "agentBuilder:enabled": true
    }
  }'
