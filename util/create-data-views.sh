curl -X POST "http://localhost:5601/api/data_views/data_view" \
  -H 'kbn-xsrf: true' \
  -H 'Content-Type: application/json' \
  -u elastic:elastic \
  -d '{
    "data_view": {
      "name": "flights",
      "title": "flights-*",
      "timeFieldName": "@timestamp"
    }
  }'
curl -X POST "http://localhost:5601/api/data_views/data_view" \
  -H 'kbn-xsrf: true' \
  -H 'Content-Type: application/json' \
  -u elastic:elastic \
  -d '{
    "data_view": {
      "name": "contracts",
      "title": "contracts",
      "timeFieldName": "@timestamp"
    }
  }'
curl -X POST "http://localhost:5601/api/data_views/data_view" \
  -H 'kbn-xsrf: true' \
  -H 'Content-Type: application/json' \
  -u elastic:elastic \
  -d '{
    "data_view": {
      "name": "airlines",
      "title": "airlines",
      "timeFieldName": "@timestamp"
    }
  }'
