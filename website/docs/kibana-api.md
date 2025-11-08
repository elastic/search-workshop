---
title: Work with Elastic Agent Builder using the APIs
description: This page provides a quick overview of the main Kibana API endpoints for Elastic Agent Builder. For complete details including all available parameters,...
url: https://www.elastic.co/docs/solutions/search/agent-builder/kibana-api
---

# Work with Elastic Agent Builder using the APIs

This page provides a quick overview of the main Kibana API endpoints for Elastic Agent Builder. For complete details including all available parameters, request/response schemas, and error handling, refer to the [Kibana API reference](https://www.elastic.co/docs/api/doc/kibana/group/endpoint-agent-builder).
These APIs allow you to programmatically work with the Elastic Agent Builder abstractions.

## Using the APIs

The examples in this documentation use Dev Tools [Console](https://www.elastic.co/docs/explore-analyze/query-filter/tools/console) syntax.
```json
```

To use these APIs with tools like `curl`, replace the `kbn://` protocol with your Kibana URL.
<note>
  Set the required environment variables before running curl commands:
  ```bash
  export KIBANA_URL="your-kibana-url"
  export API_KEY="your-api-key"
  ```
</note>

```bash
curl -X GET "https://${KIBANA_URL}/api/agent_builder/tools" \
     -H "Authorization: ApiKey ${API_KEY}"
```

<tip>
  To generate API keys, search for `API keys` in the [global search bar](https://www.elastic.co/docs/explore-analyze/find-and-organize/find-apps-and-objects).
  [Learn more](https://www.elastic.co/docs/solutions/search/search-connection-details).
</tip>


### Working with Spaces

To run APIs in non-default [spaces](https://www.elastic.co/docs/deploy-manage/manage-spaces), you must include the space identifier in the URL when making API calls with `curl` or other external tools. Insert `/s/<space_name>` before `/api/agent_builder` in your requests.
For example, to list tools in a space named `my-space`:
```bash
curl -X GET "https://${KIBANA_URL}/s/my-space/api/agent_builder/tools" \
     -H "Authorization: ApiKey ${API_KEY}"
```

The default space does not require the `/s/default` prefix.
Dev Tools [Console](https://www.elastic.co/docs/explore-analyze/query-filter/tools/console) automatically uses your current space context and does not require the `/s/<space_name>` prefix.

## Available APIs


### Tools

**Example:** List all tools
This example uses the [list tools API](https://www.elastic.co/docs/api/doc/kibana/operation/operation-get-agent-builder-tools).
<tab-set>

  <tab-item title="Console">

    ```json
    ```
  </tab-item>

  <tab-item title="curl">

    ```bash
    curl -X GET "https://${KIBANA_URL}/api/agent_builder/tools" \
         -H "Authorization: ApiKey ${API_KEY}"
    ```

    <tip>
      If you're using Spaces, you need to prefix `/api/agent_builder` with `/s/<space_name>`. Refer to [Working with Spaces](#working-with-spaces).
    </tip>
  </tab-item>
</tab-set>

**Example:** Create a tool
This example uses the [create a tool API](https://www.elastic.co/docs/api/doc/kibana/operation/operation-post-agent-builder-tools).
<tab-set>

  <tab-item title="Console">

    ```json

    {
      "id": "example-esql-tool",
      "type": "esql",
      "description": "An ES|QL query tool for analyzing financial trades with time filtering",
      "tags": ["analytics", "finance", "updated"],
      "configuration": {
        "query": "FROM financial_trades | WHERE execution_timestamp >= ?startTime | STATS trade_count=COUNT(*), avg_price=AVG(execution_price) BY symbol | SORT trade_count DESC | LIMIT ?limit",
        "params": {
          "startTime": {
            "type": "date",
            "description": "Start time for the analysis in ISO format"
          },
          "limit": {
            "type": "integer",
            "description": "Maximum number of results to return"
          }
        }
      }
    }
    ```
  </tab-item>

  <tab-item title="curl">

    ```bash
    curl -X POST "https://${KIBANA_URL}/api/agent_builder/tools" \
         -H "Authorization: ApiKey ${API_KEY}" \
         -H "kbn-xsrf: true" \
         -H "Content-Type: application/json" \
         -d '{
           "id": "example-esql-tool",
           "type": "esql",
           "description": "Example ES|QL query tool for analyzing financial trades with time filtering",
           "tags": ["analytics", "finance"],
           "configuration": {
             "query": "FROM financial_trades | WHERE execution_timestamp >= ?startTime | STATS trade_count=COUNT(*), avg_price=AVG(execution_price) BY symbol | SORT trade_count DESC | LIMIT ?limit",
             "params": {
               "startTime": {
                 "type": "date",
                 "description": "Start time for the analysis in ISO format"
               },
               "limit": {
                 "type": "integer",
                 "description": "Maximum number of results to return"
               }
             }
           }
         }'
    ```

    <tip>
      If you're using Spaces, you need to prefix `/api/agent_builder` with `/s/<space_name>`. Refer to [Working with Spaces](#working-with-spaces).
    </tip>
  </tab-item>
</tab-set>

**Example:** Get a tool by ID
This example uses the [get a tool by ID API](https://www.elastic.co/docs/api/doc/kibana/operation/operation-get-agent-builder-tools-id).
<tab-set>

  <tab-item title="Console">

    ```json
    ```
  </tab-item>

  <tab-item title="curl">

    ```bash
    curl -X GET "https://${KIBANA_URL}/api/agent_builder/tools/{id}" \
         -H "Authorization: ApiKey ${API_KEY}"
    ```

    <tip>
      If you're using Spaces, you need to prefix `/api/agent_builder` with `/s/<space_name>`. Refer to [Working with Spaces](#working-with-spaces).
    </tip>
  </tab-item>
</tab-set>

**Example:** Delete a tool by ID
This example uses the [delete a tool by ID API](https://www.elastic.co/docs/api/doc/kibana/operation/operation-delete-agent-builder-tools-id).
<tab-set>

  <tab-item title="Console">

    ```json
    ```
  </tab-item>

  <tab-item title="curl">

    ```bash
    curl -X DELETE "https://${KIBANA_URL}/api/agent_builder/tools/{id}" \
         -H "Authorization: ApiKey ${API_KEY}" \
         -H "kbn-xsrf: true"
    ```

    <tip>
      If you're using Spaces, you need to prefix `/api/agent_builder` with `/s/<space_name>`. Refer to [Working with Spaces](#working-with-spaces).
    </tip>
  </tab-item>
</tab-set>

**Example:** Update a tool by ID
This example uses the [update a tool API](https://www.elastic.co/docs/api/doc/kibana/operation/operation-put-agent-builder-tools-toolid).
<tab-set>

  <tab-item title="Console">

    ```json

    {
      "description": "Updated ES|QL query tool for analyzing financial trades with time filtering",
      "tags": ["analytics", "finance", "updated"],
      "configuration": {
        "query": "FROM financial_trades | WHERE execution_timestamp >= ?startTime | STATS trade_count=COUNT(*), avg_price=AVG(execution_price) BY symbol | SORT trade_count DESC | LIMIT ?limit",
        "params": {
          "startTime": {
            "type": "date",
            "description": "Start time for the analysis in ISO format"
          },
          "limit": {
            "type": "integer",
            "description": "Maximum number of results to return"
          }
        }
      }
    }
    ```
  </tab-item>

  <tab-item title="curl">

    ```bash
    curl -X PUT "https://${KIBANA_URL}/api/agent_builder/tools/{toolId}" \
         -H "Authorization: ApiKey ${API_KEY}" \
         -H "kbn-xsrf: true" \
         -H "Content-Type: application/json" \
         -d '{
           "description": "Updated ES|QL query tool for analyzing financial trades with time filtering",
           "tags": ["analytics", "finance", "updated"],
           "configuration": {
             "query": "FROM financial_trades | WHERE execution_timestamp >= ?startTime | STATS trade_count=COUNT(*), avg_price=AVG(execution_price) BY symbol | SORT trade_count DESC | LIMIT ?limit",
             "params": {
               "startTime": {
                 "type": "date",
                 "description": "Start time for the analysis in ISO format"
               },
               "limit": {
                 "type": "integer",
                 "description": "Maximum number of results to return"
               }
             }
           }
         }'
    ```

    <tip>
      If you're using Spaces, you need to prefix `/api/agent_builder` with `/s/<space_name>`. Refer to [Working with Spaces](#working-with-spaces).
    </tip>
  </tab-item>
</tab-set>

**Example:** Run a tool
This example uses the [execute a tool API](https://www.elastic.co/docs/api/doc/kibana/operation/operation-post-agent-builder-tools-execute).
<tab-set>

  <tab-item title="Console">

    ```json

    {
      "tool_id": "platform.core.search",
      "tool_params": {
        "query": "can you find john doe's email from the employee index?"
      }
    }
    ```
  </tab-item>

  <tab-item title="curl">

    ```bash
    curl -X POST "https://${KIBANA_URL}/api/agent_builder/tools/_execute" \
         -H "Authorization: ApiKey ${API_KEY}" \
         -H "kbn-xsrf: true" \
         -H "Content-Type: application/json" \
         -d '{
           "tool_id": "platform.core.search",
           "tool_params": {
             "query": "can you find john doe's email from the employee index?"}
           }
         }'
    ```

    <tip>
      If you're using Spaces, you need to prefix `/api/agent_builder` with `/s/<space_name>`. Refer to [Working with Spaces](#working-with-spaces).
    </tip>
  </tab-item>
</tab-set>


### Agents

**Example:** List all agents
This example uses the [list agents API](https://www.elastic.co/docs/api/doc/kibana/operation/operation-get-agent-builder-agents).
<tab-set>

  <tab-item title="Console">

    ```json
    ```
  </tab-item>

  <tab-item title="curl">

    ```bash
    curl -X GET "https://${KIBANA_URL}/api/agent_builder/agents" \
         -H "Authorization: ApiKey ${API_KEY}"
    ```

    <tip>
      If you're using Spaces, you need to prefix `/api/agent_builder` with `/s/<space_name>`. Refer to [Working with Spaces](#working-with-spaces).
    </tip>
  </tab-item>
</tab-set>

**Example:** Create an agent
This example uses the [create an agent API](https://www.elastic.co/docs/api/doc/kibana/operation/operation-post-agent-builder-agents).
<tab-set>

  <tab-item title="Console">

    ```json

    {
      "id": "new-agent-id",
      "name": "Search Index Helper",
      "description": "Hi! I can help you search the data within the indices starting with \"content-\" prefix.",
      "labels": ["custom-indices", "department-search"],
      "avatar_color": "#BFDBFF",
      "avatar_symbol": "SI",
      "configuration": {
        "instructions": "You are a custom agent that wants to help searching data using all indices starting with prefix \"content-\".",
        "tools": [
          {
            "tool_ids": [
              "platform.core.search",
              "platform.core.list_indices",
              "platform.core.get_index_mapping",
              "platform.core.get_document_by_id"
            ]
          }
        ]
      }
    }
    ```
  </tab-item>

  <tab-item title="curl">

    ```bash
    curl -X POST "https://${KIBANA_URL}/api/agent_builder/agents" \
         -H "Authorization: ApiKey ${API_KEY}" \
         -H "kbn-xsrf: true" \
         -H "Content-Type: application/json" \
         -d '{
           "id": "new-agent-id",
           "name": "Search Index Helper",
           "description": "Hi! I can help you search the data within the indices starting with \"content-\" prefix.",
           "labels": ["custom-indices", "department-search"],
           "avatar_color": "#BFDBFF",
           "avatar_symbol": "SI",
           "configuration": {
             "instructions": "You are a custom agent that wants to help searching data using all indices starting with prefix \"content-\".",
             "tools": [
               {
                 "tool_ids": [
                   "platform.core.search",
                   "platform.core.list_indices",
                   "platform.core.get_index_mapping",
                   "platform.core.get_document_by_id"
                 ]
               }
             ]
           }
         }'
    ```

    <tip>
      If you're using Spaces, you need to prefix `/api/agent_builder` with `/s/<space_name>`. Refer to [Working with Spaces](#working-with-spaces).
    </tip>
  </tab-item>
</tab-set>

**Example:** Get an agent by ID
This example uses the [get an agent by ID API](https://www.elastic.co/docs/api/doc/kibana/operation/operation-get-agent-builder-agents-id).
<tab-set>

  <tab-item title="Console">

    ```json
    ```
  </tab-item>

  <tab-item title="curl">

    ```bash
    curl -X GET "https://${KIBANA_URL}/api/agent_builder/agents/{id}" \
         -H "Authorization: ApiKey ${API_KEY}"
    ```

    <tip>
      If you're using Spaces, you need to prefix `/api/agent_builder` with `/s/<space_name>`. Refer to [Working with Spaces](#working-with-spaces).
    </tip>
  </tab-item>
</tab-set>

**Example:** Update an agent by ID
This example uses the [update an agent API](https://www.elastic.co/docs/api/doc/kibana/operation/operation-put-agent-builder-agents-id).
<tab-set>

  <tab-item title="Console">

    ```json

    {
      "name": "Search Index Helper",
      "description": "Updated description - Search for anything in \"content-*\" indices!",
      "labels": ["custom-indices", "department-search", "elastic-employees"],
      "avatar_color": "#BFDBFF",
      "avatar_symbol": "SI",
      "configuration": {
        "instructions": "You are a custom agent that wants to help searching data using all indices starting with prefix \"content-\".",
        "tools": [{
          "tool_ids": [
            "platform.core.search",
            "platform.core.list_indices",
            "platform.core.get_index_mapping",
            "platform.core.get_document_by_id"
          ]
        }]
      }
    }
    ```
  </tab-item>

  <tab-item title="curl">

    ```bash
    curl -X PUT "https://${KIBANA_URL}/api/agent_builder/agents/{id}" \
         -H "Authorization: ApiKey ${API_KEY}" \
         -H "kbn-xsrf: true" \
         -H "Content-Type: application/json" \
         -d '{
           "name": "Search Index Helper",
           "description": "Updated description - Search for anything in \"content-*\" indices!",
           "labels": ["custom-indices", "department-search", "elastic-employees"],
           "avatar_color": "#BFDBFF",
           "avatar_symbol": "SI",
           "configuration": {
             "instructions": "You are a custom agent that wants to help searching data using all indices starting with prefix \"content-\".",
             "tools": [{
               "tool_ids": [
                 "platform.core.search",
                 "platform.core.list_indices",
                 "platform.core.get_index_mapping",
                 "platform.core.get_document_by_id"
               ]
             }]
           }
         }'
    ```

    <tip>
      If you're using Spaces, you need to prefix `/api/agent_builder` with `/s/<space_name>`. Refer to [Working with Spaces](#working-with-spaces).
    </tip>
  </tab-item>
</tab-set>

**Example:** Delete an agent by ID
This example uses the [delete an agent by ID API](https://www.elastic.co/docs/api/doc/kibana/operation/operation-delete-agent-builder-agents-id).
<tab-set>

  <tab-item title="Console">

    ```json
    ```
  </tab-item>

  <tab-item title="curl">

    ```bash
    curl -X DELETE "https://${KIBANA_URL}/api/agent_builder/agents/{id}" \
         -H "Authorization: ApiKey ${API_KEY}" \
         -H "kbn-xsrf: true"
    ```

    <tip>
      If you're using Spaces, you need to prefix `/api/agent_builder` with `/s/<space_name>`. Refer to [Working with Spaces](#working-with-spaces).
    </tip>
  </tab-item>
</tab-set>


### Chat and conversations

**Example:** Chat with an agent
This example uses the [send chat message API](https://www.elastic.co/docs/api/doc/kibana/operation/operation-post-agent-builder-converse).
<tab-set>

  <tab-item title="Console">

    ```json

    {
      "input": "What is Elasticsearch?",
      "agent_id": "elastic-ai-agent"
    }
    ```
  </tab-item>

  <tab-item title="curl">

    ```bash
    curl -X POST "https://${KIBANA_URL}/api/agent_builder/converse" \
         -H "Authorization: ApiKey ${API_KEY}" \
         -H "kbn-xsrf: true" \
         -H "Content-Type: application/json" \
         -d '{
           "input": "What is Elasticsearch?",
           "agent_id": "elastic-ai-agent"}'
    ```

    <tip>
      If you're using Spaces, you need to prefix `/api/agent_builder` with `/s/<space_name>`. Refer to [Working with Spaces](#working-with-spaces).
    </tip>
  </tab-item>
</tab-set>

**Example:** Chat with an agent and stream events
This example uses the [send chat message (streaming) API](https://www.elastic.co/docs/api/doc/kibana/operation/operation-post-agent-builder-converse-async).
<tab-set>

  <tab-item title="Console">

    ```json

    {
      "input": "Hello again let's have an async chat",
      "agent_id": "elastic-ai-agent",
      "conversation_id": "<CONVERSATION_ID>"
    }
    ```
  </tab-item>

  <tab-item title="curl">

    ```bash
    curl -X POST "https://${KIBANA_URL}/api/agent_builder/converse/async" \
         -H "Authorization: ApiKey ${API_KEY}" \
         -H "kbn-xsrf: true" \
         -H "Content-Type: application/json" \
         -d '{
           "input": "Hello again let us have an async chat",
           "agent_id": "elastic-ai-agent",
           "conversation_id": "<CONVERSATION_ID>"
         }'
    ```

    <tip>
      If you're using Spaces, you need to prefix `/api/agent_builder` with `/s/<space_name>`. Refer to [Working with Spaces](#working-with-spaces).
    </tip>
  </tab-item>
</tab-set>

**Example:** List conversations
This example uses the [list conversations API](https://www.elastic.co/docs/api/doc/kibana/operation/operation-get-agent-builder-conversations).
<tab-set>

  <tab-item title="Console">

    ```json
    ```
  </tab-item>

  <tab-item title="curl">

    ```bash
    curl -X GET "https://${KIBANA_URL}/api/agent_builder/conversations" \
         -H "Authorization: ApiKey ${API_KEY}"
    ```

    <tip>
      If you're using Spaces, you need to prefix `/api/agent_builder` with `/s/<space_name>`. Refer to [Working with Spaces](#working-with-spaces).
    </tip>
  </tab-item>
</tab-set>

**Example:** Get conversation by ID
This example uses the [get conversation by ID API](https://www.elastic.co/docs/api/doc/kibana/operation/operation-get-agent-builder-conversations-conversation-id).
<tab-set>

  <tab-item title="Console">

    ```json
    ```
  </tab-item>

  <tab-item title="curl">

    ```bash
    curl -X GET "https://${KIBANA_URL}/api/agent_builder/conversations/{conversation_id}" \
         -H "Authorization: ApiKey ${API_KEY}"
    ```

    <tip>
      If you're using Spaces, you need to prefix `/api/agent_builder` with `/s/<space_name>`. Refer to [Working with Spaces](#working-with-spaces).
    </tip>
  </tab-item>
</tab-set>

**Example:** Delete conversation by ID
This example uses the [delete conversation by ID API](https://www.elastic.co/docs/api/doc/kibana/operation/operation-delete-agent-builder-conversations-conversation-id).
<tab-set>

  <tab-item title="Console">

    ```json
    ```
  </tab-item>

  <tab-item title="curl">

    ```bash
    curl -X DELETE "https://${KIBANA_URL}/api/agent_builder/conversations/{conversation_id}" \
         -H "Authorization: ApiKey ${API_KEY}" \
         -H "kbn-xsrf: true"
    ```

    <tip>
      If you're using Spaces, you need to prefix `/api/agent_builder` with `/s/<space_name>`. Refer to [Working with Spaces](#working-with-spaces).
    </tip>
  </tab-item>
</tab-set>


### Get A2A agent card configuration

<important>
  You shouldn't use the REST APIs to interact with the A2A endpoint, apart from getting the A2A agent card configuration.
  Refer to [Agent-to-Agent (A2A) server](https://www.elastic.co/docs/solutions/search/agent-builder/a2a-server) for more information about using the A2A protocol.
</important>

**Example:** Get A2A agent card configuration
<tab-set>

  <tab-item title="Console">

    ```json
    ```
  </tab-item>

  <tab-item title="curl">

    ```bash
    curl -X GET "https://${KIBANA_URL}/api/agent_builder/a2a/{agentId}.json" \
         -H "Authorization: ApiKey ${API_KEY}"
    ```

    <tip>
      If you're using Spaces, you need to prefix `/api/agent_builder` with `/s/<space_name>`. Refer to [Working with Spaces](#working-with-spaces).
    </tip>
  </tab-item>
</tab-set>


## API reference

For the full API documentation, refer to the [Kibana API reference](https://www.elastic.co/docs/api/doc/kibana/group/endpoint-agent-builder).