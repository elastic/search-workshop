---
slug: whats-new-elastic-search-9-2
id: 81j5xntmmbkp
type: challenge
title: What's New Elastic Search 9.2
teaser: Let's take a look at What's New in Elastic Search 9.2
tabs:
- id: bqwokexzlx7f
  title: Terminal 1
  type: terminal
  hostname: search-workshop
  cmd: sudo -i -u ubuntu bash -c "cd /home/ubuntu/search-workshop && exec bash"
- id: cnmrcifxm592
  title: Terminal 2
  type: terminal
  hostname: search-workshop
  cmd: sudo -i -u ubuntu bash -c "cd /home/ubuntu/search-workshop && exec bash"
- id: fmeeyd7y2grm
  title: Elastic
  type: service
  hostname: search-workshop
  path: /app/discover#/
  port: 5601
  custom_request_headers:
  - key: Content-Security-Policy
    value: 'script-src ''self''; worker-src blob: ''self''; style-src ''unsafe-inline''
      ''self'''
  - key: Authorization
    value: Basic ZWxhc3RpYzplbGFzdGlj
  custom_response_headers:
  - key: Content-Security-Policy
    value: 'script-src ''self''; worker-src blob: ''self''; style-src ''unsafe-inline''
      ''self'''
- id: zjy1pltw6qib
  title: Website
  type: service
  hostname: search-workshop
  path: /
  port: 5000
difficulty: ""
timelimit: 0
enhanced_loading: null
---
# Know Your Data
===

## What You'll Learn

- **Simple Data Ingestion**: Loading data into Elastic is straightforward
- **Immediate Availability**: Data becomes searchable instantly
- **Powerful Analytics**: Complex questions get answered quickly through search and aggregations
- **Visual Exploration**: Elastic makes data analysis intuitive

## Overview

This workshop is using a local deployment of Elastic, deployed with [Start Local](https://www.elastic.co/downloads/elasticsearch).  If you want to run these exercises on your laptop, `Start Local` is a great way to get Elastic setup quickly.

For this workshop, we are going to be looking at U.S. domestic flight data from 2019-June 2025.  Some of it has been preloaded into a set of `flights` indices. But it's up to you to load the latest July 2025 data and to understand the schema so you can hypothesize questions to ask.

## Instructions

### Step 1: Load July 2025 Data

Let's load the latest data released by the Bureau of Transportation Statistics (BTS).  It takes about *30 seconds* to complete.

Copy this command, paste it in [Terminal 1](tab-0), then hit return:

```bash
./1-load-flights.sh
```

This loads 631,428 flights from July 2025 into the `flights-2025-07` index.

### Step 2: Explore Your Data

1. Open the [Elastic](tab-2) tab and navigate to **Discover**
2. Ensure the `flights` Data View is selected
3. Click the Time Picker in the top right and select `Last 7 years`

### Step 3: Answer These Questions

Use Elastic's search and visualization tools to answer the following questions:

- How many flights were there in the past 7 years?
- Which airline had the most flights?
- Which airline had the most cancellations?

Once you've adjusted the time window to the `Last 7 years`, the number of documents is displayed above the results.

Then click the `Reporting_Airline` filter on the left to see a sample rollup.  The values shown are the U.S. domestic carrier IATA codes (e.g., UA = United Airlines).

In the next section, we'll JOIN the formal name of the airline with their IATA code to make it easier to read.  For now, click `Visualize` to see a Lens with the complete data over 7 years.

To see which airline had the most cancellations from that Lens visualization, add the following to the search box `Cancelled : true `.

Note:  You can also try answering these questions using ES|QL.  Go back to Discover and click `Try ES|QL`.

## Key Takeaway

This demonstrates how Elastic can help you quickly transform raw data into valuable insights.  On to the next section, where we'll use JOINs to enhance our data*!*

# ES|QL Joins with Flight Data
===

## Challenge Overview

Now, let's explore LOOKUP JOINs, a new feature in Elasticsearch. The `flights` index doesn't contain the airline names, just their short codes. For example, "UA" is United Airlines, but which airline is "WN"?  Traditionally, we'd have to reimport the flights with their full airline name in order to use them in our analysis.  Now with JOINs we don't have to, we can simply import the new data and then JOIN it to the existing index!

Elastic has been preloaded with 7 years of flight data already (2019 - June 2025).  We previously ran a script to load July 2025 flight data to show how easy ingestion is.

## Understanding the Data Structure

The `airlines` index contains the following fields with short codes and full names:

- `Reporting_Airline`: Short airline code (e.g., "AA", "UA", "DL")
- `Airline_Name`: Full airline name (e.g., "American Airlines", "United Airlines")

The `flights` data view (which matches all `flights-*` indices) contains the historical flight data with airline codes:

- `Reporting_Airline`: Links to the airlines index
- Flight performance data (delays, cancellations, etc.)
- Geographic and timing information

## Instructions

### Step 1: Access ES|QL in Kibana

Open [Elastic](tab-2) and navigate to the **ES|QL** interface in Discover. This is where we'll write our JOIN queries to combine airline names with flight data.

Try a simple ES|QL command to start:

```sql
FROM flights-*
| STATS COUNT()
```

You should get the same count from the previous exercise:  42,208,549

### Step 2: Find Which Airline Canceled the Most Flights

**What's your guess?**

Copy this ES|QL query into the query text area of Discover:

```sql
FROM flights-*
| WHERE Cancelled : true
| LOOKUP JOIN airlines ON Reporting_Airline
| STATS flight_count = COUNT() BY Airline_Name
| SORT flight_count DESC
```

Notice how the **LOOKUP JOIN** connects the airline codes with full airline names.

### Step 3: Discover the #1 Reason for Flight Cancellations

Let's query the data to find the #1 reason for flight cancellations.  Each flight that gets canceled is classified as one of the following:

|Code|Description|
|:----:|-------|
|A|Carrier|
|B|Weather|
|C|National Air System|
|D|Security|

**What's your guess for the #1 reason why flights are cancelled?**

Find the real answer with this query:

```sql
FROM flights-*
| WHERE Cancelled : true AND CancellationCode IS NOT NULL
| STATS cancellation_count = COUNT() BY CancellationCode
| SORT cancellation_count DESC
```

**What's the actual answer after looking at the data?**

The first answer isn't a surprise, but the second answer is.  The “Security” category has historically been very small in terms of Delays, which suggests Cancellations should similarly be small, yet that's not what the data shows.  There could be some quirks in classification from the airlines, who self-report this data to varying degrees of agreement on the definitions.  There could also be reporting incentives or operational fault-lines that could explain why Security is comparatively large.  Either way, as they say in Latin, "id est in notitia", or tranlated "it's in the data".

Surprises like this are one of the most fascinating aspects of using Elastic to explore a data set.  As the famous Jim Barksdale (the former CEO of Netscape) once said, "If we have data, let's look at data. If all we have are opinions, let's go with mine."

## What You're Learning

This challenge demonstrated:

* **ES|QL LOOKUP JOINs**: How to combine data from multiple indexes without re-importing

* **Real-Time Insights**: Analyzing cancellation patterns across airlines and reasons

* **Efficient Analytics**: SQL-like syntax for complex data relationships

You've just experienced how ES|QL JOINs make complex data relationships simple and powerful in Elastic.  Onto the next section, semantic search*!*

# Semantic Search with Airline Contracts
===

## Challenge Overview

When working with unstructured documents like contracts and policies, traditional keyword search often falls short. Semantic search allows you to find relevant information using natural language queries, even when the exact terms don't match. In this challenge, you'll load airline contract PDFs and experience the power of Elastic's semantic search capabilities.

We have carrier contracts from American, United, Southwest, and Delta airlines in PDF format that need to be processed for semantic search.

## Understanding Keyword Search vs Semantic Search

**Keyword Search**: Matches exact words and phrases. Searching for "baggage fees" would only find documents containing those specific terms and may miss documents for "luggage" or "bags".

**Semantic Search**: Understands meaning and context, to deduce *intent*. Searching for "baggage fees" should also find relevant content about "luggage charges," "bag costs," or "carry-on pricing" even without exact keyword matches.

Our web application will allow you to compare both search methods to see the differences in results.

## Instructions

### Step 1: Load the Contract Data

Let's load the airline contract PDFs.  It takes around *3 minutes* to complete.

Copy this command, paste it in [Terminal 1](tab-0), then hit return:

```copy
./2-load-contracts.sh
```

This script will:
 - Read each PDF file
- Base64-encode it
- Send it to Elastic

This script uses the [attachment processor](https://www.elastic.co/docs/reference/enrich-processor/attachment) in Elastic to extract text from the PDF and index that text into the `contracts` index using vectors so we can semantically search it.  Before you index large amounts of text using vectors, you have to "chunk" the text into segments based on what the model supports.  Elastic is natively handling the chunking for us, which you can read more about in Elastic's [docs](https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/semantic-text#auto-text-chunking).  We're going to use [ELSER](https://www.elastic.co/docs/explore-analyze/machine-learning/nlp/ml-nlp-elser) for the model.  ELSER is trained by Elastic and it enables you to perform searches based on the contextual meaning and intent behind a query, rather than just exact keyword matches.

### Step 2: Start the Website App

First, we need to start the sample web app.  Open [Terminal 2](tab-1) and copy and paste the following:

```copy
./3-start-website.sh
```

Then open the web application in the tab labeled [Website](tab-3).  This interface allows you to:

- Perform keyword searches against the airline contracts
- Perform semantic searches against the airline contracts
- See how ELSER understands the meaning behind your queries

### Step 3: Try Semantic Search Examples

Use the web app to test these search queries and observe how Keyword queries compare to Semantic queries:

- "baggage" vs "luggage"
- "cancellation policy"
- "pet policy"

For each query, notice:
- How semantic search finds relevant content even without exact keyword matches
- The quality and relevance of results compared to traditional keyword search
- How different airlines handle similar policies

### Step 4: Compare Search Methods in Kibana Dev Tools

Let's see the difference between keyword and semantic search directly in Kibana Dev Tools.

Open Kibana Dev Tools and try these two searches:

**Traditional Keyword Search:**
```
GET contracts/_search
{
  "query": {
    "match": {
      "attachment.content": "baggage fees"
    }
  },
  "size": 2
}
```

**Semantic Search:**
```
GET contracts/_search
{
  "query": {
    "semantic": {
      "field": "semantic_content",
      "query": "What are the baggage fees for checked luggage?"
    }
  },
  "size": 2
}
```

Notice how semantic search finds relevant content about luggage costs even without exact keyword matches.

## What You're Learning

This challenge demonstrates:

- **Powerful Document Processing**: How Elasticsearch can automatically process complex PDFs and prepare them for search
- **Semantic Understanding**: ELSER's ability to understand meaning and context, not just keywords
- **Real-World Applications**: How semantic search improves user experience when searching through policies, contracts, and documentation
- **Chunking Strategy**: How breaking large documents into smaller segments improves search relevance and performance

## Key Takeaways

- Semantic search dramatically improves the user experience when searching unstructured documents
- ELSER embeddings allow Elasticsearch to understand meaning and context beyond exact keyword matching
- Proper document chunking enhances search relevance and performance
- The same technology can be applied to any collection of documents, contracts, policies, or knowledge bases
- Users can ask questions in natural language and get relevant answers even when using different terminology

## Real-World Impact

This type of semantic search capability transforms how organizations handle:
- Customer service inquiries about policies
- Employee access to internal documentation
- Legal document review and analysis
- Knowledge management systems
- Regulatory compliance searches

You've just experienced how Elasticsearch makes complex document search both powerful and accessible!

# Building AI Agents with Agent Builder
===

## Challenge Overview

Now let's bring everything together! In the previous challenges, you've experienced the power of semantic search for airline contracts and ES|QL LOOKUP JOINs for flight analysis. Agent Builder provides a complete set of capabilities to significantly simplify the approach to interacting with and building context-driven AI Agents leveraging the power of Elasticsearch.

We'll create a custom AI agent that can intelligently answer questions about both flight operations and airline policies by combining all the capabilities you've learned.

## Understanding Agent Builder

Elastic's [Agent Builder](https://www.elastic.co/docs/solutions/search/elastic-agent-builder) allows you to easily build agentic workflows with MCP.  If you're not familiar with MCP, you can read more about it on the [What is MCP](https://www.elastic.co/what-is/mcp) page.  We're going to build an experience that allows you to start chatting with your data in Elasticsearch.

The agent will be able to:
- Use semantic search to find relevant information in airline contracts
- Execute ES|QL queries with LOOKUP JOINs to analyze flight data
- Combine insights from both data sources to provide comprehensive answers

## Instructions

### Step 1: Access Agent Builder

In the [Elastic](tab-2) tab, navigate to **Agents** in the side navigation.

### Step 2: Create a Custom Tool for Flight Analysis

Click **Manage tools**, then **New Tool**. We'll create an ES|QL tool for flight cancellation analysis:

**Tool ID:**
```
flight-cancellation-analysis
```

**Description:**
```
Analyzes flight cancellations by airline using LOOKUP JOINs to provide airline names instead of codes
```

**Tool Type:** ES|QL

**ES|QL Query:**
```sql
FROM flights-*
| WHERE Cancelled : true
| LOOKUP JOIN airlines ON Reporting_Airline
| STATS cancellation_count = COUNT() BY Airline_Name
| SORT cancellation_count DESC
| LIMIT 10
```

Click **Save & Test** to validate the tool.

### Step 3: Create a Custom Aviation Assistant Agent
In the Elastic tab, navigate to Agents in the side navigation.
Click **Create a new agent** and configure:

**Agent ID:**
```
flight-ai
```
**Custom Instructions:**
```
You are a specialized assistant for the "AIr Search" website. You ONLY answer questions directly related to the US domestic flights from 2019 to 2025 or the contract of carriage for each of the big four US carriers: United, American, Delta, and Southwest.

IMPORTANT
- The data for Flights is in the data view 'flights'.  Only use this index when querying anything related to flights.
- The data for Airlines is in the index 'airlines'
- The data for each of the big four's Contract of Carriage is in the index 'contracts'

If a question is not related to these flights, airlines, or their contracts of carriage, politely decline and redirect the user to ask about these topics. Be concise but informative in your answers. Use markdown formatting for better readability.  It's OK if you don't find the answer to the users question in the provided indices; there is NO NEED to make something up.  If you do use the information in the indices provided, you MUST mention it or the ES|QL you used to get the answer.

If a user's query is vague, ask clarifying questions.  Don't say "That's a broad topic!" or anything as your first sentence that warrants an exclamation mark.  In general, you can assume their query is about US domestic flights and the contract of carriage for each of the four big US air carrier:  United, American, Delta, and Southwest.  Overall, your goal is to help them as they try to learn about these flights or contracts.   Fee free to write ample ES|QL for  use in the tools provided to you.  When users ask about airline policies, baggage fees, travel rules, or contract terms, search the 'contracts' index using semantic search to find relevant policy information.  Always provide specific, actionable answers backed by the actual data. If combining flight operations data with policy information, explain how both sources inform your response.

Keep your answers short, returning the answer in a single sentence where possible.  If you used ES|QL to answer your questions, return it in a code block.  Return all answers in markdown.  If you can show a table of the results from a query, show it in markdown.

IMPORTANT
- If a query refers to "carrier" or "airline", use the `Reporting_Airline` field in the `flights` index.
- If a query refers to "aircraft" or "airplane", use the `Tail_Number` field in the `flights` index.
- If you are querying specific fields using ES|QL, be sure to exclude NULL value from those fields (e.g., WHERE TaxiOutMin IS NOT NULL)
```
**Display Name:**
```
Flight AI
```

**Display description:**
```
Expert assistant for flight operations and airline policy questions
```


### Step 4: Assign Tools to Your Agent

In the **Tools** tab, assign:

- ✅ **platform.core.search** (for semantic search of contracts)
- ✅ **platform.core.get_document_by_id** (retrieves full documents based on ID and index name)
- ✅ **platform.core.execute_esql** (execute ES|QL statements)
- ✅ **platform.core.generate_esql** (generate ES|QL from natural language)
- ✅ **platform.core.get_index_mapping** (retrieve mappings for indices)
- ❌ **platform.core.list_indices** (list indices)
- ❌ **platform.core.index_explorer** (list indices by natural language)
- ✅ **flight-cancellation-analysis** (your custom ES|QL tool)

Click **Save and Chat**.

### Step 5: Test Your Aviation Assistant

Try these questions to see how your agent combines both data sources:

**Flight Operations Questions:**
```
Which airline has cancelled the most flights?
```

**Policy Questions:**
```
What is Southwest's baggage policies for checked luggage?
```
```
What happens if American Airlines cancels my flight?
```

**Combined Analysis Questions:**
```
United cancelled my flight. What are their cancellation policies and how do their cancellation rates compare to other airlines?
```

```
I'm flying Southwest and worried about delays. What are their compensation policies and how often do they have delays?
```

### Step 6: Observe the Agent's Reasoning

Notice how your agent:

- Automatically selects the appropriate tool (ES|QL for flight data, semantic search for policies)
- Combines insights from multiple data sources
- Provides context-aware responses that address both operational and policy aspects
- Shows its reasoning and the queries it executed

## What You're Learning

This challenge demonstrates:

**Intelligent Tool Selection**: Agent Builder comes with a set of built-in tools including a powerful search capability that selects the right index, understands the structure of that data, translates natural language into optimized semantic, hybrid or structured queries

**Context Engineering**: How agents can combine structured operational data with unstructured policy documents to provide comprehensive answers

**Custom Business Logic**: Using ES|QL tools to implement specific analytical workflows while maintaining conversational interfaces

**Multi-Modal Data Analysis**: Seamlessly working with both semantic search and analytical queries within a single conversation

## Key Takeaways

- Agent Builder makes sophisticated data analysis conversational and accessible
- Custom tools allow you to embed specific business logic (like ES|QL LOOKUP JOINs) into conversational interfaces
- Semantic search and structured queries can be combined intelligently by AI agents
- Users can ask complex questions spanning multiple data sources without knowing the underlying technical details
- The agent handles tool selection, query optimization, and result synthesis automatically

## Real-World Impact

This type of intelligent agent transforms how organizations provide:
- **Customer Service**: Agents can instantly access both operational data and policy information
- **Business Intelligence**: Complex analysis becomes as simple as asking questions
- **Employee Self-Service**: Staff can get immediate answers combining multiple data sources
- **Decision Support**: Leaders can ask nuanced questions that require multiple types of analysis

You've just built an AI agent using Elastic's [Agent Builder](https://www.elastic.co/docs/solutions/search/elastic-agent-builder) that demonstrates the future of data interaction*!*
