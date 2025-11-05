# Deployment Guide: Fresh Environment Setup

This guide covers deploying the airline contract search system to a new, isolated Elasticsearch environment.

## Minimum Required Files

### Essential Scripts (Required)
```
├── index_pdf.py              # Main PDF indexing script
├── setup_pipeline.py         # Create ingest pipeline
├── setup_index.py            # Create index with mappings
├── requirements.txt          # Python dependencies
└── .env                      # Elasticsearch credentials (create new)
```

### PDF Documents (Required)
```
└── airline_contracts/
    ├── american-airlines-*.pdf
    ├── SouthWest_*.pdf
    ├── United_*.pdf
    └── dl-dgr-*.pdf
```

### Optional Utilities
```
├── example_queries.py        # Test semantic search
├── inspect_chunks.py         # View chunking stats
├── check_pdf.py              # Validate PDF compatibility
└── create_dataview.py        # Create Kibana data view
```

## Step-by-Step Deployment

### 1. Prerequisites

**Elasticsearch Requirements:**
- Elasticsearch version 8.11+ or Elastic Cloud deployment
- Built-in inference endpoint `.elser-2-elastic` must be available
- API key with index/ingest privileges

**Check if `.elser-2-elastic` exists:**
```bash
curl -X GET "$ES_ENDPOINT/_inference/_all" \
  -H "Authorization: ApiKey $ES_API_KEY" | grep elser
```

### 2. Create .env File

Create a new `.env` file with your Elasticsearch credentials:

```env
# Elasticsearch Connection
ES_API_KEY="your-api-key-here"
ES_ENDPOINT="https://your-cluster.es.region.gcp.cloud.es.io:443"
ES_INDEX="contracts"

# Inference Settings (optional - for reference)
ELSER_MODEL=".elser_model_2_linux-x86_64"
INFERENCE_ENDPOINT=".elser-2-elastic"
```

**How to get these values:**
- `ES_API_KEY`: Create in Kibana → Management → API Keys
- `ES_ENDPOINT`: Copy from Elastic Cloud deployment overview
- `ES_INDEX`: Choose your index name (default: "contracts")

### 3. Install Dependencies

```bash
# Create virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

**requirements.txt contents:**
```
python-dotenv==1.2.1
requests==2.32.5
PyPDF2==3.0.1
```

### 4. Setup Pipeline

Create the PDF processing pipeline:

```bash
python3 setup_pipeline.py
```

**Expected output:**
```
✅ Pipeline created/updated successfully
   Using semantic_text field - inference handled automatically
```

**What this does:**
- Creates `pdf_pipeline` ingest pipeline
- Configures PDF text extraction (attachment processor)
- Sets up automatic field copy to `semantic_content`

### 5. Create Index

Create the index with proper mappings:

```bash
python3 setup_index.py
```

**Expected output:**
```
✅ Index created successfully
   - Acknowledged: True
   - Shards acknowledged: True
```

**What this does:**
- Creates index with name from `ES_INDEX` in .env
- Adds `semantic_content` field (semantic_text type)
- Links to `.elser-2-elastic` inference endpoint
- Configures automatic chunking

### 6. Index PDFs

Index all airline contract PDFs:

```bash
python3 index_pdf.py airline_contracts/
```

**Expected output:**
```
============================================================
SUMMARY
============================================================
Total files: 4
✅ Successfully indexed: 4
============================================================
```

**What this does:**
- Extracts text from each PDF
- Automatically chunks documents (~250 words per chunk)
- Generates ELSER embeddings for each chunk
- Stores airline name extracted from filename

**Processing time:** ~30-60 seconds for all 4 PDFs

## Verification

### 1. Check Document Count

```bash
curl -X GET "$ES_ENDPOINT/contracts/_count" \
  -H "Authorization: ApiKey $ES_API_KEY"
```

**Expected:** `{"count":4,...}`

### 2. Verify Chunking (Optional)

If you copied `inspect_chunks.py`:

```bash
python3 inspect_chunks.py --stats
```

**Expected output:**
```
📊 Overall Statistics:
   Total Documents: 4
   Total Words: ~54,000
   Estimated total chunks: ~215
   Average chunks/doc: ~53
```

### 3. Test Semantic Search (Optional)

If you copied `example_queries.py`:

```bash
python3 example_queries.py --query "What are the baggage fees?" --size 2
```

**Expected:** Returns relevant sections from Delta and Southwest contracts with highlighted chunks.

## Troubleshooting

### Issue: "inference_id not found"

**Error:**
```json
{
  "error": {
    "type": "resource_not_found_exception",
    "reason": "Inference endpoint [.elser-2-elastic] not found"
  }
}
```

**Solutions:**

**Option A: Check available inference endpoints**
```bash
curl -X GET "$ES_ENDPOINT/_inference/_all" \
  -H "Authorization: ApiKey $ES_API_KEY"
```

Look for endpoints with "elser" in the name. Update `.env` with the correct endpoint.

**Option B: Use a different built-in endpoint**

If `.elser-2-elastic` doesn't exist, try:
- `.elser-2-elasticsearch`
- Check Elastic Cloud console for available ML models

Update both `setup_index.py` and `index_pdf.py`:
```python
'inference_id': '.elser-2-elasticsearch'  # or your endpoint name
```

Then recreate the index:
```bash
python3 setup_index.py --recreate
python3 index_pdf.py airline_contracts/
```

**Option C: Deploy ELSER model manually**

If no ELSER endpoints exist, deploy one via Kibana:
1. Go to Kibana → Machine Learning → Trained Models
2. Find "ELSER" model
3. Click "Deploy" and wait for allocation
4. Use the model ID in your configuration

### Issue: "Model not allocated to any nodes"

**Error:**
```json
{
  "reason": "Trained model deployment [airline-contracts-chunked] is not allocated to any nodes"
}
```

**Solution:** This means you're using a custom inference endpoint that hasn't been deployed yet.

**Quick fix:** Use the built-in endpoint instead:
```bash
# Update both files
# In setup_index.py and index_pdf.py, change:
'inference_id': '.elser-2-elastic'  # Use built-in endpoint

# Recreate and re-index
python3 setup_index.py --recreate
python3 index_pdf.py airline_contracts/
```

### Issue: PDF indexing fails

**Check PDF compatibility:**
```bash
python3 check_pdf.py airline_contracts/american-airlines-*.pdf
```

**Common causes:**
- Encrypted/password-protected PDFs
- Scanned images (need OCR)
- Corrupted files

### Issue: Low relevance scores

If semantic search returns low scores or poor results:

1. **Verify chunking is enabled:**
   ```bash
   python3 inspect_chunks.py --stats
   ```

2. **Check semantic_content field exists:**
   ```bash
   curl -X GET "$ES_ENDPOINT/contracts/_mapping" \
     -H "Authorization: ApiKey $ES_API_KEY" | grep semantic_content
   ```

3. **Re-index if needed:**
   ```bash
   python3 setup_index.py --recreate
   python3 index_pdf.py airline_contracts/
   ```

## Minimal File Checklist

**Absolute minimum to ingest PDFs:**

- ✅ `index_pdf.py`
- ✅ `setup_pipeline.py`
- ✅ `setup_index.py`
- ✅ `requirements.txt`
- ✅ `.env` (with your credentials)
- ✅ `airline_contracts/` folder with 4 PDFs

**Total:** 3 Python scripts + 1 config file + 1 requirements file + PDF folder

## Quick Start Commands

```bash
# 1. Setup environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Create .env file with your credentials
nano .env  # or use your editor

# 3. One-time setup
python3 setup_pipeline.py
python3 setup_index.py

# 4. Index PDFs
python3 index_pdf.py airline_contracts/

# Done! ✅
```

## What Gets Created in Elasticsearch

After successful deployment:

- **Index:** `contracts` (or your ES_INDEX value)
- **Documents:** 4 (one per airline contract)
- **Chunks:** ~215 (automatic, internal to semantic_text field)
- **Pipeline:** `pdf_pipeline`
- **Mappings:**
  - `filename` (keyword)
  - `airline` (keyword)
  - `upload_date` (date)
  - `attachment.*` (PDF metadata)
  - `semantic_content` (semantic_text with chunking)

## Next Steps After Deployment

1. **Test semantic search** in Kibana Dev Tools:
   ```json
   GET /contracts/_search
   {
     "query": {
       "semantic": {
         "field": "semantic_content",
         "query": "What are the baggage fees?"
       }
     }
   }
   ```

2. **Create data view** (optional):
   ```bash
   python3 create_dataview.py
   ```

3. **Run example queries** (if copied):
   ```bash
   python3 example_queries.py
   ```

## Production Considerations

- **API Key Security:** Use environment-specific .env files, never commit to git
- **Index Naming:** Use environment prefixes (e.g., `prod-contracts`, `dev-contracts`)
- **Monitoring:** Set up alerts for indexing failures
- **Backup:** Regular snapshots of the index
- **Updates:** To re-index, use `setup_index.py --recreate` first

## Support

If you encounter issues not covered here:

1. Check Elasticsearch logs in Kibana
2. Verify ELSER inference endpoint status
3. Confirm API key has proper permissions
4. Test with a single PDF first before bulk indexing
