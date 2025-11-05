// State
let currentSearchMode = 'bm25';
let currentQuery = '';

// DOM Elements
const searchInput = document.getElementById('searchInput');
const searchButton = document.getElementById('searchButton');
const modeButtons = document.querySelectorAll('.mode-btn');
const loadingIndicator = document.getElementById('loadingIndicator');
const errorMessage = document.getElementById('errorMessage');
const resultsContainer = document.getElementById('resultsContainer');

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    // Set up event listeners
    searchButton.addEventListener('click', performSearch);
    searchInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            performSearch();
        }
    });
    
    // Mode toggle
    modeButtons.forEach(btn => {
        btn.addEventListener('click', () => {
            modeButtons.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            currentSearchMode = btn.dataset.mode;
            
            // Re-search if there's a current query
            if (currentQuery) {
                performSearch();
            }
        });
    });
});

async function performSearch() {
    const query = searchInput.value.trim();
    
    if (!query) {
        return;
    }
    
    currentQuery = query;
    hideError();
    showLoading();
    clearResults();
    
    try {
        const response = await fetch('/api/search', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                query: query,
                type: currentSearchMode
            })
        });
        
        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.error || `HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        displayResults(data);
    } catch (error) {
        showError(error.message);
    } finally {
        hideLoading();
    }
}

function displayResults(data) {
    if (!data.hits || !data.hits.hits || data.hits.hits.length === 0) {
        resultsContainer.innerHTML = '<div class="no-results">No results found</div>';
        return;
    }
    
    const hits = data.hits.hits;
    const total = data.hits.total;
    let html = '';
    
    hits.forEach(hit => {
        const source = hit._source || {};
        const highlight = hit.highlight || {};
        
        // Extract title
        const title = highlight['attachment.title']?.[0] || 
                    source.attachment?.title || 
                    source.filename || 
                    'Untitled';
        
        // Extract content snippets - prefer description, then content
        let snippets = highlight['attachment.description'] || highlight['attachment.content'] || [];
        if (snippets.length === 0) {
            // Fallback: use description or content
            const description = source.attachment?.description;
            const content = source.attachment?.content;
            const text = description || content || '';
            if (text) {
                snippets = [text.substring(0, 200) + (text.length > 200 ? '...' : '')];
            }
        }
        
        // Build snippet HTML
        const snippetHtml = snippets.map(snippet => {
            // Clean up highlighting markers
            return snippet
                .replace(/<em>/g, '<mark class="result-highlight">')
                .replace(/<\/em>/g, '</mark>');
        }).join(' ... ');
        
        // Extract metadata
        const filename = source.filename || 'Unknown file';
        const uploadDate = source.upload_date ? 
            new Date(source.upload_date).toLocaleDateString() : '';
        const author = source.attachment?.author || '';
        
        html += `
            <div class="result-item">
                <div class="result-title">${escapeHtml(title)}</div>
                <div class="result-url">${escapeHtml(filename)}</div>
                <div class="result-snippet">${snippetHtml}</div>
                <div class="result-meta">
                    ${author ? `Author: ${escapeHtml(author)} | ` : ''}
                    ${uploadDate ? `Uploaded: ${uploadDate}` : ''}
                </div>
            </div>
        `;
    });
    
    // Add stats
    const totalValue = typeof total === 'object' ? total.value : total;
    html += `<div class="result-stats">About ${totalValue.toLocaleString()} results</div>`;
    
    resultsContainer.innerHTML = html;
}

function clearResults() {
    resultsContainer.innerHTML = '';
}

function showLoading() {
    loadingIndicator.style.display = 'block';
}

function hideLoading() {
    loadingIndicator.style.display = 'none';
}

function showError(message) {
    errorMessage.textContent = `Error: ${message}`;
    errorMessage.style.display = 'block';
}

function hideError() {
    errorMessage.style.display = 'none';
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
