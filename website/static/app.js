// State
let currentSearchMode = 'bm25';
let currentQuery = '';
let currentIndex = 'all';
let searchTimeout = null;

// DOM Elements
const searchInput = document.getElementById('searchInput');
const searchButton = document.getElementById('searchButton');
const modeButtons = document.querySelectorAll('.mode-btn');
const indexButtons = document.querySelectorAll('.index-btn');
const loadingIndicator = document.getElementById('loadingIndicator');
const errorMessage = document.getElementById('errorMessage');
const resultsContainer = document.getElementById('resultsContainer');
const themeToggle = document.getElementById('themeToggle');
const themeIcon = document.getElementById('themeIcon');
const logo = document.getElementById('logo');

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    // Initialize theme
    initializeTheme();
    
    // Restore search state from URL
    restoreSearchFromURL();
    
    // Update URL with current state (to persist defaults if no URL params)
    updateURL();
    
    // Set up event listeners
    searchButton.addEventListener('click', performSearch);
    searchInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            // Clear any pending timeout and search immediately
            if (searchTimeout) {
                clearTimeout(searchTimeout);
                searchTimeout = null;
            }
            performSearch();
        }
    });
    
    // Search as you type with debouncing
    searchInput.addEventListener('input', () => {
        // Clear any existing timeout
        if (searchTimeout) {
            clearTimeout(searchTimeout);
        }
        
        const query = searchInput.value.trim();
        
        // If input is cleared, show default results (10 documents)
        if (!query) {
            currentQuery = '';
            updateURL();
            // Load default results
            searchTimeout = setTimeout(() => {
                performSearch();
            }, 300);
            return;
        }
        
        // Debounce the search - wait 300ms after user stops typing
        searchTimeout = setTimeout(() => {
            performSearch();
        }, 300);
    });
    
    // Logo click to reset search
    logo.addEventListener('click', resetSearch);
    
    // Theme toggle
    themeToggle.addEventListener('click', toggleTheme);
    
    // Set initial active button states
    const activeModeBtn = document.querySelector('.mode-btn.active');
    if (activeModeBtn) {
        activeModeBtn.classList.remove('btn-outline-primary');
        activeModeBtn.classList.add('btn-primary');
    }
    
    const activeIndexBtn = document.querySelector('.index-btn.active');
    if (activeIndexBtn) {
        activeIndexBtn.classList.remove('btn-outline-primary');
        activeIndexBtn.classList.add('btn-primary');
    }
    
    // Mode toggle
    modeButtons.forEach(btn => {
        btn.addEventListener('click', () => {
            modeButtons.forEach(b => {
                b.classList.remove('active');
                b.classList.remove('btn-primary');
                b.classList.add('btn-outline-primary');
            });
            btn.classList.add('active');
            btn.classList.remove('btn-outline-primary');
            btn.classList.add('btn-primary');
            currentSearchMode = btn.dataset.mode;
            
            // Update URL and re-search if there's a current query
            updateURL();
            if (currentQuery) {
                performSearch();
            }
        });
    });
    
    // Index toggle
    indexButtons.forEach(btn => {
        btn.addEventListener('click', () => {
            indexButtons.forEach(b => {
                b.classList.remove('active');
                b.classList.remove('btn-primary');
                b.classList.add('btn-outline-primary');
            });
            btn.classList.add('active');
            btn.classList.remove('btn-outline-primary');
            btn.classList.add('btn-primary');
            currentIndex = btn.dataset.index;
            
            // Update URL
            updateURL();
            
            // Load documents - search if there's a query, otherwise load 10 documents
            performSearch();
        });
    });
});

async function performSearch() {
    const query = searchInput.value.trim();
    
    // Allow empty queries to load documents when index is selected
    currentQuery = query;
    hideError();
    showLoading();
    clearResults();
    
    // Update URL with search parameters
    updateURL();
    
    try {
        const response = await fetch('/api/search', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                query: query,
                type: currentSearchMode,
                index: currentIndex
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
        resultsContainer.innerHTML = `
            <div class="text-center py-5">
                <i class="bi bi-inbox display-1 text-muted"></i>
                <p class="text-muted mt-3">No results found</p>
            </div>
        `;
        return;
    }
    
    const hits = data.hits.hits;
    const total = data.hits.total;
    let html = '';
    
    hits.forEach(hit => {
        const source = hit._source || {};
        const highlight = hit.highlight || {};
        const indexName = hit._index || 'unknown';
        
        let title, url, snippets, meta;
        
        // Format results based on index type
        if (indexName === 'flights') {
            // Flights index
            const flightNum = highlight['Flight_Number']?.[0] || source.Flight_Number || '';
            const airline = highlight['Reporting_Airline']?.[0] || source.Reporting_Airline || '';
            const origin = highlight['Origin']?.[0] || source.Origin || '';
            const dest = highlight['Dest']?.[0] || source.Dest || '';
            
            title = `Flight ${flightNum || source.Flight_Number || 'N/A'}`;
            url = `${source.Origin || 'N/A'} → ${source.Dest || 'N/A'}`;
            
            // Build snippet from flight details
            const parts = [];
            if (airline) parts.push(`Airline: ${airline}`);
            if (source.DepDelayMin !== undefined) parts.push(`Departure Delay: ${source.DepDelayMin} min`);
            if (source.ArrDelayMin !== undefined) parts.push(`Arrival Delay: ${source.ArrDelayMin} min`);
            if (source.DistanceMiles) parts.push(`Distance: ${source.DistanceMiles} miles`);
            if (source.Cancelled) parts.push('Status: Cancelled');
            
            snippets = parts.length > 0 ? [parts.join(' | ')] : ['Flight information'];
            
            // Meta information
            meta = [];
            if (source.FlightID) meta.push(`Flight ID: ${source.FlightID}`);
            if (source.Tail_Number) meta.push(`Tail Number: ${source.Tail_Number}`);
            if (source['@timestamp']) {
                const date = new Date(source['@timestamp']).toLocaleDateString();
                meta.push(`Date: ${date}`);
            }
            
        } else if (indexName === 'airlines') {
            // Airlines index - two field format
            let airlineName = highlight['Airline_Name']?.[0] || '';
            let code = highlight['Reporting_Airline']?.[0] || '';
            
            // Process highlights - replace <em> tags with highlight markup if highlights exist
            if (airlineName) {
                airlineName = airlineName.replace(/<em>/g, '<mark class="result-highlight">').replace(/<\/em>/g, '</mark>');
            } else {
                airlineName = source.Airline_Name || 'Unknown Airline';
            }
            
            if (code) {
                code = code.replace(/<em>/g, '<mark class="result-highlight">').replace(/<\/em>/g, '</mark>');
            } else {
                code = source.Reporting_Airline || 'N/A';
            }
            
            title = airlineName;
            url = code;
            snippets = [];
            meta = [];
            
        } else {
            // Contracts index (original logic)
            title = highlight['attachment.title']?.[0] || 
                    source.attachment?.title || 
                    source.filename || 
                    'Untitled';
            
            url = source.filename || 'Unknown file';
            
            snippets = highlight['attachment.description'] || highlight['attachment.content'] || [];
            if (snippets.length === 0) {
                const description = source.attachment?.description;
                const content = source.attachment?.content;
                const text = description || content || '';
                if (text) {
                    snippets = [text.substring(0, 200) + (text.length > 200 ? '...' : '')];
                }
            }
            
            meta = [];
            if (source.upload_date) {
                const date = new Date(source.upload_date).toLocaleDateString();
                meta.push(`Uploaded: ${date}`);
            }
            if (source.attachment?.author) {
                meta.push(`Author: ${source.attachment.author}`);
            }
        }
        
        // Build snippet HTML - process <em> tags in snippets
        const snippetHtml = snippets.map(snippet => {
            if (typeof snippet === 'string') {
                return snippet
                    .replace(/<em>/g, '<mark class="result-highlight">')
                    .replace(/<\/em>/g, '</mark>');
            }
            return snippet;
        }).join(' ... ');
        
        // Add index badge with consistent styling
        const badgeClass = indexName === 'flights'
            ? 'badge-index badge-flights'
            : indexName === 'airlines'
                ? 'badge-index badge-airlines'
                : 'badge-index badge-contracts';
        const badgeLabelMap = {
            flights: 'Flights',
            airlines: 'Airlines',
            contracts: 'Contracts'
        };
        const badgeLabel = badgeLabelMap[indexName] ||
            indexName
                .replace(/[_-]+/g, ' ')
                .replace(/\b\w/g, (char) => char.toUpperCase());
        const indexBadge = `<span class="badge ${badgeClass}">${badgeLabel}</span>`;
        
        // Special formatting for airlines - two field format
        if (indexName === 'airlines') {
            html += `
                <div class="result-item mb-3 pb-3 border-bottom">
                    <div class="d-flex justify-content-between align-items-start mb-1">
                        <h6 class="mb-0 fw-bold">${title}</h6>
                        ${indexBadge}
                    </div>
                    <p class="mb-0 small text-muted">Airline Code: ${url}</p>
                </div>
            `;
        } else {
            html += `
                <div class="result-item mb-3 pb-3 border-bottom">
                    <div class="d-flex justify-content-between align-items-start mb-1">
                        <h6 class="mb-0 fw-bold">${escapeHtml(title)}</h6>
                        ${indexBadge}
                    </div>
                    ${snippetHtml ? `<p class="small mb-1">${snippetHtml}</p>` : ''}
                    ${meta.length > 0 ? `<small class="text-muted">${meta.join(' | ')}</small>` : ''}
                </div>
            `;
        }
    });
    
    // Add stats
    const totalValue = typeof total === 'object' ? total.value : total;
    const indicesInfo = data.searched_indices ? ` (${data.searched_indices.join(', ')})` : '';
    html += `
        <div class="text-center mt-4 pt-3 border-top">
            <p class="text-muted mb-0 small">
                <i class="bi bi-bar-chart"></i> About ${totalValue.toLocaleString()} results${indicesInfo}
            </p>
        </div>
    `;
    
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
    errorMessage.innerHTML = `<i class="bi bi-exclamation-triangle"></i> <strong>Error:</strong> ${escapeHtml(message)}`;
    errorMessage.style.display = 'block';
}

function hideError() {
    errorMessage.style.display = 'none';
}

function resetSearch() {
    // Clear search input
    searchInput.value = '';
    currentQuery = '';
    
    // Clear results
    clearResults();
    
    // Hide loading and error messages
    hideLoading();
    hideError();
    
    // Update URL (keeps type and index, removes query)
    updateURL();
    
    // Focus back on search input
    searchInput.focus();
}

function updateURL() {
    const params = new URLSearchParams();
    if (currentQuery) {
        params.set('q', currentQuery);
    }
    // Always persist search mode and index in URL
    if (currentSearchMode) {
        params.set('type', currentSearchMode);
    }
    if (currentIndex) {
        params.set('index', currentIndex);
    }
    
    const newURL = params.toString() 
        ? `${window.location.pathname}?${params.toString()}`
        : window.location.pathname;
    
    window.history.pushState({}, '', newURL);
}

function restoreSearchFromURL() {
    const params = new URLSearchParams(window.location.search);
    const query = params.get('q');
    const type = params.get('type');
    const index = params.get('index');
    
    // Restore search mode (even if no query)
    if (type && ['bm25', 'semantic', 'ai'].includes(type)) {
        currentSearchMode = type;
        // Update button states
        modeButtons.forEach(btn => {
            if (btn.dataset.mode === type) {
                btn.classList.remove('btn-outline-primary');
                btn.classList.add('btn-primary');
                btn.classList.add('active');
            } else {
                btn.classList.remove('active');
                btn.classList.remove('btn-primary');
                btn.classList.add('btn-outline-primary');
            }
        });
    }
    
    // Restore index selection (even if no query)
    if (index && ['all', 'flights', 'airlines', 'contracts'].includes(index)) {
        currentIndex = index;
        // Update button states
        indexButtons.forEach(btn => {
            if (btn.dataset.index === index) {
                btn.classList.remove('btn-outline-primary');
                btn.classList.add('btn-primary');
                btn.classList.add('active');
            } else {
                btn.classList.remove('active');
                btn.classList.remove('btn-primary');
                btn.classList.add('btn-outline-primary');
            }
        });
    }
    
    if (query) {
        // Restore search input
        searchInput.value = query;
        currentQuery = query;
        
        // Perform the search
        performSearch();
    } else {
        // If there's no query, load default results (10 documents)
        currentQuery = '';
        performSearch();
    }
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Theme management
function initializeTheme() {
    const savedTheme = localStorage.getItem('theme') || 'light';
    setTheme(savedTheme);
}

function toggleTheme() {
    const currentTheme = document.documentElement.getAttribute('data-bs-theme') || 'light';
    const newTheme = currentTheme === 'light' ? 'dark' : 'light';
    setTheme(newTheme);
    localStorage.setItem('theme', newTheme);
}

function setTheme(theme) {
    document.documentElement.setAttribute('data-bs-theme', theme);
    
    // Update icon
    if (themeIcon) {
        if (theme === 'dark') {
            themeIcon.classList.remove('bi-moon-fill');
            themeIcon.classList.add('bi-sun-fill');
            themeToggle.setAttribute('title', 'Toggle light mode');
        } else {
            themeIcon.classList.remove('bi-sun-fill');
            themeIcon.classList.add('bi-moon-fill');
            themeToggle.setAttribute('title', 'Toggle dark mode');
        }
    }
}
