// State
let currentSearchMode = 'bm25';
let currentQuery = '';
let currentIndex = 'all';
let searchTimeout = null;
let currentFilters = {}; // Active filters for flights

// DOM Elements
const searchInput = document.getElementById('searchInput');
const searchButton = document.getElementById('searchButton');
const modeButtons = document.querySelectorAll('.mode-btn');
const indexButtons = document.querySelectorAll('.index-btn');
const loadingIndicator = document.getElementById('loadingIndicator');
const errorMessage = document.getElementById('errorMessage');
const resultsContainer = document.getElementById('resultsContainer');
const facetsContainer = document.getElementById('facetsContainer');
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
            
            // Clear filters when switching indices
            currentFilters = {};
            hideFacets();
            
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
        const requestBody = {
            query: query,
            type: currentSearchMode,
            index: currentIndex
        };
        
        // Add filters if viewing flights
        if (currentIndex === 'flights' && Object.keys(currentFilters).length > 0) {
            requestBody.filters = currentFilters;
        }
        
        const response = await fetch('/api/search', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(requestBody)
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
        const rawIndexName = hit._index || currentIndex || 'unknown';
        
        // Normalize index name: flights-2019, flights-2020, etc. -> 'flights'
        // Also handle 'all' case where currentIndex might be used
        let indexName = rawIndexName;
        if (rawIndexName.startsWith('flights-')) {
            indexName = 'flights';
        } else if (currentIndex === 'flights' && (rawIndexName === 'unknown' || !rawIndexName)) {
            indexName = 'flights';
        }
        
        let title, url, snippets, meta;
        
        // Format results based on index type
        if (indexName === 'flights') {
            // Flights index
            let flightNum = highlight['Flight_Number']?.[0] || source.Flight_Number || '';
            let airline = highlight['Reporting_Airline']?.[0] || source.Reporting_Airline || '';
            let origin = highlight['Origin']?.[0] || source.Origin || '';
            let dest = highlight['Dest']?.[0] || source.Dest || '';
            
            // Process highlights - replace <em> tags with highlight markup
            flightNum = flightNum.replace(/<em>/g, '<mark class="result-highlight">').replace(/<\/em>/g, '</mark>');
            airline = airline.replace(/<em>/g, '<mark class="result-highlight">').replace(/<\/em>/g, '</mark>');
            origin = origin.replace(/<em>/g, '<mark class="result-highlight">').replace(/<\/em>/g, '</mark>');
            dest = dest.replace(/<em>/g, '<mark class="result-highlight">').replace(/<\/em>/g, '</mark>');
            
            // Fallback to source values if no highlights
            if (!highlight['Flight_Number']?.[0]) flightNum = source.Flight_Number || 'N/A';
            if (!highlight['Reporting_Airline']?.[0]) airline = source.Reporting_Airline || 'N/A';
            if (!highlight['Origin']?.[0]) origin = source.Origin || 'N/A';
            if (!highlight['Dest']?.[0]) dest = source.Dest || 'N/A';
            
            title = `Flight ${flightNum}`;
            url = `${origin} → ${dest}`;
            
            // Build snippet from flight details with icons
            const parts = [];
            if (airline) parts.push(`<i class="bi bi-airplane-engines me-1"></i>Airline: ${airline}`);
            if (source.DepDelayMin !== undefined) {
                const delayIcon = source.DepDelayMin > 0 ? 'bi-clock-history' : 'bi-clock';
                parts.push(`<i class="bi ${delayIcon} me-1"></i>Departure Delay: ${source.DepDelayMin} min`);
            }
            if (source.ArrDelayMin !== undefined) {
                const delayIcon = source.ArrDelayMin > 0 ? 'bi-clock-history' : 'bi-clock';
                parts.push(`<i class="bi ${delayIcon} me-1"></i>Arrival Delay: ${source.ArrDelayMin} min`);
            }
            if (source.DistanceMiles) parts.push(`<i class="bi bi-rulers me-1"></i>Distance: ${source.DistanceMiles} miles`);
            if (source.Cancelled) parts.push(`<i class="bi bi-x-circle-fill me-1 text-danger"></i>Status: Cancelled`);
            
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
                        <h6 class="mb-0 fw-bold">
                            <i class="bi bi-building text-primary me-2"></i>${title}
                        </h6>
                        ${indexBadge}
                    </div>
                    <p class="mb-0 small text-muted">
                        <i class="bi bi-tag-fill me-1"></i>Airline Code: ${url}
                    </p>
                </div>
            `;
        } else if (indexName === 'flights') {
            // Special formatting for flights - show route prominently
            html += `
                <div class="result-item mb-3 pb-3 border-bottom">
                    <div class="d-flex justify-content-between align-items-start mb-1">
                        <h6 class="mb-0 fw-bold">
                            <i class="bi bi-airplane text-primary me-2"></i>${title}
                        </h6>
                        ${indexBadge}
                    </div>
                    <p class="mb-1 small">
                        <i class="bi bi-geo-alt-fill text-primary me-1"></i><strong>Route:</strong> ${url}
                    </p>
                    ${snippetHtml ? `<p class="small mb-1">${snippetHtml}</p>` : ''}
                    ${meta.length > 0 ? `<small class="text-muted">
                        ${meta.map(m => {
                            if (m.includes('Flight ID')) return `<i class="bi bi-hash me-1"></i>${m}`;
                            if (m.includes('Tail Number')) return `<i class="bi bi-tag me-1"></i>${m}`;
                            if (m.includes('Date')) return `<i class="bi bi-calendar3 me-1"></i>${m}`;
                            return m;
                        }).join(' | ')}
                    </small>` : ''}
                </div>
            `;
        } else {
            html += `
                <div class="result-item mb-3 pb-3 border-bottom">
                    <div class="d-flex justify-content-between align-items-start mb-1">
                        <h6 class="mb-0 fw-bold">
                            <i class="bi bi-file-earmark-text text-primary me-2"></i>${escapeHtml(title)}
                        </h6>
                        ${indexBadge}
                    </div>
                    ${snippetHtml ? `<p class="small mb-1">${snippetHtml}</p>` : ''}
                    ${meta.length > 0 ? `<small class="text-muted">
                        ${meta.map(m => {
                            if (m.includes('Uploaded')) return `<i class="bi bi-calendar3 me-1"></i>${m}`;
                            if (m.includes('Author')) return `<i class="bi bi-person me-1"></i>${m}`;
                            return m;
                        }).join(' | ')}
                    </small>` : ''}
                </div>
            `;
        }
    });
    
    // Add warnings if any indices failed
    if (data.warnings && data.warnings.length > 0) {
        html += `
            <div class="alert alert-warning mt-3" role="alert">
                <i class="bi bi-exclamation-triangle"></i> <strong>Warning:</strong> ${data.warnings.join('; ')}
            </div>
        `;
    }
    
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
    
    // Display facets if viewing flights and aggregations are available
    if (currentIndex === 'flights' && data.aggregations) {
        displayFacets(data.aggregations);
    } else {
        hideFacets();
    }
}

function displayFacets(aggregations) {
    if (!aggregations || currentIndex !== 'flights') {
        hideFacets();
        return;
    }
    
    let facetsHtml = '<div class="row"><div class="col-12"><h6 class="mb-3"><i class="bi bi-funnel-fill me-2"></i>Filter Results</h6></div></div>';
    facetsHtml += '<div class="row g-3">';
    
    // Cancelled facet
    if (aggregations.cancelled && aggregations.cancelled.buckets) {
        facetsHtml += '<div class="col-md-6 col-lg-3">';
        facetsHtml += '<div class="card h-100"><div class="card-body p-3">';
        facetsHtml += '<h6 class="card-title small mb-2"><i class="bi bi-x-circle me-1"></i>Cancelled</h6>';
        aggregations.cancelled.buckets.forEach(bucket => {
            const isCancelled = bucket.key === true || bucket.key === 'true';
            const isActive = currentFilters.cancelled === isCancelled;
            const activeClass = isActive ? 'btn-primary' : 'btn-outline-secondary';
            facetsHtml += `
                <button class="btn btn-sm ${activeClass} w-100 mb-1 facet-btn" 
                        data-facet="cancelled" 
                        data-value="${isCancelled}">
                    ${isCancelled ? 'Yes' : 'No'} <span class="badge bg-light text-dark">${bucket.doc_count}</span>
                </button>
            `;
        });
        facetsHtml += '</div></div></div>';
    }
    
    // Diverted facet
    if (aggregations.diverted && aggregations.diverted.buckets) {
        facetsHtml += '<div class="col-md-6 col-lg-3">';
        facetsHtml += '<div class="card h-100"><div class="card-body p-3">';
        facetsHtml += '<h6 class="card-title small mb-2"><i class="bi bi-arrow-repeat me-1"></i>Diverted</h6>';
        aggregations.diverted.buckets.forEach(bucket => {
            const isDiverted = bucket.key === true || bucket.key === 'true';
            const isActive = currentFilters.diverted === isDiverted;
            const activeClass = isActive ? 'btn-primary' : 'btn-outline-secondary';
            facetsHtml += `
                <button class="btn btn-sm ${activeClass} w-100 mb-1 facet-btn" 
                        data-facet="diverted" 
                        data-value="${isDiverted}">
                    ${isDiverted ? 'Yes' : 'No'} <span class="badge bg-light text-dark">${bucket.doc_count}</span>
                </button>
            `;
        });
        facetsHtml += '</div></div></div>';
    }
    
    // Airlines facet
    if (aggregations.airlines && aggregations.airlines.buckets && aggregations.airlines.buckets.length > 0) {
        facetsHtml += '<div class="col-md-6 col-lg-3">';
        facetsHtml += '<div class="card h-100"><div class="card-body p-3">';
        facetsHtml += '<h6 class="card-title small mb-2"><i class="bi bi-building me-1"></i>Airline</h6>';
        facetsHtml += '<div style="max-height: 200px; overflow-y: auto;">';
        aggregations.airlines.buckets.forEach(bucket => {
            const isActive = currentFilters.airline === bucket.key;
            const activeClass = isActive ? 'btn-primary' : 'btn-outline-secondary';
            facetsHtml += `
                <button class="btn btn-sm ${activeClass} w-100 mb-1 text-start facet-btn" 
                        data-facet="airline" 
                        data-value="${escapeHtml(bucket.key)}">
                    ${escapeHtml(bucket.key)} <span class="badge bg-light text-dark float-end">${bucket.doc_count}</span>
                </button>
            `;
        });
        facetsHtml += '</div></div></div></div>';
    }
    
    // Origins facet
    if (aggregations.origins && aggregations.origins.buckets && aggregations.origins.buckets.length > 0) {
        facetsHtml += '<div class="col-md-6 col-lg-3">';
        facetsHtml += '<div class="card h-100"><div class="card-body p-3">';
        facetsHtml += '<h6 class="card-title small mb-2"><i class="bi bi-geo-alt me-1"></i>Origin</h6>';
        facetsHtml += '<div style="max-height: 200px; overflow-y: auto;">';
        aggregations.origins.buckets.forEach(bucket => {
            const isActive = currentFilters.origin === bucket.key;
            const activeClass = isActive ? 'btn-primary' : 'btn-outline-secondary';
            facetsHtml += `
                <button class="btn btn-sm ${activeClass} w-100 mb-1 text-start facet-btn" 
                        data-facet="origin" 
                        data-value="${escapeHtml(bucket.key)}">
                    ${escapeHtml(bucket.key)} <span class="badge bg-light text-dark float-end">${bucket.doc_count}</span>
                </button>
            `;
        });
        facetsHtml += '</div></div></div></div>';
    }
    
    // Destinations facet
    if (aggregations.destinations && aggregations.destinations.buckets && aggregations.destinations.buckets.length > 0) {
        facetsHtml += '<div class="col-md-6 col-lg-3">';
        facetsHtml += '<div class="card h-100"><div class="card-body p-3">';
        facetsHtml += '<h6 class="card-title small mb-2"><i class="bi bi-geo-alt-fill me-1"></i>Destination</h6>';
        facetsHtml += '<div style="max-height: 200px; overflow-y: auto;">';
        aggregations.destinations.buckets.forEach(bucket => {
            const isActive = currentFilters.dest === bucket.key;
            const activeClass = isActive ? 'btn-primary' : 'btn-outline-secondary';
            facetsHtml += `
                <button class="btn btn-sm ${activeClass} w-100 mb-1 text-start facet-btn" 
                        data-facet="dest" 
                        data-value="${escapeHtml(bucket.key)}">
                    ${escapeHtml(bucket.key)} <span class="badge bg-light text-dark float-end">${bucket.doc_count}</span>
                </button>
            `;
        });
        facetsHtml += '</div></div></div></div>';
    }
    
    facetsHtml += '</div>';
    
    // Show active filters
    const activeFilters = Object.keys(currentFilters).filter(key => currentFilters[key] !== null && currentFilters[key] !== undefined);
    if (activeFilters.length > 0) {
        facetsHtml += '<div class="mt-3"><strong>Active Filters:</strong> ';
        const filterLabels = [];
        if (currentFilters.cancelled !== undefined) {
            filterLabels.push(`Cancelled: ${currentFilters.cancelled ? 'Yes' : 'No'}`);
        }
        if (currentFilters.diverted !== undefined) {
            filterLabels.push(`Diverted: ${currentFilters.diverted ? 'Yes' : 'No'}`);
        }
        if (currentFilters.airline) {
            filterLabels.push(`Airline: ${currentFilters.airline}`);
        }
        if (currentFilters.origin) {
            filterLabels.push(`Origin: ${currentFilters.origin}`);
        }
        if (currentFilters.dest) {
            filterLabels.push(`Destination: ${currentFilters.dest}`);
        }
        facetsHtml += filterLabels.join(' | ');
        facetsHtml += ' <button class="btn btn-sm btn-outline-danger ms-2" onclick="clearFilters()"><i class="bi bi-x-circle"></i> Clear All</button>';
        facetsHtml += '</div>';
    }
    
    facetsContainer.innerHTML = facetsHtml;
    facetsContainer.style.display = 'block';
    
    // Attach event listeners to facet buttons
    facetsContainer.querySelectorAll('.facet-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const facetName = this.dataset.facet;
            let value = this.dataset.value;
            
            // Convert string booleans to actual booleans
            if (value === 'true') value = true;
            if (value === 'false') value = false;
            
            toggleFacet(facetName, value);
        });
    });
}

function hideFacets() {
    facetsContainer.style.display = 'none';
    facetsContainer.innerHTML = '';
}

function toggleFacet(facetName, value) {
    if (currentFilters[facetName] === value) {
        // Remove filter if clicking the same value
        delete currentFilters[facetName];
    } else {
        // Set filter
        currentFilters[facetName] = value;
    }
    
    // Perform search with updated filters
    performSearch();
}

function clearFilters() {
    currentFilters = {};
    performSearch();
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
