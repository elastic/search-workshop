// State
let currentSearchMode = 'keyword';
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
const filterSidebar = document.getElementById('filterSidebar');
const filterToggleMobile = document.getElementById('filterToggleMobile');
const sidebarToggle = document.getElementById('sidebarToggle');
const themeToggle = document.getElementById('themeToggle');
const themeIcon = document.getElementById('themeIcon');
const logo = document.getElementById('logo');

const TEXT_EXTRACTION_KEYS = [
    'chunk',
    'delta',
    'text',
    'message',
    'content',
    'response',
    'output',
    'output_text',
    'result',
    'value',
    'body'
];

function extractTextFromPayload(payload) {
    const segments = [];
    const visited = new WeakSet();

    const collect = (value, depth = 0) => {
        if (value == null || depth > 6) return;

        if (typeof value === 'string') {
            if (value.trim()) {
                segments.push(value);
            }
            return;
        }

        if (typeof value === 'number' || typeof value === 'boolean') {
            segments.push(String(value));
            return;
        }

        if (Array.isArray(value)) {
            value.forEach(item => collect(item, depth + 1));
            return;
        }

        if (typeof value === 'object') {
            if (visited.has(value)) return;
            visited.add(value);

            if ('markdown' in value) {
                collect(value.markdown, depth + 1);
            }

            TEXT_EXTRACTION_KEYS.forEach(key => {
                if (key in value) {
                    collect(value[key], depth + 1);
                }
            });
        }
    };

    collect(payload, 0);

    return segments.join('').replace(/\r/g, '');
}

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

    // Sidebar toggle for mobile
    if (filterToggleMobile) {
        filterToggleMobile.addEventListener('click', openSidebar);
    }
    if (sidebarToggle) {
        sidebarToggle.addEventListener('click', closeSidebar);
    }

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

            // Update URL and re-search if appropriate
            updateURL();
            if (currentQuery || currentSearchMode === 'keyword' || currentSearchMode === 'semantic') {
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

            // Update URL
            updateURL();
            
            // Load documents - search if there's a query, otherwise load 10 documents
            performSearch();
        });
    });
});

async function performSearch() {
    const query = searchInput.value.trim();
    
    currentQuery = query;
    hideError();
    showLoading();
    clearResults();
    
    updateURL();
    
    if (currentSearchMode === 'ai') {
        await performStreamingSearch(query);
    } else {
        await performRegularSearch(query);
    }
}

async function performRegularSearch(query) {
    try {
        const requestBody = {
            query: query,
            type: currentSearchMode,
            index: currentIndex,
            filters: Object.keys(currentFilters).length > 0 ? currentFilters : {}
        };
        
        const response = await fetch('/api/search', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
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

async function performStreamingSearch(query) {
    try {
        const requestBody = {
            query: query,
            filters: Object.keys(currentFilters).length > 0 ? currentFilters : {}
        };

        const response = await fetch('/api/search/stream', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(requestBody)
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.error || `HTTP error! status: ${response.status}`);
        }

        await displayAIResults(response, query);
    } catch (error) {
        showError(error.message);
    } finally {
        hideLoading();
    }
}

async function displayAIResults(response, query) {
    resultsContainer.innerHTML = `
        <div class="ai-stream-wrapper" id="ai-stream-container">
            <div class="ai-message ai-message-user">
                <div class="ai-message-header">
                    <span class="ai-avatar">You</span>
                    <span class="ai-timestamp">${new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}</span>
                </div>
                <div class="ai-message-content">${escapeHtml(query || '(empty query)')}</div>
            </div>
            <div class="ai-message ai-message-assistant">
                <div class="ai-message-header">
                    <span class="ai-avatar">Flight AI</span>
                    <span class="ai-status-badge" id="ai-status-badge">Thinking…</span>
                </div>
                <div class="ai-message-content">
                    <div class="ai-assistant-text" id="ai-assistant-text"></div>
                    <div class="typing-indicator is-active" id="ai-typing-indicator">
                        <span></span><span></span><span></span>
                    </div>
                    <div class="ai-tool-events" id="ai-tool-events"></div>
                </div>
            </div>
        </div>
    `;

    const streamContainer = document.getElementById('ai-stream-container');
    const assistantTextEl = document.getElementById('ai-assistant-text');
    const typingIndicatorEl = document.getElementById('ai-typing-indicator');
    const toolEventsEl = document.getElementById('ai-tool-events');
    const statusBadgeEl = document.getElementById('ai-status-badge');
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    const context = {
        streamContainer,
        assistantTextEl,
        typingIndicatorEl,
        toolEventsEl,
        statusBadgeEl,
        assistantBuffer: '',
        lastStreamText: '',
        hasFinalMessage: false,
        lastToolEvent: null,
    };

    while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop();

        for (const line of lines) {
            const trimmed = line.trim();
            if (trimmed === '') continue;
            try {
                const jsonText = trimmed.startsWith('data:') ? trimmed.replace(/^data:\s*/, '') : trimmed;
                const data = JSON.parse(jsonText);
                renderAIStream(data, context);
            } catch (e) {
                console.error('Error parsing stream data:', e, 'line:', line);
                renderAIStream({ kind: 'unknown', raw: trimmed }, context);
            }
        }
    }

    context.typingIndicatorEl.classList.remove('is-active');
    context.statusBadgeEl.textContent = context.hasFinalMessage ? 'Done' : 'Completed';
    context.statusBadgeEl.classList.add('is-complete');
}

function renderAIStream(data, context) {
    console.debug('AI stream event:', data);
    const kind = data.kind || data.event || data.type || 'unknown';
    const {
        assistantTextEl,
        typingIndicatorEl,
        toolEventsEl,
        statusBadgeEl
    } = context;

    const appendToolEvent = (title, bodyText) => {
        const text = bodyText != null
            ? (typeof bodyText === 'string' ? bodyText : JSON.stringify(bodyText, null, 2))
            : '';

        const eventEl = document.createElement('div');
        eventEl.className = 'ai-tool-event';

        const titleEl = document.createElement('div');
        titleEl.className = 'ai-tool-event-title';
        titleEl.textContent = title;

        const preEl = document.createElement('pre');
        const codeEl = document.createElement('code');
        codeEl.textContent = text;
        preEl.appendChild(codeEl);

        eventEl.appendChild(titleEl);
        eventEl.appendChild(preEl);
        toolEventsEl.appendChild(eventEl);
        toolEventsEl.classList.add('is-visible');

        context.lastToolEvent = {
            container: eventEl,
            codeEl
        };

        return context.lastToolEvent;
    };

    const applyAssistantText = (incomingText, { replace = false } = {}) => {
        if (!incomingText) return;
        const text = incomingText.replace(/\r/g, '');
        if (!text.trim()) return;

        if (replace) {
            context.assistantBuffer = text;
            context.lastStreamText = text;
        } else {
            if (text === context.lastStreamText) {
                return;
            }

            if (context.assistantBuffer && text.startsWith(context.assistantBuffer)) {
                const remainder = text.slice(context.assistantBuffer.length);
                if (remainder) {
                    context.assistantBuffer += remainder;
                }
            } else if (context.lastStreamText && text.startsWith(context.lastStreamText)) {
                const remainder = text.slice(context.lastStreamText.length);
                if (remainder) {
                    context.assistantBuffer += remainder;
                }
            } else if (!context.assistantBuffer) {
                context.assistantBuffer = text;
            } else {
                const needsSpace = !context.assistantBuffer.endsWith(' ') && !text.startsWith(' ');
                context.assistantBuffer += needsSpace ? ` ${text}` : text;
            }

            context.lastStreamText = text;
        }

        assistantTextEl.textContent = context.assistantBuffer;
        assistantTextEl.classList.add('is-visible');
        typingIndicatorEl.classList.add('is-active');
        if (!statusBadgeEl.classList.contains('is-complete')) {
            statusBadgeEl.textContent = 'Responding…';
            statusBadgeEl.classList.remove('is-error');
        }
    };

    const chunkPayload = data.chunk ?? data.delta ?? data.message ?? data.response ?? data.output ?? null;
    const chunkText = chunkPayload ? extractTextFromPayload(chunkPayload) : '';
    const hasChunkText = Boolean(chunkText && chunkText.trim());

    if (['tool_code', 'tool_start'].includes(kind)) {
        appendToolEvent(data.tool_name ? `Running ${data.tool_name}` : 'Tool Code', data.code || chunkText);
        statusBadgeEl.textContent = 'Calling tool…';
        statusBadgeEl.classList.remove('is-complete', 'is-error');
    } else if (['tool_output', 'tool_result', 'tool_end'].includes(kind)) {
        const output = data.output || data.result || chunkText;
        appendToolEvent(data.tool_name ? `${data.tool_name} Output` : 'Tool Output', output);
        statusBadgeEl.textContent = 'Tool complete';
        statusBadgeEl.classList.remove('is-error');
    } else if (kind === 'tool_progress') {
        const progressMessages = [];

        if (typeof data.message === 'string') {
            progressMessages.push(data.message);
        }

        if (Array.isArray(data.data)) {
            data.data.forEach(item => {
                if (item && typeof item.message === 'string') {
                    progressMessages.push(item.message);
                }
            });
        } else if (data.data && typeof data.data === 'object' && typeof data.data.message === 'string') {
            progressMessages.push(data.data.message);
        }

        if (hasChunkText) {
            progressMessages.push(chunkText);
        }

        const progressText = progressMessages
            .map(msg => msg.trim())
            .filter(Boolean)
            .join('\n');

        if (progressText) {
            if (context.lastToolEvent && context.lastToolEvent.codeEl) {
                const codeEl = context.lastToolEvent.codeEl;
                const existing = codeEl.textContent || '';
                codeEl.textContent = existing ? `${existing}\n${progressText}` : progressText;
                context.lastToolEvent.container.classList.add('has-progress');
                toolEventsEl.classList.add('is-visible');
            } else {
                appendToolEvent('Tool Progress', progressText);
            }

            statusBadgeEl.textContent = 'In progress…';
            statusBadgeEl.classList.remove('is-error');
        }
    } else if (['tool_call', 'agent_action'].includes(kind)) {
        const payload = data.payload || data.parameters || data.args || data;
        const displayPayload = typeof payload === 'string' ? payload : JSON.stringify(payload, null, 2);
        const title = data.tool_name || data.name || 'Tool Call';
        appendToolEvent(title, displayPayload);
        statusBadgeEl.textContent = 'Calling tool…';
        statusBadgeEl.classList.remove('is-error');
    } else if (['status'].includes(kind) && hasChunkText) {
        statusBadgeEl.textContent = chunkText;
        statusBadgeEl.classList.remove('is-error');
    } else if (['llm_response_chunk', 'response_chunk', 'message', 'assistant_message', 'text_delta', 'content_delta'].includes(kind)) {
        if (hasChunkText) {
            applyAssistantText(chunkText);
        }
    } else if (['final_response', 'response', 'assistant_response', 'message_completed', 'completion'].includes(kind)) {
        const responseText = chunkText || extractTextFromPayload(data);
        if (responseText) {
            applyAssistantText(responseText, { replace: true });
        }
        typingIndicatorEl.classList.remove('is-active');
        statusBadgeEl.textContent = 'Responded';
        statusBadgeEl.classList.add('is-complete');
        context.hasFinalMessage = true;
    } else if (kind === 'error') {
        appendToolEvent('Error', data.message || chunkText || 'Unknown error');
        typingIndicatorEl.classList.remove('is-active');
        statusBadgeEl.textContent = 'Error';
        statusBadgeEl.classList.add('is-error');
        context.hasFinalMessage = true;
    } else if (kind === 'done') {
        typingIndicatorEl.classList.remove('is-active');
        statusBadgeEl.textContent = 'Done';
        statusBadgeEl.classList.add('is-complete');
        context.hasFinalMessage = true;
    } else if (hasChunkText) {
        applyAssistantText(chunkText);
    } else if (kind === 'unknown' && data.raw) {
        const rawText = typeof data.raw === 'string' ? data.raw : JSON.stringify(data.raw, null, 2);
        appendToolEvent('Event', rawText);
    } else {
        appendToolEvent(kind || 'Event', JSON.stringify(data, null, 2));
    }

    setTimeout(() => {
        context.streamContainer.scrollTop = context.streamContainer.scrollHeight;
    }, 0);
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

            // Create airline display for title (will be enhanced in rendering)
            title = `${airline}${flightNum}`;
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
            if (source.DistanceMiles) parts.push(`<i class="bi bi-signpost-2 me-1"></i>Distance: ${source.DistanceMiles} miles`);
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
            // Check for highlight on the .text subfield
            let airlineName = highlight['Airline_Name.text']?.[0] || highlight['Airline_Name']?.[0] || '';
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
                <div class="result-item result-item-airlines">
                    <div class="d-flex justify-content-between align-items-start">
                        <h6 class="fw-bold">
                            <i class="bi bi-building me-2"></i>${title}
                        </h6>
                        ${indexBadge}
                    </div>
                    <p class="mb-0 small text-muted">
                        <i class="bi bi-tag-fill me-1"></i>Airline Code: ${url}
                    </p>
                </div>
            `;
        } else if (indexName === 'flights') {
            // Special formatting for flights - show route prominently with structured layout
            const source = hit._source;
            // Use Airline_Name if available (from ES|QL LOOKUP JOIN), otherwise fall back to Reporting_Airline code
            const airlineDisplay = source.Airline_Name
                ? `${source.Airline_Name} (${source.Reporting_Airline})`
                : source.Reporting_Airline;

            // Format times (e.g., "1430" -> "14:30" or "2:30 PM")
            const formatTime = (timeStr) => {
                if (!timeStr) return '';
                const str = String(timeStr).padStart(4, '0');
                const hours = parseInt(str.substring(0, 2));
                const minutes = str.substring(2, 4);
                const period = hours >= 12 ? 'PM' : 'AM';
                const displayHours = hours === 0 ? 12 : (hours > 12 ? hours - 12 : hours);
                return `${displayHours}:${minutes} ${period}`;
            };

            // Calculate actual arrival time by adding delay to scheduled time
            const calculateActualArrival = (scheduledTime, delayMinutes) => {
                if (!scheduledTime || delayMinutes === undefined) return null;
                const str = String(scheduledTime).padStart(4, '0');
                let hours = parseInt(str.substring(0, 2));
                let minutes = parseInt(str.substring(2, 4));

                // Add delay
                minutes += delayMinutes;

                // Handle overflow
                while (minutes >= 60) {
                    hours++;
                    minutes -= 60;
                }
                while (minutes < 0) {
                    hours--;
                    minutes += 60;
                }
                while (hours >= 24) {
                    hours -= 24;
                }
                while (hours < 0) {
                    hours += 24;
                }

                const period = hours >= 12 ? 'PM' : 'AM';
                const displayHours = hours === 0 ? 12 : (hours > 12 ? hours - 12 : hours);
                const displayMinutes = String(minutes).padStart(2, '0');
                return `${displayHours}:${displayMinutes} ${period}`;
            };

            // Format delay in hours and minutes
            const formatDelay = (delayMinutes) => {
                const absDelay = Math.abs(delayMinutes);
                const hours = Math.floor(absDelay / 60);
                const minutes = absDelay % 60;
                const sign = delayMinutes < 0 ? '-' : '';

                if (hours > 0) {
                    return `${sign}${hours}h ${minutes}m`;
                } else {
                    return `${sign}${minutes}m`;
                }
            };

            // Format flight date with day of week in UTC
            const formatFlightDate = (timestamp) => {
                if (!timestamp) return '';
                const date = new Date(timestamp);
                const days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
                const dayName = days[date.getUTCDay()];
                const dateStr = date.toLocaleDateString('en-US', {
                    month: 'short',
                    day: 'numeric',
                    year: 'numeric',
                    timeZone: 'UTC'
                });
                return `${dayName}, ${dateStr}`;
            };

            html += `
                <div class="result-item result-item-flights">
                    <div class="d-flex justify-content-between align-items-start mb-2">
                        <div class="flex-grow-1">
                            <h6 class="fw-bold mb-1">
                                <i class="bi bi-airplane me-2"></i>${airlineDisplay} Flight #${source.Flight_Number || 'N/A'}
                            </h6>
                            ${source['@timestamp'] ? `<div class="small text-muted">
                                <i class="bi bi-calendar-event me-1"></i>${formatFlightDate(source['@timestamp'])}
                            </div>` : ''}
                        </div>
                        ${indexBadge}
                    </div>

                    <div class="flight-route-display p-2" style="background-color: var(--color-bg-secondary); border-radius: 6px;">
                        <div class="d-flex align-items-center justify-content-between">
                            <div class="text-center flex-shrink-0" style="min-width: 80px;">
                                <div class="fw-bold" style="font-size: 1.1em;">${source.Origin || ''}</div>
                                ${source.CRSDepTimeLocal ? `<div class="fw-bold" style="font-size: 0.95em;">${formatTime(source.CRSDepTimeLocal)}</div>` : ''}
                                ${source.CRSDepTimeLocal && source.DepDelayMin !== undefined ? `<div class="small"><span class="text-muted">Actual:</span> <span class="fw-bold">${calculateActualArrival(source.CRSDepTimeLocal, source.DepDelayMin)}</span></div>` : ''}
                                ${source.DepDelayMin !== undefined && source.DepDelayMin !== 0 ? `<div class="small ${source.DepDelayMin > 0 ? 'text-danger' : 'text-success'}">${formatDelay(source.DepDelayMin)} <span style="color: #999;">(${source.DepDelayMin > 0 ? 'Late' : 'Early'})</span></div>` : ''}
                            </div>
                            <div class="flex-grow-1 text-center mx-3">
                                <div class="d-flex align-items-center justify-content-center">
                                    <div style="flex: 1; height: 2px; background: var(--color-border);"></div>
                                    <i class="bi bi-airplane-fill mx-2" style="transform: rotate(90deg);"></i>
                                    <div style="flex: 1; height: 2px; background: var(--color-border);"></div>
                                </div>
                                ${source.DistanceMiles ? `<div class="small text-muted mt-1">${source.DistanceMiles.toLocaleString()} mi</div>` : ''}
                            </div>
                            <div class="text-center flex-shrink-0" style="min-width: 80px;">
                                <div class="fw-bold" style="font-size: 1.1em;">${source.Dest || ''}</div>
                                ${source.CRSArrTimeLocal ? `<div class="fw-bold" style="font-size: 0.95em;">${formatTime(source.CRSArrTimeLocal)}</div>` : ''}
                                ${source.CRSArrTimeLocal && source.ArrDelayMin !== undefined ? `<div class="small"><span class="text-muted">Actual:</span> <span class="fw-bold">${calculateActualArrival(source.CRSArrTimeLocal, source.ArrDelayMin)}</span></div>` : ''}
                                ${source.ArrDelayMin !== undefined && source.ArrDelayMin !== 0 ? `<div class="small ${source.ArrDelayMin > 0 ? 'text-danger' : 'text-success'}">${formatDelay(source.ArrDelayMin)} <span style="color: #999;">(${source.ArrDelayMin > 0 ? 'Late' : 'Early'})</span></div>` : ''}
                            </div>
                        </div>
                    </div>

                    ${source.Cancelled || source.Diverted ? `<div class="d-flex flex-wrap gap-2 small mt-2">
                        ${source.Cancelled ? `<div class="flex-fill">
                            <i class="bi bi-x-circle-fill me-1 text-danger"></i><strong class="text-danger">Cancelled</strong>
                        </div>` : ''}
                        ${source.Diverted ? `<div class="flex-fill">
                            <i class="bi bi-arrow-repeat me-1 text-warning"></i><strong class="text-warning">Diverted</strong>
                        </div>` : ''}
                    </div>` : ''}
                    ${source.FlightID ? `<div class="mt-2 pt-2" style="border-top: 1px solid var(--color-border);">
                        <div class="text-start" style="font-size: 0.75em; color: #aaa;">
                            ${source.FlightID}
                        </div>
                    </div>` : ''}
                </div>
            `;
        } else {
            html += `
                <div class="result-item result-item-contracts">
                    <div class="d-flex justify-content-between align-items-start">
                        <h6 class="fw-bold">
                            <i class="bi bi-file-earmark-text me-2"></i>${escapeHtml(title)}
                        </h6>
                        ${indexBadge}
                    </div>
                    ${snippetHtml ? `<p class="small">${snippetHtml}</p>` : ''}
                    ${meta.length > 0 ? `<small class="text-muted d-block">
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
    const resultText = totalValue === 1 ? 'Result' : 'Results';
    html += `
        <div class="text-center mt-4 pt-3 border-top">
            <p class="text-muted mb-0 small">
                <i class="bi bi-bar-chart"></i> ${totalValue.toLocaleString()} ${resultText}${indicesInfo}
            </p>
        </div>
    `;
    
    resultsContainer.innerHTML = html;

    // Always display sidebar with index selector and facets
    displayFacets(data.aggregations);
}

function displayFacets(aggregations) {
    // Always show sidebar with at least index selector
    let facetsHtml = '';

    // Show active filters at the top
    const activeFilters = Object.keys(currentFilters).filter(key => currentFilters[key] !== null && currentFilters[key] !== undefined);
    if (activeFilters.length > 0) {
        facetsHtml += '<div class="active-filters-section">';
        facetsHtml += '<strong>Active Filters</strong>';
        facetsHtml += '<div class="mt-2">';

        // Flights filters
        if (currentFilters.cancelled !== undefined) {
            facetsHtml += `<div class="mb-1"><small>Cancelled: ${currentFilters.cancelled ? 'Yes' : 'No'}</small></div>`;
        }
        if (currentFilters.diverted !== undefined) {
            facetsHtml += `<div class="mb-1"><small>Diverted: ${currentFilters.diverted ? 'Yes' : 'No'}</small></div>`;
        }
        if (currentFilters.airline) {
            facetsHtml += `<div class="mb-1"><small>Airline: ${currentFilters.airline}</small></div>`;
        }
        if (currentFilters.origin) {
            facetsHtml += `<div class="mb-1"><small>Origin: ${currentFilters.origin}</small></div>`;
        }
        if (currentFilters.dest) {
            facetsHtml += `<div class="mb-1"><small>Destination: ${currentFilters.dest}</small></div>`;
        }
        if (currentFilters.flight_date) {
            facetsHtml += `<div class="mb-1"><small>Date: ${currentFilters.flight_date}</small></div>`;
        }

        // Airlines filters
        if (currentFilters.airline_code) {
            facetsHtml += `<div class="mb-1"><small>Airline Code: ${currentFilters.airline_code}</small></div>`;
        }

        // Contracts filters
        if (currentFilters.author) {
            facetsHtml += `<div class="mb-1"><small>Author: ${currentFilters.author}</small></div>`;
        }
        if (currentFilters.upload_year) {
            facetsHtml += `<div class="mb-1"><small>Upload Year: ${currentFilters.upload_year}</small></div>`;
        }

        facetsHtml += '</div>';
        facetsHtml += '<button class="btn btn-sm btn-outline-danger w-100 mt-2" onclick="clearFilters()"><i class="bi bi-x-circle me-1"></i> Clear All</button>';
        facetsHtml += '</div>';
    }

    // Index selector (always visible)
    facetsHtml += '<div class="filter-section">';
    facetsHtml += '<div class="filter-title"><i class="bi bi-collection me-2"></i>Index</div>';

    // Define all indices
    const indices = [
        { key: 'all', label: 'All Indices' },
        { key: 'flights', label: 'Flights' },
        { key: 'airlines', label: 'Airlines' },
        { key: 'contracts', label: 'Contracts' }
    ];

    indices.forEach(index => {
        const isActive = currentIndex === index.key;
        const activeClass = isActive ? 'btn-primary' : 'btn-outline-secondary';

        // Show counts if we're on "all" and have aggregations
        let countDisplay = '';
        if (currentIndex === 'all' && aggregations.record_types && aggregations.record_types.buckets) {
            const bucket = aggregations.record_types.buckets.find(b => b.key === index.key);
            if (bucket && bucket.doc_count > 0) {
                countDisplay = ` <span style="font-size: 0.85em; font-weight: normal;">(${bucket.doc_count.toLocaleString()})</span>`;
            }
        }

        facetsHtml += `
            <button class="btn btn-sm ${activeClass} facet-btn index-switch-btn"
                    data-index="${escapeHtml(index.key)}">
                ${index.label}${countDisplay}
            </button>
        `;
    });

    facetsHtml += '</div>';

    // Flights-specific facets
    if (currentIndex === 'flights') {
        // Cancelled facet
        if (aggregations.cancelled && aggregations.cancelled.buckets) {
        facetsHtml += '<div class="filter-section">';
        facetsHtml += '<div class="filter-title"><i class="bi bi-slash-circle me-2"></i>Cancelled</div>';
        aggregations.cancelled.buckets.forEach(bucket => {
            // Handle various boolean representations
            const isCancelled = bucket.key === true || bucket.key === 'true' || bucket.key === 1 || bucket.key === '1';
            const isActive = currentFilters.cancelled === isCancelled;
            const activeClass = isActive ? 'btn-primary' : 'btn-outline-secondary';
            const label = isCancelled ? 'Yes' : 'No';
            facetsHtml += `
                <button class="btn btn-sm ${activeClass} facet-btn"
                        data-facet="cancelled"
                        data-value="${isCancelled}">
                    ${label} <span style="font-size: 0.85em; font-weight: normal;">(${bucket.doc_count.toLocaleString()})</span>
                </button>
            `;
        });
        facetsHtml += '</div>';
    }

    // Diverted facet
    if (aggregations.diverted && aggregations.diverted.buckets) {
        facetsHtml += '<div class="filter-section">';
        facetsHtml += '<div class="filter-title"><i class="bi bi-arrow-repeat me-2"></i>Diverted</div>';
        aggregations.diverted.buckets.forEach(bucket => {
            // Handle various boolean representations
            const isDiverted = bucket.key === true || bucket.key === 'true' || bucket.key === 1 || bucket.key === '1';
            const isActive = currentFilters.diverted === isDiverted;
            const activeClass = isActive ? 'btn-primary' : 'btn-outline-secondary';
            const label = isDiverted ? 'Yes' : 'No';
            facetsHtml += `
                <button class="btn btn-sm ${activeClass} facet-btn"
                        data-facet="diverted"
                        data-value="${isDiverted}">
                    ${label} <span style="font-size: 0.85em; font-weight: normal;">(${bucket.doc_count.toLocaleString()})</span>
                </button>
            `;
        });
        facetsHtml += '</div>';
    }

    // Airlines facet
    if (aggregations.airlines && aggregations.airlines.buckets && aggregations.airlines.buckets.length > 0) {
        facetsHtml += '<div class="filter-section">';
        facetsHtml += '<div class="filter-title"><i class="bi bi-building me-2"></i>Airline</div>';
        facetsHtml += '<div class="scrollable-filters">';
        aggregations.airlines.buckets.forEach(bucket => {
            const isActive = currentFilters.airline === bucket.key;
            const activeClass = isActive ? 'btn-primary' : 'btn-outline-secondary';
            facetsHtml += `
                <button class="btn btn-sm ${activeClass} facet-btn"
                        data-facet="airline"
                        data-value="${escapeHtml(bucket.key)}">
                    ${escapeHtml(bucket.key)} <span style="font-size: 0.85em; font-weight: normal;">(${bucket.doc_count.toLocaleString()})</span>
                </button>
            `;
        });
        facetsHtml += '</div></div>';
    }

    // Origins facet
    if (aggregations.origins && aggregations.origins.buckets && aggregations.origins.buckets.length > 0) {
        facetsHtml += '<div class="filter-section">';
        facetsHtml += '<div class="filter-title"><i class="bi bi-geo-alt me-2"></i>Origin</div>';
        facetsHtml += '<div class="scrollable-filters">';
        aggregations.origins.buckets.forEach(bucket => {
            const isActive = currentFilters.origin === bucket.key;
            const activeClass = isActive ? 'btn-primary' : 'btn-outline-secondary';
            facetsHtml += `
                <button class="btn btn-sm ${activeClass} facet-btn"
                        data-facet="origin"
                        data-value="${escapeHtml(bucket.key)}">
                    ${escapeHtml(bucket.key)} <span style="font-size: 0.85em; font-weight: normal;">(${bucket.doc_count.toLocaleString()})</span>
                </button>
            `;
        });
        facetsHtml += '</div></div>';
    }

    // Destinations facet
    if (aggregations.destinations && aggregations.destinations.buckets && aggregations.destinations.buckets.length > 0) {
        facetsHtml += '<div class="filter-section">';
        facetsHtml += '<div class="filter-title"><i class="bi bi-geo-alt-fill me-2"></i>Destination</div>';
        facetsHtml += '<div class="scrollable-filters">';
        aggregations.destinations.buckets.forEach(bucket => {
            const isActive = currentFilters.dest === bucket.key;
            const activeClass = isActive ? 'btn-primary' : 'btn-outline-secondary';
            facetsHtml += `
                <button class="btn btn-sm ${activeClass} facet-btn"
                        data-facet="dest"
                        data-value="${escapeHtml(bucket.key)}">
                    ${escapeHtml(bucket.key)} <span style="font-size: 0.85em; font-weight: normal;">(${bucket.doc_count.toLocaleString()})</span>
                </button>
            `;
        });
        facetsHtml += '</div></div>';
        }

    // Flight dates facet
    if (aggregations.flight_dates && aggregations.flight_dates.buckets && aggregations.flight_dates.buckets.length > 0) {
        facetsHtml += '<div class="filter-section">';
        facetsHtml += '<div class="filter-title"><i class="bi bi-calendar-event me-2"></i>Flight Date</div>';
        facetsHtml += '<div class="scrollable-filters">';
        aggregations.flight_dates.buckets.forEach(bucket => {
            if (bucket.doc_count > 0) {
                const isActive = currentFilters.flight_date === bucket.key_as_string;
                const activeClass = isActive ? 'btn-primary' : 'btn-outline-secondary';
                facetsHtml += `
                    <button class="btn btn-sm ${activeClass} facet-btn"
                            data-facet="flight_date"
                            data-value="${escapeHtml(bucket.key_as_string)}">
                        ${escapeHtml(bucket.key_as_string)} <span style="font-size: 0.85em; font-weight: normal;">(${bucket.doc_count.toLocaleString()})</span>
                    </button>
                `;
            }
        });
        facetsHtml += '</div></div>';
        }
    }

    // Airlines-specific facets
    if (currentIndex === 'airlines') {
        // Airline codes facet
        if (aggregations.airline_codes && aggregations.airline_codes.buckets && aggregations.airline_codes.buckets.length > 0) {
            facetsHtml += '<div class="filter-section">';
            facetsHtml += '<div class="filter-title"><i class="bi bi-tag me-2"></i>Airline Code</div>';
            facetsHtml += '<div class="scrollable-filters">';
            aggregations.airline_codes.buckets.forEach(bucket => {
                const isActive = currentFilters.airline_code === bucket.key;
                const activeClass = isActive ? 'btn-primary' : 'btn-outline-secondary';
                facetsHtml += `
                    <button class="btn btn-sm ${activeClass} facet-btn"
                            data-facet="airline_code"
                            data-value="${escapeHtml(bucket.key)}">
                        ${escapeHtml(bucket.key)} <span style="font-size: 0.85em; font-weight: normal;">(${bucket.doc_count.toLocaleString()})</span>
                    </button>
                `;
            });
            facetsHtml += '</div></div>';
        }
    }

    // Contracts-specific facets
    if (currentIndex === 'contracts') {
        // Authors facet
        if (aggregations.authors && aggregations.authors.buckets && aggregations.authors.buckets.length > 0) {
            facetsHtml += '<div class="filter-section">';
            facetsHtml += '<div class="filter-title"><i class="bi bi-person me-2"></i>Author</div>';
            facetsHtml += '<div class="scrollable-filters">';
            aggregations.authors.buckets.forEach(bucket => {
                const isActive = currentFilters.author === bucket.key;
                const activeClass = isActive ? 'btn-primary' : 'btn-outline-secondary';
                facetsHtml += `
                    <button class="btn btn-sm ${activeClass} facet-btn"
                            data-facet="author"
                            data-value="${escapeHtml(bucket.key)}">
                        ${escapeHtml(bucket.key)} <span style="font-size: 0.85em; font-weight: normal;">(${bucket.doc_count.toLocaleString()})</span>
                    </button>
                `;
            });
            facetsHtml += '</div></div>';
        }

        // Upload years facet
        if (aggregations.upload_years && aggregations.upload_years.buckets && aggregations.upload_years.buckets.length > 0) {
            facetsHtml += '<div class="filter-section">';
            facetsHtml += '<div class="filter-title"><i class="bi bi-calendar3 me-2"></i>Upload Year</div>';
            aggregations.upload_years.buckets.forEach(bucket => {
                const isActive = currentFilters.upload_year === bucket.key_as_string;
                const activeClass = isActive ? 'btn-primary' : 'btn-outline-secondary';
                facetsHtml += `
                    <button class="btn btn-sm ${activeClass} facet-btn"
                            data-facet="upload_year"
                            data-value="${escapeHtml(bucket.key_as_string)}">
                        ${escapeHtml(bucket.key_as_string)} <span style="font-size: 0.85em; font-weight: normal;">(${bucket.doc_count.toLocaleString()})</span>
                    </button>
                `;
            });
            facetsHtml += '</div>';
        }
    }

    facetsContainer.innerHTML = facetsHtml;

    // Show sidebar on first render only (avoid flickering)
    if (filterSidebar.style.display !== 'block') {
        filterSidebar.style.display = 'block';
        filterToggleMobile.style.display = 'block';
    }

    // Attach event listeners to facet buttons
    facetsContainer.querySelectorAll('.facet-btn:not(.index-switch-btn)').forEach(btn => {
        btn.addEventListener('click', function() {
            const facetName = this.dataset.facet;
            let value = this.dataset.value;

            // Convert string booleans to actual booleans
            if (value === 'true') value = true;
            if (value === 'false') value = false;

            toggleFacet(facetName, value);
        });
    });

    // Attach event listeners to index switch buttons
    facetsContainer.querySelectorAll('.index-switch-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const newIndex = this.dataset.index;
            switchToIndex(newIndex);
        });
    });
}

function hideFacets() {
    filterSidebar.style.display = 'none';
    filterToggleMobile.style.display = 'none';
    facetsContainer.innerHTML = '';
    closeSidebar();
}

function openSidebar() {
    filterSidebar.classList.add('show');
    // Create overlay for mobile
    let overlay = document.querySelector('.sidebar-overlay');
    if (!overlay) {
        overlay = document.createElement('div');
        overlay.className = 'sidebar-overlay';
        overlay.addEventListener('click', closeSidebar);
        document.body.appendChild(overlay);
    }
    setTimeout(() => overlay.classList.add('show'), 10);
}

function closeSidebar() {
    filterSidebar.classList.remove('show');
    const overlay = document.querySelector('.sidebar-overlay');
    if (overlay) {
        overlay.classList.remove('show');
        setTimeout(() => overlay.remove(), 300);
    }
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

function switchToIndex(newIndex) {
    // Clear filters when switching indices
    currentFilters = {};

    // Update the active index button
    indexButtons.forEach(btn => {
        btn.classList.remove('active');
        btn.classList.remove('btn-primary');
        btn.classList.add('btn-outline-primary');
        if (btn.dataset.index === newIndex) {
            btn.classList.add('active');
            btn.classList.remove('btn-outline-primary');
            btn.classList.add('btn-primary');
        }
    });

    currentIndex = newIndex;

    // Update URL
    updateURL();

    // Perform search
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
    if (!loadingIndicator) return;
    loadingIndicator.style.display = 'flex';
    loadingIndicator.setAttribute('aria-hidden', 'false');
}

function hideLoading() {
    if (!loadingIndicator) return;
    loadingIndicator.style.display = 'none';
    loadingIndicator.setAttribute('aria-hidden', 'true');
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

    // Clear all filters
    currentFilters = {};

    // Reset to default search mode (Keyword)
    currentSearchMode = 'keyword';
    modeButtons.forEach(btn => {
        if (btn.dataset.mode === 'keyword') {
            btn.classList.add('active');
            btn.classList.remove('btn-outline-primary');
            btn.classList.add('btn-primary');
        } else {
            btn.classList.remove('active');
            btn.classList.remove('btn-primary');
            btn.classList.add('btn-outline-primary');
        }
    });

    // Reset to default index (all)
    currentIndex = 'all';
    indexButtons.forEach(btn => {
        if (btn.dataset.index === 'all') {
            btn.classList.add('active');
            btn.classList.remove('btn-outline-primary');
            btn.classList.add('btn-primary');
        } else {
            btn.classList.remove('active');
            btn.classList.remove('btn-primary');
            btn.classList.add('btn-outline-primary');
        }
    });

    // Clear results
    clearResults();

    // Hide loading and error messages
    hideLoading();
    hideError();

    // Hide sidebar
    hideFacets();

    // Update URL (clears all parameters)
    updateURL();

    // Focus back on search input
    searchInput.focus();

    // Reload default results so the page isn't empty
    performSearch();
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

    // Add filters to URL
    Object.keys(currentFilters).forEach(key => {
        const value = currentFilters[key];
        if (value !== null && value !== undefined) {
            if (typeof value === 'boolean') {
                params.set(key, value.toString());
            } else if (Array.isArray(value)) {
                params.set(key, value.join(','));
            } else {
                params.set(key, value.toString());
            }
        }
    });

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
    if (type && ['keyword', 'semantic', 'ai'].includes(type)) {
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

    // Restore filters from URL
    currentFilters = {};
    const filterKeys = ['cancelled', 'diverted', 'airline', 'origin', 'dest', 'flight_date', 'airline_code', 'author', 'upload_year'];
    filterKeys.forEach(key => {
        const value = params.get(key);
        if (value !== null) {
            // Convert boolean strings to actual booleans
            if (value === 'true') {
                currentFilters[key] = true;
            } else if (value === 'false') {
                currentFilters[key] = false;
            } else {
                currentFilters[key] = value;
            }
        }
    });

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
