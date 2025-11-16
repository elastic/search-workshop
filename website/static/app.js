// State
let currentSearchMode = 'keyword';
let currentQuery = '';
let currentIndex = 'all';
let searchTimeout = null;
let currentFilters = {}; // Active filters for flights
let currentConversationId = null; // Current conversation ID for AI mode

// DOM Elements
const searchInput = document.getElementById('searchInput');
const searchButton = document.getElementById('searchButton');
const modeButtons = document.querySelectorAll('.mode-btn');
const indexButtons = document.querySelectorAll('.index-btn');
const loadingIndicator = document.getElementById('loadingIndicator');
const errorMessage = document.getElementById('errorMessage');
const resultsContainer = document.getElementById('resultsContainer');
const facetsContainer = document.getElementById('facetsContainer');
let conversationSidebar = document.getElementById('conversationSidebar');
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

function escapeForRegex(value) {
    return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function applyQueryHighlight(text, query) {
    if (!text || !query) {
        return text;
    }

    // Skip if highlight markup already present
    if (/<(?:em|mark)\b/i.test(text)) {
        return text;
    }

    const terms = query.trim().split(/\s+/).filter(Boolean);
    if (terms.length === 0) {
        return text;
    }

    const pattern = new RegExp(`\\b(${terms.map(escapeForRegex).join('|')})\\b`, 'gi');
    return text.replace(pattern, '<mark class="result-highlight">$1</mark>');
}

function getQueryTokens(query) {
    if (!query) {
        return [];
    }
    return query.trim().split(/\s+/).filter(Boolean).map(token => token.toLowerCase());
}

function collectSemanticTermsFromEmbeddings(terms, embeddings, threshold) {
    if (!embeddings) return;
    Object.entries(embeddings).forEach(([token, value]) => {
        if (typeof value === 'number' && value > threshold) {
            terms.add(token);
        }
    });
}

function collectSemanticTermsFromChunks(terms, chunks, threshold) {
    if (!chunks) return;

    const stack = Array.isArray(chunks) ? [...chunks] : [chunks];
    const visited = new Set();

    while (stack.length > 0) {
        const current = stack.pop();
        if (!current || typeof current !== 'object') {
            continue;
        }
        if (visited.has(current)) {
            continue;
        }
        visited.add(current);

        collectSemanticTermsFromEmbeddings(terms, current.embeddings, threshold);

        const nextValues = [];
        if (current.inference && typeof current.inference === 'object') {
            nextValues.push(current.inference);
        }
        if (current.chunks) {
            nextValues.push(current.chunks);
        }
        Object.values(current).forEach(value => {
            if (!value || typeof value !== 'object') {
                return;
            }
            if (value === current.embeddings || value === current.inference || value === current.chunks) {
                return;
            }
            nextValues.push(value);
        });

        nextValues.forEach(value => {
            if (!value) return;
            if (Array.isArray(value)) {
                value.forEach(item => stack.push(item));
            } else if (typeof value === 'object') {
                stack.push(value);
            }
        });
    }
}

function collectSemanticTermsFromField(terms, fieldValue, threshold) {
    if (!fieldValue) return;

    const values = Array.isArray(fieldValue) ? fieldValue : [fieldValue];
    values.forEach(value => {
        if (!value || typeof value !== 'object') return;
        const inference = value.inference || value;
        if (!inference || typeof inference !== 'object') return;
        collectSemanticTermsFromChunks(terms, inference.chunks, threshold);
    });
}

function collectSemanticTermsFromContainer(terms, container, fieldName, threshold) {
    if (!container) return;

    if (typeof container === 'string') {
        try {
            const parsed = JSON.parse(container);
            collectSemanticTermsFromContainer(terms, parsed, fieldName, threshold);
        } catch (err) {
            return;
        }
        return;
    }

    if (Array.isArray(container)) {
        container.forEach(item => collectSemanticTermsFromContainer(terms, item, fieldName, threshold));
        return;
    }

    if (typeof container !== 'object') return;

    const fieldValue = container[fieldName];
    if (fieldValue) {
        collectSemanticTermsFromField(terms, fieldValue, threshold);
    }
}

function getSemanticHighlightTerms(hit, fieldName = 'Airline_Name.semantic', threshold = 1.2) {
    const terms = new Set();
    if (!hit) return terms;

    collectSemanticTermsFromContainer(terms, hit._source?._inference_fields, fieldName, threshold);
    collectSemanticTermsFromContainer(terms, hit.fields?._inference_fields, fieldName, threshold);

    return terms;
}

function applySemanticHighlight(text, terms, queryTokens = []) {
    if (!text || !terms || terms.size === 0) {
        return text;
    }

    const queryTokenSet = new Set(queryTokens.map(token => token.toLowerCase()));
    const tokens = Array.from(terms)
        .filter(token => !queryTokenSet.has(String(token).toLowerCase()))
        .sort((a, b) => b.length - a.length);
    if (tokens.length === 0) {
        return text;
    }

    const pattern = new RegExp(`\\b(${tokens.map(escapeForRegex).join('|')})\\b`, 'gi');

    return text.split(/(<[^>]+>)/g).map(segment => {
        if (/^<[^>]+>$/.test(segment)) {
            return segment;
        }
        return segment.replace(pattern, '<mark class="semantic-highlight">$1</mark>');
    }).join('');
}

function ensureQueryHighlight(text, queryTokens) {
    if (!text || !queryTokens || queryTokens.length === 0) {
        return text;
    }

    const container = document.createElement('div');
    container.innerHTML = text;

    const tokenPattern = new RegExp(`\\b(${queryTokens.map(escapeForRegex).join('|')})\\b`, 'gi');
    const walker = document.createTreeWalker(container, NodeFilter.SHOW_TEXT, null);
    const textNodes = [];

    while (walker.nextNode()) {
        textNodes.push(walker.currentNode);
    }

    textNodes.forEach(node => {
        const value = node.nodeValue;
        if (!value) return;

        let ancestor = node.parentNode;
        while (ancestor) {
            if (ancestor.nodeType === 1 && ancestor.tagName === 'MARK') {
                return;
            }
            ancestor = ancestor.parentNode;
        }

        tokenPattern.lastIndex = 0;
        const replacedValue = value.replace(tokenPattern, '<mark class="result-highlight">$1</mark>');
        if (replacedValue === value) return;

        const temp = document.createElement('span');
        temp.innerHTML = replacedValue;
        const fragment = document.createDocumentFragment();
        while (temp.firstChild) {
            fragment.appendChild(temp.firstChild);
        }
        node.parentNode.replaceChild(fragment, node);
    });

    return container.innerHTML;
}

function convertEmTags(text, { defaultClass = 'result-highlight', queryTokens = [] } = {}) {
    if (!text) {
        return text;
    }

    const normalizedTokens = queryTokens.map(token => token.toLowerCase());

    return text.replace(/<em>([\s\S]*?)<\/em>/gi, (match, inner) => {
        const normalizedInner = inner.replace(/<[^>]+>/g, ' ').toLowerCase();
        const hasQueryToken = normalizedTokens.length > 0 && normalizedTokens.some(token => normalizedInner.includes(token));
        const targetClass = hasQueryToken ? 'result-highlight' : defaultClass;
        return `<mark class="${targetClass}">${inner}</mark>`;
    });
}

function renderHighlightedText(text) {
    if (text == null) {
        return '';
    }

    const div = document.createElement('div');
    div.textContent = text;

    return div.innerHTML
        .replace(/&lt;mark class=&quot;result-highlight&quot;&gt;/g, '<mark class="result-highlight">')
        .replace(/&lt;mark class=&#39;result-highlight&#39;&gt;/g, '<mark class="result-highlight">')
        .replace(/&lt;mark class=&quot;semantic-highlight&quot;&gt;/g, '<mark class="semantic-highlight">')
        .replace(/&lt;mark class=&#39;semantic-highlight&#39;&gt;/g, '<mark class="semantic-highlight">')
        .replace(/&lt;mark class="semantic-highlight"&gt;/g, '<mark class="semantic-highlight">')
        .replace(/&lt;mark class='semantic-highlight'&gt;/g, '<mark class="semantic-highlight">')
        .replace(/&lt;mark class="result-highlight"&gt;/g, '<mark class="result-highlight">')
        .replace(/&lt;mark class='result-highlight'&gt;/g, '<mark class="result-highlight">')
        .replace(/&lt;\/mark&gt;/g, '</mark>')
        .replace(/&lt;br&gt;/g, '<br>');
}

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

function normalizeAssistantText(text) {
    if (typeof text !== 'string') {
        return '';
    }

    let normalized = text.replace(/\r/g, '');

    // Convert repeated <br> tags into single newline characters
    normalized = normalized.replace(/(<br\s*\/?>\s*)+/gi, '\n');

    // Collapse runs of blank lines (with optional whitespace) down to a single newline
    normalized = normalized.replace(/(\n\s*){2,}/g, '\n');

    return normalized;
}

function renderAssistantMarkdown(targetEl, text) {
    const normalized = normalizeAssistantText(text);

    if (!targetEl) {
        return normalized;
    }

    if (typeof marked !== 'undefined') {
        const rendered = marked.parse(normalized);
        const compacted = rendered.replace(/(?:<br\s*\/?>\s*){2,}/gi, '<br>');
        targetEl.innerHTML = compacted;
    } else {
        targetEl.textContent = normalized;
    }

    if (normalized) {
        targetEl.classList.add('is-visible');
    } else {
        targetEl.classList.remove('is-visible');
    }
    return normalized;
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
    
    // Search as you type with debouncing (disabled for AI mode)
    searchInput.addEventListener('input', () => {
        // Disable search-as-you-type for AI mode
        if (currentSearchMode === 'ai') {
            return;
        }
        
        // Clear any existing timeout
        if (searchTimeout) {
            clearTimeout(searchTimeout);
        }
        
        const query = searchInput.value.trim();
        
        // If input is cleared, show welcome message
        if (!query) {
            currentQuery = '';
            updateURL();
            clearResults();
            displayWelcomeMessage();
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
    const activeIndexBtn = document.querySelector('.index-btn.active');
    if (activeIndexBtn) {
        activeIndexBtn.classList.remove('btn-outline-primary');
        activeIndexBtn.classList.add('btn-primary');
    }
    
    // Mode toggle - listen to radio button changes
    const modeRadioInputs = document.querySelectorAll('input[name="searchMode"]');
    modeRadioInputs.forEach(radio => {
        radio.addEventListener('change', () => {
            if (radio.checked) {
                const label = document.querySelector(`label[for="${radio.id}"]`);
                currentSearchMode = label ? label.dataset.mode : radio.value;

                // Update URL and re-search if appropriate
                updateURL();
                // Don't auto-submit when switching to AI mode, but clear results and filters
                if (currentSearchMode === 'ai') {
                    clearResults();
                    // Clear facets but keep sidebar visible (it contains search mode selector)
                    facetsContainer.innerHTML = '';
                    // Display index selector and conversation history
                    ensureConversationSidebar({ forceIndex: true, refresh: true });
                    // Clear conversation ID when switching to AI mode (unless already set from URL)
                    if (!currentConversationId) {
                        updateURL();
                    }
                } else if (currentQuery || currentSearchMode === 'keyword' || currentSearchMode === 'semantic') {
                    // Clear conversation ID when switching away from AI mode
                    currentConversationId = null;
                    updateURL();
                    performSearch();
                    ensureConversationSidebar();
                }
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
        await displayResults(data);
    } catch (error) {
        showError(error.message);
    } finally {
        hideLoading();
    }
}

async function performStreamingSearch(query) {
    try {
        await ensureConversationSidebar();

        const requestBody = {
            query: query,
            filters: Object.keys(currentFilters).length > 0 ? currentFilters : {}
        };
        
        // Include conversation_id if present
        if (currentConversationId) {
            requestBody.conversation_id = currentConversationId;
        }

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
        await ensureConversationSidebar({ refresh: true });
    } catch (error) {
        showError(error.message);
    } finally {
        hideLoading();
    }
}

async function displayAIResults(response, query) {
    resultsContainer.innerHTML = `
        <div class="ai-stream-wrapper" id="ai-stream-container">
            <div class="ai-conversation-title" id="ai-conversation-title" style="display: none; padding: 12px; margin-bottom: 16px; background: var(--color-bg-secondary); border-radius: 8px; font-weight: 600; color: var(--bs-body-color);"></div>
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
    const conversationTitleEl = document.getElementById('ai-conversation-title');
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    const context = {
        streamContainer,
        assistantTextEl,
        typingIndicatorEl,
        toolEventsEl,
        statusBadgeEl,
        conversationTitleEl,
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
    const eventData = data.data || data;
    const {
        assistantTextEl,
        typingIndicatorEl,
        toolEventsEl,
        statusBadgeEl,
        conversationTitleEl
    } = context;

    const appendInfoEvent = (title, bodyText) => {
        const text = bodyText != null
            ? (typeof bodyText === 'string' ? bodyText : JSON.stringify(bodyText, null, 2))
            : '';

        const eventEl = document.createElement('div');
        eventEl.className = 'ai-tool-event ai-info-event';
        eventEl.style.backgroundColor = 'var(--color-bg-secondary)';
        eventEl.style.borderLeft = '3px solid #6c757d';

        const titleEl = document.createElement('div');
        titleEl.className = 'ai-tool-event-title';
        titleEl.style.color = '#6c757d';
        titleEl.textContent = title;

        const bodyEl = document.createElement('div');
        bodyEl.style.padding = '8px';
        bodyEl.style.whiteSpace = 'pre-wrap';
        bodyEl.textContent = text;

        eventEl.appendChild(titleEl);
        if (text) {
            eventEl.appendChild(bodyEl);
        }
        toolEventsEl.appendChild(eventEl);
        toolEventsEl.classList.add('is-visible');

        return eventEl;
    };

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

    // Handle events from async_events.txt documentation
    if (kind === 'conversation_id_set') {
        // Store conversation ID and update URL
        const conversationId = eventData.conversation_id;
        context.conversationId = conversationId;
        currentConversationId = conversationId;
        updateURL();
    } else if (kind === 'reasoning') {
        // Display AI's reasoning/thinking
        const reasoning = eventData.reasoning || '';
        if (reasoning) {
            appendInfoEvent('💭 Reasoning', reasoning);
            statusBadgeEl.textContent = 'Thinking…';
            statusBadgeEl.classList.remove('is-error');
        }
    } else if (kind === 'tool_call') {
        // Display tool call with parameters
        const toolId = eventData.tool_id || 'unknown';
        const params = eventData.params || {};
        const paramsText = Object.keys(params).length > 0
            ? JSON.stringify(params, null, 2)
            : '(no parameters)';
        appendToolEvent(`🔧 Calling Tool: ${toolId}`, paramsText);
        statusBadgeEl.textContent = 'Calling tool…';
        statusBadgeEl.classList.remove('is-error');
    } else if (kind === 'tool_progress') {
        // Display tool progress messages
        const progressMessages = [];

        if (typeof eventData.message === 'string') {
            progressMessages.push(eventData.message);
        }

        if (Array.isArray(eventData.data)) {
            eventData.data.forEach(item => {
                if (item && typeof item.message === 'string') {
                    progressMessages.push(item.message);
                }
            });
        } else if (eventData.data && typeof eventData.data === 'object' && typeof eventData.data.message === 'string') {
            progressMessages.push(eventData.data.message);
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
                appendInfoEvent('⏳ Progress', progressText);
            }

            statusBadgeEl.textContent = 'In progress…';
            statusBadgeEl.classList.remove('is-error');
        }
    } else if (kind === 'tool_result') {
        // Display tool results
        const toolId = eventData.tool_id || 'unknown';
        const results = eventData.results || [];
        const resultsText = Array.isArray(results)
            ? `Received ${results.length} result(s)`
            : JSON.stringify(results, null, 2);
        appendInfoEvent(`✅ Tool Result: ${toolId}`, resultsText);
        statusBadgeEl.textContent = 'Tool complete';
        statusBadgeEl.classList.remove('is-error');
    } else if (kind === 'message_chunk') {
        // Special handling: concatenate text chunks for teletype effect
        const textChunk = eventData.text_chunk || '';
        if (textChunk) {
            // Simply append the chunk to the buffer
            context.assistantBuffer = (context.assistantBuffer || '') + textChunk;
            assistantTextEl.textContent = context.assistantBuffer;
            assistantTextEl.classList.add('is-visible');
            typingIndicatorEl.classList.add('is-active');
            if (!statusBadgeEl.classList.contains('is-complete')) {
                statusBadgeEl.textContent = 'Responding…';
                statusBadgeEl.classList.remove('is-error');
            }
        }
    } else if (kind === 'message_complete') {
        // Message is complete - render as markdown
        const messageContent = eventData.message_content || '';
        if (messageContent && messageContent !== context.assistantBuffer) {
            // Use the complete message if different from what we've built
            context.assistantBuffer = messageContent;
        }
        context.assistantBuffer = renderAssistantMarkdown(assistantTextEl, context.assistantBuffer || '');
        typingIndicatorEl.classList.remove('is-active');
        statusBadgeEl.textContent = 'Response complete';
        statusBadgeEl.classList.remove('is-error');
    } else if (kind === 'thinking_complete') {
        // Thinking phase is complete
        appendInfoEvent('✓ Thinking Complete', '');
        statusBadgeEl.textContent = 'Ready to respond';
        statusBadgeEl.classList.remove('is-error');
    } else if (kind === 'round_complete') {
        // Round is complete - ensure final message is rendered compactly
        if (context.assistantBuffer) {
            context.assistantBuffer = renderAssistantMarkdown(assistantTextEl, context.assistantBuffer);
        }
        typingIndicatorEl.classList.remove('is-active');
        statusBadgeEl.textContent = 'Done';
        statusBadgeEl.classList.add('is-complete');
        context.hasFinalMessage = true;
    } else if (kind === 'conversation_created') {
        // Conversation was created - display title at top
        const title = eventData.title || 'Untitled';
        if (conversationTitleEl) {
            conversationTitleEl.textContent = title;
            conversationTitleEl.style.display = 'block';
        }
        appendInfoEvent('💬 Conversation Created', `"${title}"`);
        ensureConversationSidebar({ refresh: true });
    } else if (kind === 'conversation_updated') {
        // Conversation was updated
        const title = eventData.title || 'Untitled';
        if (conversationTitleEl) {
            conversationTitleEl.textContent = title;
            conversationTitleEl.style.display = 'block';
        }
        appendInfoEvent('💬 Conversation Updated', `"${title}"`);
        ensureConversationSidebar({ refresh: true });
    } else if (['tool_code', 'tool_start'].includes(kind)) {
        appendToolEvent(data.tool_name ? `Running ${data.tool_name}` : 'Tool Code', data.code || chunkText);
        statusBadgeEl.textContent = 'Calling tool…';
        statusBadgeEl.classList.remove('is-complete', 'is-error');
    } else if (['tool_output', 'tool_end'].includes(kind)) {
        const output = data.output || data.result || chunkText;
        appendToolEvent(data.tool_name ? `${data.tool_name} Output` : 'Tool Output', output);
        statusBadgeEl.textContent = 'Tool complete';
        statusBadgeEl.classList.remove('is-error');
    } else if (['agent_action'].includes(kind)) {
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
        const finalText = responseText || context.assistantBuffer;
        if (finalText) {
            context.assistantBuffer = renderAssistantMarkdown(assistantTextEl, finalText);
        }
        typingIndicatorEl.classList.remove('is-active');
        statusBadgeEl.textContent = 'Responded';
        statusBadgeEl.classList.add('is-complete');
        context.hasFinalMessage = true;
    } else if (kind === 'error') {
        const defaultError = currentSearchMode === 'ai' ? 'Flight AI agent not setup' : 'Unknown error';
        const errorMsg = eventData.message || data.message || chunkText || defaultError;
        appendInfoEvent('❌ Error', errorMsg);
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
        // Fallback: try to display as text
        applyAssistantText(chunkText);
    } else {
        // Unknown event - display in a minimal way
        console.warn('Unknown event type:', kind, data);
    }

    setTimeout(() => {
        context.streamContainer.scrollTop = context.streamContainer.scrollHeight;
    }, 0);
}

async function displayResults(data) {
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
    
    const isSemanticModeGlobal = currentSearchMode === 'semantic';
    const isKeywordModeGlobal = currentSearchMode === 'keyword';
    const showHighlightLegend = (isSemanticModeGlobal || isKeywordModeGlobal) &&
        (currentIndex === 'airlines' || currentIndex === 'contracts');
    const highlightLegendHtmlSemantic = `
        <div class="match-legend" role="status" aria-label="Highlight legend">
            <span class="legend-item">
                <span class="result-highlight">Keyword</span>
            </span>
            <span class="legend-item legend-item-clickable" id="semantic-legend-item" style="cursor: pointer;" title="Click to highlight semantic matches">
                <span class="semantic-highlight">Semantic / Similar</span>
            </span>
        </div>
    `;
    const highlightLegendHtmlKeyword = `
        <div class="match-legend" role="status" aria-label="Highlight legend">
            <span class="legend-item">
                <span class="result-highlight">Keyword</span>
            </span>
        </div>
    `;
    const highlightLegendHtml = isSemanticModeGlobal ? highlightLegendHtmlSemantic : highlightLegendHtmlKeyword;
    let legendInserted = false;
    const queryTokensGlobal = getQueryTokens(currentQuery);

    hits.forEach((hit) => {
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

        const isSemanticMode = isSemanticModeGlobal;
        let semanticTerms = null;
        if (isSemanticMode) {
            if (indexName === 'airlines') {
                semanticTerms = getSemanticHighlightTerms(hit);
            } else if (indexName === 'contracts') {
                semanticTerms = getSemanticHighlightTerms(hit, 'semantic_content');
            }
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
            if (showHighlightLegend && !legendInserted) {
                html += highlightLegendHtml;
                legendInserted = true;
            }
            // Airlines index - two field format
            // Prefer semantic highlight, then text highlight, then source values
            let airlineName = highlight['Airline_Name.semantic']?.[0] ||
                              highlight['Airline_Name.text']?.[0] ||
                              highlight['Airline_Name']?.[0] ||
                              '';
            let code = highlight['Reporting_Airline']?.[0] || '';

            const highlightClass = isSemanticMode ? 'semantic-highlight' : 'result-highlight';

            if (airlineName) {
                airlineName = convertEmTags(airlineName, {
                    defaultClass: highlightClass,
                    queryTokens: queryTokensGlobal
                });
            } else {
                airlineName = source.Airline_Name || 'Unknown Airline';
            }

            if (isSemanticMode) {
                const semanticallyHighlighted = applySemanticHighlight(airlineName, semanticTerms, queryTokensGlobal);
                const withQueryHighlight = ensureQueryHighlight(semanticallyHighlighted, queryTokensGlobal) || semanticallyHighlighted;
                if (withQueryHighlight === semanticallyHighlighted && currentQuery) {
                    airlineName = applyQueryHighlight(airlineName, currentQuery);
                } else {
                    airlineName = withQueryHighlight;
                }
            } else {
                airlineName = applyQueryHighlight(airlineName, currentQuery);
            }

            if (code) {
                code = convertEmTags(code, {
                    defaultClass: 'result-highlight',
                    queryTokens: queryTokensGlobal
                });
                code = applyQueryHighlight(code, currentQuery);
            } else {
                code = applyQueryHighlight(source.Reporting_Airline || 'N/A', currentQuery);
            }
            
            title = airlineName;
            url = code;
            snippets = [];
            meta = [];
            
        } else {
            if (showHighlightLegend && !legendInserted) {
                html += highlightLegendHtml;
                legendInserted = true;
            }
            // Contracts index (original logic)
            const rawTitle = highlight['attachment.title']?.[0] ||
                             source.attachment?.title ||
                             source.filename ||
                             'Untitled';

            const highlightClass = isSemanticMode ? 'semantic-highlight' : 'result-highlight';

            let processedTitle = convertEmTags(rawTitle, {
                defaultClass: highlightClass,
                queryTokens: queryTokensGlobal
            });

            if (isSemanticMode) {
                processedTitle = applySemanticHighlight(processedTitle, semanticTerms, queryTokensGlobal);
                processedTitle = ensureQueryHighlight(processedTitle, queryTokensGlobal) || processedTitle;
            } else {
                processedTitle = applyQueryHighlight(processedTitle, currentQuery);
            }

            title = processedTitle;

            url = source.filename || 'Unknown file';
            
            if (isSemanticMode && Array.isArray(highlight['semantic_content']) && highlight['semantic_content'].length > 0) {
                snippets = highlight['semantic_content'];
            } else {
                snippets = highlight['attachment.description'] || highlight['attachment.content'] || [];
            }
            if (snippets.length === 0) {
                const description = source.attachment?.description;
                const content = source.attachment?.content;
                const text = description || content || '';
                if (text) {
                    const fallbackLimit = isSemanticMode ? 100 : 400;
                    snippets = [text.substring(0, fallbackLimit) + (text.length > fallbackLimit ? '...' : '')];
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
            if (source.airline) {
                meta.push(`Airline: ${source.airline}`);
            }
        }
        
        // Build snippet HTML - process <em> tags in snippets
        const snippetFragments = snippets.map(snippet => {
            if (typeof snippet === 'string') {
                const snippetHighlightClass = (isSemanticMode && (indexName === 'airlines' || indexName === 'contracts'))
                    ? 'semantic-highlight'
                    : 'result-highlight';
                const baseSnippet = convertEmTags(snippet, {
                    defaultClass: snippetHighlightClass,
                    queryTokens: queryTokensGlobal
                });
                let snippetProcessed = baseSnippet;
                if (isSemanticMode && semanticTerms) {
                    snippetProcessed = applySemanticHighlight(snippetProcessed, semanticTerms, queryTokensGlobal);
                    snippetProcessed = ensureQueryHighlight(snippetProcessed, queryTokensGlobal) || snippetProcessed;
                } else if (isSemanticMode) {
                    snippetProcessed = ensureQueryHighlight(snippetProcessed, queryTokensGlobal) || snippetProcessed;
                } else {
                    snippetProcessed = applyQueryHighlight(snippetProcessed, currentQuery);
                }

                // Convert newlines to <br> tags for semantic mode on contracts
                if (isSemanticMode && indexName === 'contracts') {
                    // Collapse multiple consecutive newlines into single <br>
                    snippetProcessed = snippetProcessed.replace(/\n+/g, '<br>');
                }

                return renderHighlightedText(snippetProcessed);
            }
            return renderHighlightedText(String(snippet || ''));
        }).filter(fragment => fragment && fragment.trim());

        const snippetHtml = snippetFragments
            .map(fragment => {
                const ellipsis = isKeywordModeGlobal ? '' : '…';
                return `<div class="snippet-fragment">${ellipsis}${fragment}${ellipsis}</div>`;
            })
            .join('');
        
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
                            <i class="bi bi-file-earmark-text me-2"></i>${renderHighlightedText(title)}
                        </h6>
                        ${indexBadge}
                    </div>
                    ${snippetHtml ? `<div class="small snippet-group">${snippetHtml}</div>` : ''}
                    ${meta.length > 0 ? `<small class="text-muted d-block">
                        ${meta.map(m => {
                            if (m.includes('Uploaded')) return `<i class="bi bi-calendar3 me-1"></i>${m}`;
                            if (m.includes('Author')) return `<i class="bi bi-person me-1"></i>${m}`;
                            if (m.includes('Airline')) return `<i class="bi bi-airplane-engines me-1"></i>${m}`;
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
    // (displayFacets will also display conversation history if in AI mode)
    await displayFacets(data.aggregations);

    // Add click handler for semantic legend item
    const semanticLegendItem = document.getElementById('semantic-legend-item');
    if (semanticLegendItem) {
        semanticLegendItem.addEventListener('click', highlightSemanticMatches);
    }
}

async function displayFacets(aggregations) {
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

    // Index selector (hide when type=ai is in URL or currentSearchMode is 'ai')
    const params = new URLSearchParams(window.location.search);
    const urlType = params.get('type');
    const isAiMode = currentSearchMode === 'ai' || urlType === 'ai';
    
    if (!isAiMode) {
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
    }

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

async function loadConversations() {
    try {
        const response = await fetch('/api/conversations');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        console.log('Conversations API response:', data);
        return collectConversationRecords(data);
    } catch (error) {
        console.error('Error loading conversations:', error);
        return [];
    }
}

function hasConversationFields(record) {
    if (!record || typeof record !== 'object') {
        return false;
    }

    const candidate = record.conversation && typeof record.conversation === 'object'
        ? record.conversation
        : record;

    return Boolean(
        candidate.conversation_id ||
        candidate.conversationId ||
        candidate.id ||
        (candidate.metadata && (candidate.metadata.conversation_id || candidate.metadata.conversationId)) ||
        candidate.title ||
        candidate.name
    );
}

function collectConversationRecords(payload) {
    const results = [];
    const visited = new WeakSet();
    const seen = new Set();

    function walk(node) {
        if (!node || typeof node !== 'object') {
            return;
        }
        if (visited.has(node)) {
            return;
        }
        visited.add(node);

        if (Array.isArray(node)) {
            node.forEach(item => walk(item));
            return;
        }

        if (hasConversationFields(node)) {
            const normalized = normalizeConversationRecord(node);
            const key = normalized.conversation_id || normalized.title || JSON.stringify(normalized);
            if (!seen.has(key)) {
                seen.add(key);
                results.push(normalized);
            }
        }

        Object.values(node).forEach(value => {
            if (value && typeof value === 'object') {
                walk(value);
            }
        });
    }

    walk(payload);
    return results;
}

async function displayIndexSelector() {
    // Display just the index selector for the homepage
    let facetsHtml = '';

    // Index selector (hide when type=ai is in URL or currentSearchMode is 'ai')
    const params = new URLSearchParams(window.location.search);
    const urlType = params.get('type');
    const isAiMode = currentSearchMode === 'ai' || urlType === 'ai';
    
    if (!isAiMode) {
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

            facetsHtml += `
                <button class="btn btn-sm ${activeClass} facet-btn index-switch-btn"
                        data-index="${escapeHtml(index.key)}">
                    ${index.label}
                </button>
            `;
        });

        facetsHtml += '</div>';
    }

    facetsContainer.innerHTML = facetsHtml;

    // Show sidebar
    if (filterSidebar.style.display !== 'block') {
        filterSidebar.style.display = 'block';
        if (filterToggleMobile) {
            filterToggleMobile.style.display = 'block';
        }
    }

    // Attach event listeners to index switch buttons
    facetsContainer.querySelectorAll('.index-switch-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const newIndex = this.dataset.index;
            switchToIndex(newIndex);
        });
    });

}

async function displayConversationHistory() {
    console.log('displayConversationHistory called, currentSearchMode:', currentSearchMode);
    const sidebar = getOrCreateConversationSidebar();
    if (!sidebar) return;

    const conversations = await loadConversations();
    console.log('Loaded conversations:', conversations);
    
    let conversationsHtml = '<div class="filter-section conversations-section">';
    conversationsHtml += '<div class="filter-title"><i class="bi bi-chat-left-text me-2"></i>Conversations</div>';
    
    // Add "New Conversation" button (always show)
    conversationsHtml += `
        <button class="btn btn-sm btn-primary facet-btn new-conversation-btn" id="newConversationBtn">
            <i class="bi bi-plus-circle me-1"></i> New Conversation
        </button>
    `;
    
    if (conversations.length > 0) {
        conversations.forEach(conv => {
            const record = normalizeConversationRecord(conv);
            const title = record.title || 'Untitled Conversation';
            const id = record.conversation_id || '';
            const date = record.updated_at || record.created_at || '';
            let dateStr = '';
            if (date) {
                try {
                    const dateObj = new Date(date);
                    dateStr = dateObj.toLocaleDateString();
                } catch (e) {
                    dateStr = '';
                }
            }
            
            const isActive = currentConversationId && id && currentConversationId === id;
            const activeClass = isActive ? 'btn-primary' : 'btn-outline-secondary';
            
            conversationsHtml += `
                <div class="conversation-row" data-conversation-id="${escapeHtml(id)}">
                    <button class="btn btn-sm ${activeClass} facet-btn conversation-btn"
                            data-conversation-id="${escapeHtml(id)}"
                            title="${escapeHtml(title)}">
                        <div class="conversation-item">
                            <div class="conversation-title">${escapeHtml(title)}</div>
                            <div class="conversation-date">
                                <span class="conversation-delete-link"
                                      role="button"
                                      tabindex="0"
                                      data-conversation-id="${escapeHtml(id)}">
                                    Delete
                                </span>
                            </div>
                        </div>
                    </button>
                </div>
            `;
        });
    } else {
        conversationsHtml += '<div class="text-muted small mt-2" style="text-align: center; padding: 0.5rem;">No previous conversations</div>';
    }
    
    conversationsHtml += '</div>';

    sidebar.innerHTML = conversationsHtml;
    sidebar.style.display = 'block';
    
    // Attach event listener to "New Conversation" button
    const newConversationBtn = sidebar.querySelector('#newConversationBtn');
    if (newConversationBtn) {
        newConversationBtn.addEventListener('click', createNewConversation);
    }
    
    // Attach event listeners to conversation buttons
    sidebar.querySelectorAll('.conversation-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            sidebar.querySelectorAll('.conversation-btn').forEach(button => {
                button.classList.remove('btn-primary');
                button.classList.add('btn-outline-secondary');
            });

            this.classList.remove('btn-outline-secondary');
            this.classList.add('btn-primary');

            const conversationId = this.dataset.conversationId;
            loadConversation(conversationId);
        });
    });

    // Attach event listeners to delete links
    sidebar.querySelectorAll('.conversation-delete-link').forEach(link => {
        link.addEventListener('click', function(event) {
            event.stopPropagation();
            event.preventDefault();
            const conversationId = this.dataset.conversationId;
            handleConversationDelete(conversationId);
        });
        link.addEventListener('keypress', function(event) {
            if (event.key === 'Enter' || event.key === ' ') {
                event.preventDefault();
                const conversationId = this.dataset.conversationId;
                handleConversationDelete(conversationId);
            }
        });
    });
}

async function ensureConversationSidebar(options = {}) {
    const { refresh = false, forceIndex = false } = options;

    if (!facetsContainer) {
        return;
    }

    if (currentSearchMode !== 'ai') {
        const sidebar = conversationSidebar || document.getElementById('conversationSidebar');
        if (sidebar) {
            sidebar.innerHTML = '';
            sidebar.style.display = 'none';
            delete sidebar.dataset.loaded;
        }
        return;
    }

    const sidebar = getOrCreateConversationSidebar();
    if (!sidebar) {
        return;
    }

    const hasIndexSelector = facetsContainer.querySelector('.index-switch-btn');
    if (!hasIndexSelector || forceIndex) {
        await displayIndexSelector();
    }

    const needsConversations = refresh ||
        !sidebar.dataset.loaded ||
        sidebar.innerHTML.trim() === '';

    if (needsConversations) {
        await displayConversationHistory();
        sidebar.dataset.loaded = 'true';
        sidebar.style.display = 'block';
    }
}

function getOrCreateConversationSidebar() {
    if (conversationSidebar && conversationSidebar instanceof HTMLElement) {
        return conversationSidebar;
    }

    const container = document.createElement('div');
    container.id = 'conversationSidebar';
    container.className = 'conversation-sidebar';
    container.style.display = 'none';

    const sidebarContent = filterSidebar ? filterSidebar.querySelector('.sidebar-content') : null;
    if (sidebarContent) {
        sidebarContent.insertBefore(container, facetsContainer || null);
    } else if (facetsContainer && facetsContainer.parentNode) {
        facetsContainer.parentNode.insertBefore(container, facetsContainer);
    } else {
        console.error('Unable to insert conversation sidebar - container not found');
        return null;
    }

    conversationSidebar = container;
    return container;
}

function normalizeConversationRecord(raw) {
    if (!raw || typeof raw !== 'object') {
        return {};
    }

    const record = raw.conversation && typeof raw.conversation === 'object'
        ? raw.conversation
        : raw;

    const metadata = record.metadata && typeof record.metadata === 'object'
        ? record.metadata
        : {};

    return {
        ...record,
        conversation_id: record.conversation_id ||
            record.conversationId ||
            metadata.conversation_id ||
            metadata.conversationId ||
            record.id ||
            metadata.id ||
            '',
        title: record.title ||
            record.name ||
            metadata.title ||
            metadata.name ||
            '',
        created_at: record.created_at ||
            record.createdAt ||
            metadata.created_at ||
            metadata.createdAt ||
            '',
        updated_at: record.updated_at ||
            record.updatedAt ||
            metadata.updated_at ||
            metadata.updatedAt ||
            ''
    };
}


function createNewConversation() {
    // Clear conversation ID
    currentConversationId = null;
    
    // Clear search input and results
    searchInput.value = '';
    currentQuery = '';
    clearResults();
    
    // Update URL (removes conversation_id)
    updateURL();
    
    // Focus on search input
    searchInput.focus();

    ensureConversationSidebar({ refresh: true });
}

async function handleConversationDelete(conversationId) {
    if (!conversationId) {
        return;
    }

    const confirmed = window.confirm('Delete this conversation?');
    if (!confirmed) {
        return;
    }

    try {
        const response = await fetch(`/api/conversations/${encodeURIComponent(conversationId)}`, {
            method: 'DELETE'
        });

        if (!response.ok) {
            let errorMessage = 'Failed to delete conversation';
            try {
                const data = await response.json();
                if (data && data.error) {
                    errorMessage = data.error;
                }
            } catch (err) {
                // ignore json parse errors
            }
            throw new Error(errorMessage);
        }

        if (currentConversationId === conversationId) {
            currentConversationId = null;
            updateURL();
            clearResults();
        }

        await ensureConversationSidebar({ refresh: true });
    } catch (error) {
        console.error('Error deleting conversation:', error);
        showError(error.message || 'Failed to delete conversation');
        setTimeout(() => {
            hideError();
        }, 4000);
    }
}

async function loadConversation(conversationId) {
    // Switch to AI mode if not already
    if (currentSearchMode !== 'ai') {
        const aiModeRadio = document.getElementById('mode-ai');
        if (aiModeRadio) {
            aiModeRadio.checked = true;
            aiModeRadio.dispatchEvent(new Event('change'));
        }
    }
    
    // Set current conversation ID
    currentConversationId = conversationId;
    
    // Clear search input and results
    searchInput.value = '';
    currentQuery = '';
    clearResults();
    
    // Update URL with conversation ID
    updateURL();
    
    // Focus on search input
    searchInput.focus();
    
    // TODO: Load conversation messages and display them
    // For now, the conversation will be loaded when user sends a message
}

function hideFacets() {
    // Keep sidebar visible since it now contains the search mode selector
    // Only hide if there are no facets and no search mode selector
    facetsContainer.innerHTML = '';
    // Only hide sidebar if it's not needed (e.g., on reset when there's no search)
    // For now, keep it visible so users can always access search mode
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

function displayWelcomeMessage() {
    resultsContainer.innerHTML = `
        <div class="welcome-message">
            <div class="welcome-header">
                <p class="welcome-subtitle">I can help answers questions about:</p>
            </div>
            <div class="welcome-content">
                <div class="welcome-card" data-index="flights" role="button" tabindex="0">
                    <div class="welcome-icon">
                        <i class="bi bi-airplane-engines"></i>
                    </div>
                    <div class="welcome-card-content">
                        <h3 class="welcome-card-title">US Domestic Flights</h3>
                        <p class="welcome-card-text">Flight schedules, delays, cancellations, and statistics from 2019 to 2025</p>
                    </div>
                </div>
                <div class="welcome-card" data-index="airlines" role="button" tabindex="0">
                    <div class="welcome-icon">
                        <i class="bi bi-building"></i>
                    </div>
                    <div class="welcome-card-content">
                        <h3 class="welcome-card-title">Major US Airlines</h3>
                        <p class="welcome-card-text">Information about United, American, Delta, and Southwest</p>
                    </div>
                </div>
                <div class="welcome-card" data-index="contracts" role="button" tabindex="0">
                    <div class="welcome-icon">
                        <i class="bi bi-file-earmark-text"></i>
                    </div>
                    <div class="welcome-card-content">
                        <h3 class="welcome-card-title">Contracts of Carriage</h3>
                        <p class="welcome-card-text">Policies, rules, baggage fees, and travel terms for each airline</p>
                    </div>
                </div>
            </div>
        </div>
    `;
    // Display index selector in sidebar
    displayIndexSelector();
    
    // Add click handlers to welcome cards
    document.querySelectorAll('.welcome-card[data-index]').forEach(card => {
        const index = card.dataset.index;
        const handleClick = () => {
            switchToIndex(index);
        };
        card.addEventListener('click', handleClick);
        card.addEventListener('keypress', (e) => {
            if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                handleClick();
            }
        });
    });
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
    const keywordRadio = document.getElementById('mode-keyword');
    if (keywordRadio) {
        keywordRadio.checked = true;
    }

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

    // Clear conversation ID
    currentConversationId = null;

    // Clear results and show welcome message
    clearResults();
    displayWelcomeMessage();

    // Hide loading and error messages
    hideLoading();
    hideError();

    // Update URL (clears all parameters)
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
    
    // Add conversation ID if present (for AI mode)
    if (currentConversationId) {
        params.set('conversation_id', currentConversationId);
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
    const conversationId = params.get('conversation_id');

    // Restore search mode (even if no query)
    if (type && ['keyword', 'semantic', 'ai'].includes(type)) {
        currentSearchMode = type;
        // Update radio button state
        const modeRadio = document.getElementById(`mode-${type}`);
        if (modeRadio) {
            modeRadio.checked = true;
        }
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

    // Restore conversation ID from URL
    if (conversationId) {
        currentConversationId = conversationId;
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

    if (currentSearchMode === 'ai') {
        ensureConversationSidebar();
    }

    if (query) {
        // Restore search input
        searchInput.value = query;
        currentQuery = query;

        // Perform the search
        performSearch();
    } else {
        // If there's no query, show welcome message or index selector
        currentQuery = '';
        if (currentSearchMode !== 'ai') {
            displayWelcomeMessage();
        }
    }
}

function highlightSemanticMatches() {
    // Find all semantic-highlight elements in the results
    const semanticHighlights = document.querySelectorAll('.semantic-highlight');
    
    if (semanticHighlights.length === 0) {
        return;
    }
    
    // Add underline animation class to all semantic highlights
    semanticHighlights.forEach(element => {
        element.classList.add('highlight-underline');
    });
    
    // Remove the underline class after animation completes (3s: 1s animation + 2s pause)
    setTimeout(() => {
        semanticHighlights.forEach(element => {
            element.classList.remove('highlight-underline');
        });
    }, 3000);
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
