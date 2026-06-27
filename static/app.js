// Spectrum UI Client-Side Logic
document.addEventListener('DOMContentLoaded', async () => {
    // State management
    const state = {
        vms: [],
        nodes: [],
        clusterName: 'unnamed-cluster',
        pollingInterval: null,
        apiHost: '', // Local base URL
        storageContainers: [],
        isHoveringVmsTable: false,
        availableImages: []
    };

    // Immediate state restoration from session cache
    try {
        const cachedDataStr = sessionStorage.getItem('status_api_cache');
        if (cachedDataStr) {
            const cachedData = JSON.parse(cachedDataStr);
            state.vms = (cachedData.vms && cachedData.vms.list) ? cachedData.vms.list : (cachedData.vms || []);
            state.nodes = cachedData.nodes || [];
            state.clusterName = cachedData.cluster_name || 'unnamed-cluster';
            state.metrics = cachedData.metrics || null;
            
            // Set cached leader immediately in DOM if element exists
            const leaderNode = state.nodes.find(n => n.role === 'Leader') || state.nodes[0];
            const leaderNodeDisplay = document.getElementById('leader-node-display');
            if (leaderNodeDisplay && leaderNode) {
                leaderNodeDisplay.textContent = `${formatNodeName(leaderNode.name)} (${leaderNode.role})`;
            }
            const brandDisplay = document.getElementById('cluster-name-brand-display');
            if (brandDisplay) {
                brandDisplay.textContent = state.clusterName;
            }
            const clusterNameDisplay = document.getElementById('cluster-name-display');
            if (clusterNameDisplay) {
                clusterNameDisplay.textContent = state.clusterName;
            }
        }
    } catch (e) {
        console.error("Failed to load cached status data:", e);
    }

    let activeRfb = null;
    window.addEventListener('resize', () => {
        if (activeRfb) {
            try {
                activeRfb._requestRemoteResize();
            } catch (e) {
                console.error("Failed to request remote resize:", e);
            }
        }
    });
    let vncRetryCount = 0;
    let vncRetryTimeout = null;

    function formatNodeName(name) {
        if (!name) return 'N/A';
        return name;
    }

    function loadStorageContainers() {
        return fetch(`${state.apiHost}/api/storage/containers`)
            .then(res => res.json())
            .then(data => {
                state.storageContainers = data.containers || [];
            })
            .catch(err => console.error("Error loading storage containers:", err));
    }

    function loadAvailableImages() {
        return fetch(`${state.apiHost}/api/images`)
            .then(res => res.json())
            .then(data => {
                const images = data.images || data;
                state.availableImages = Array.isArray(images) ? images.filter(img => img.type === 'iso' || img.filename.endsWith('.iso')) : [];
            })
            .catch(err => console.error("Error loading available images:", err));
    }

    function loadNetworkSegmentsForDropdowns(selectIds, selectedNetworkId = null) {
        return fetch(`${state.apiHost}/api/networks`)
            .then(res => res.json())
            .then(data => {
                const networks = data.networks || [];
                selectIds.forEach(id => {
                    const selectEl = document.getElementById(id);
                    if (!selectEl) return;
                    
                    selectEl.innerHTML = '';
                    networks.forEach(net => {
                        const option = document.createElement('option');
                        option.value = net.id;
                        let text = net.name;
                        if (net.type === 'vlan') {
                            text += ` (VLAN ${net.vlan_id})`;
                        } else if (net.type === 'direct') {
                            text += ' (Direct / Bridged)';
                        }
                        option.textContent = text;
                        if (selectedNetworkId && net.id === selectedNetworkId) {
                            option.selected = true;
                        }
                        selectEl.appendChild(option);
                    });
                });
            })
            .catch(err => console.error("Error loading network segments:", err));
    }


    function recalculateCdromIndices(container) {
        if (!container) return;
        const rows = container.querySelectorAll('.cdrom-row');
        rows.forEach((row, idx) => {
            const titleEl = row.querySelector('.cdrom-title');
            if (titleEl) {
                titleEl.textContent = `CD-ROM Drive #${idx + 1}`;
            }
        });
    }

    function addCdromRow(containerId, selectedIso = "") {
        const container = document.getElementById(containerId);
        if (!container) return;
        
        const row = document.createElement('div');
        row.className = 'cdrom-row glass-card';
        row.style.display = 'flex';
        row.style.flexDirection = 'column';
        row.style.padding = '12px';
        row.style.borderRadius = '8px';
        row.style.border = '1px solid rgba(255, 255, 255, 0.08)';
        row.style.background = 'rgba(255, 255, 255, 0.02)';
        row.style.marginBottom = '12px';
        row.style.gap = '8px';
        row.style.width = '100%';
        
        let selectOptions = '<option value="">None / Empty Drive</option>';
        state.availableImages.forEach(img => {
            selectOptions += `<option value="${img.name}" ${img.name === selectedIso ? 'selected' : ''}>${img.name} (${(img.size_bytes / (1024*1024*1024)).toFixed(2)} GB)</option>`;
        });

        row.innerHTML = `
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <span class="cdrom-title" style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: #fff;">CD-ROM Drive</span>
                <button type="button" class="btn-remove-cdrom" style="padding: 4px; cursor: pointer; display: flex; align-items: center; justify-content: center; background: none; border: none; color: #ef4444; font-size: 12px; font-family: 'Space Grotesk', sans-serif; font-weight: 600; gap: 4px;">
                    <svg viewBox="0 0 24 24" style="width: 14px; height: 14px; fill: none; stroke: currentColor; stroke-width: 2;"><polyline points="3 6 5 6 21 6"></polyline><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path><line x1="10" y1="11" x2="10" y2="17"></line><line x1="14" y1="11" x2="14" y2="17"></line></svg>
                    Remove
                </button>
            </div>
            <div style="display: flex; flex-direction: column; gap: 4px; width: 100%;">
                <label style="font-size: 11px; color: var(--text-muted); font-family: 'Space Grotesk', sans-serif;">ISO Image File</label>
                <select class="form-input cdrom-select" style="width: 100%; height: 32px; font-size: 12px;">
                    ${selectOptions}
                </select>
            </div>
        `;
        
        const removeBtn = row.querySelector('.btn-remove-cdrom');
        removeBtn.addEventListener('click', () => {
            row.remove();
            recalculateCdromIndices(container);
        });
        
        container.appendChild(row);
        recalculateCdromIndices(container);
    }



    // DOM Elements - Shared Header
    const clusterNameDisplay = document.getElementById('cluster-name-display');
    const leaderNodeDisplay = document.getElementById('leader-node-display');
    
    // Dropdown Navigation elements
    const dropdownBtn = document.getElementById('console-dropdown-btn');
    const dropdownContainer = document.querySelector('.console-dropdown');

    // Dashboard-specific elements
    const vcpusStat = document.getElementById('vcpus-stat');
    const vcpusBar = document.getElementById('vcpus-bar');
    const memoryStat = document.getElementById('memory-stat');
    const memoryBar = document.getElementById('memory-bar');
    const nodesContainer = document.getElementById('nodes-container');
    const storageUsedDisplay = document.getElementById('storage-used-display');
    const storageBar = document.getElementById('storage-bar');
    const eventsContainer = document.getElementById('events-log-container');

    // VMs-specific elements
    const vmsTableBody = document.getElementById('vms-table-body');
    const vmSearch = document.getElementById('vm-search');
    const sidebarTotalVms = document.getElementById('sidebar-total-vms');
    const sidebarRunningVms = document.getElementById('sidebar-running-vms');
    const sidebarStoppedVms = document.getElementById('sidebar-stopped-vms');

    // Storage-specific elements
    const storageNbdTableBody = document.getElementById('storage-nbd-table-body');

    // Network-specific elements
    const networkTapsTableBody = document.getElementById('network-taps-table-body');

    // Consensus-specific elements
    const consensusRingStatus = document.getElementById('consensus-ring-status');

    // Wizard/Create VM elements (on vms.html)
    const btnOpenCreateModal = document.getElementById('btn-open-create-modal');
    const btnCloseCreateModal = document.getElementById('btn-close-create-modal');
    const createVmOverlay = document.getElementById('create-vm-overlay');
    const wizardStepNavs = document.querySelectorAll('.step-nav');
    const wizardPanes = document.querySelectorAll('.wizard-pane');
    const btnWizardPrev = document.getElementById('btn-wizard-prev');
    const btnWizardNext = document.getElementById('btn-wizard-next');
    const createVmForm = document.getElementById('create-vm-form');

    let currentStep = 1;
    
    // Form Inputs
    const inputVmName = document.getElementById('vm-name');
    const selectVmVcpus = document.getElementById('vm-vcpus');
    const selectVmMemory = document.getElementById('vm-memory');
    const selectVmMemoryUnit = document.getElementById('vm-memory-unit');
    const selectVmFirmware = document.getElementById('vm-firmware');
    const cdromListContainer = document.getElementById('cdrom-list-container');
    const btnAddCdrom = document.getElementById('btn-add-cdrom');
    const editCdromListContainer = document.getElementById('edit-cdrom-list-container');
    const btnEditAddCdrom = document.getElementById('btn-edit-add-cdrom');
    const diskListContainer = document.getElementById('disk-list-container');
    const btnAddDisk = document.getElementById('btn-add-disk');

    // Review details elements
    const reviewVmName = document.getElementById('review-vm-name');
    const reviewVmVcpus = document.getElementById('review-vm-vcpus');
    const reviewVmMemory = document.getElementById('review-vm-memory');
    const reviewVmFirmware = document.getElementById('review-vm-firmware');
    const reviewVmDisks = document.getElementById('review-vm-disks');
    const reviewVmIso = document.getElementById('review-vm-iso');

    // Toast container
    const toastContainer = document.getElementById('toast-container');

    // Global token fallback for environments where localStorage/sessionStorage is blocked
    window.helios_token_cache = window.helios_token_cache || "";
    
    function getStoredToken() {
        try {
            return sessionStorage.getItem('helios_session_token') || 
                   localStorage.getItem('helios_session_token') || 
                   window.helios_token_cache;
        } catch (e) {
            return window.helios_token_cache;
        }
    }
    
    function setStoredToken(token) {
        window.helios_token_cache = token;
        try {
            localStorage.setItem('helios_session_token', token);
            sessionStorage.setItem('helios_session_token', token);
        } catch (e) {
            console.warn("Storage access failed:", e);
        }
    }
    
    function removeStoredToken() {
        window.helios_token_cache = "";
        try {
            localStorage.removeItem('helios_session_token');
            sessionStorage.removeItem('helios_session_token');
        } catch (e) {
            console.warn("Storage access failed:", e);
        }
    }

    // Global Fetch Interceptor for 401 Unauthorized & Token Injection
    const originalFetch = window.fetch;
    window.fetch = async function(input, init) {
        let url = "";
        let options = init || {};
        
        if (input instanceof Request) {
            url = input.url;
        } else {
            url = String(input);
        }
        
        const token = getStoredToken();
        if (token) {
            if (input instanceof Request) {
                try {
                    const newHeaders = new Headers(input.headers);
                    newHeaders.set('Authorization', `Bearer ${token}`);
                    input = new Request(input, { headers: newHeaders });
                } catch (e) {}
            } else {
                if (!options.headers) {
                    options.headers = {};
                }
                if (options.headers instanceof Headers) {
                    options.headers.set('Authorization', `Bearer ${token}`);
                } else if (Array.isArray(options.headers)) {
                    let hasAuth = false;
                    for (let i = 0; i < options.headers.length; i++) {
                        if (options.headers[i][0].toLowerCase() === 'authorization') {
                            hasAuth = true;
                            break;
                        }
                    }
                    if (!hasAuth) {
                        options.headers.push(['Authorization', `Bearer ${token}`]);
                    }
                } else {
                    if (!options.headers['Authorization'] && !options.headers['authorization']) {
                        options.headers['Authorization'] = `Bearer ${token}`;
                    }
                }
            }
        }
        
        try {
            const response = await originalFetch(input, options);
            if (response.status === 401 && !url.endsWith('/api/login') && !url.endsWith('/api/auth/check')) {
                handleUnauthorized();
            }
            return response;
        } catch (error) {
            throw error;
        }
    };

    function handleUnauthorized() {
        if (state.pollingInterval) {
            clearInterval(state.pollingInterval);
            state.pollingInterval = null;
        }
        if (state.dagurInterval) {
            clearInterval(state.dagurInterval);
            state.dagurInterval = null;
        }
        initLoginOverlay();
        document.getElementById('login-overlay').classList.add('active');
    }

    function initLoginOverlay() {
        if (document.getElementById('login-overlay')) return;
        
        const overlay = document.createElement('div');
        overlay.id = 'login-overlay';
        overlay.className = 'login-overlay';
        overlay.innerHTML = `
            <div class="login-card">
                <div class="pe-logo-container" style="margin: 0 auto 15px auto; width: 48px; height: 48px; display: flex; justify-content: center; align-items: center; background: rgba(59, 130, 246, 0.1); border-radius: 12px; border: 1px solid rgba(59, 130, 246, 0.25);">
                    <svg viewBox="0 0 24 24" style="width: 28px; height: 28px; fill: var(--color-primary);">
                        <path d="M12 2L3 5v6c0 5.5 3.8 10.6 9 11 5.2-.4 9-5.5 9-11V5l-9-3z" />
                        <path d="M9 12l2 2 4-4" stroke="#fff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" fill="none" />
                    </svg>
                </div>
                <h2 class="login-title">Helios Login</h2>
                <p class="login-subtitle">Please sign in to manage the hyperconverged cluster.</p>
                <form id="login-form" class="login-form">
                    <div class="form-group" style="margin-bottom: 15px;">
                        <label style="display: block; font-size: 12px; margin-bottom: 6px; font-weight: 500;">Username</label>
                        <input type="text" id="login-username" class="form-input" style="width: 100%;" placeholder="Enter username" required autocomplete="username">
                    </div>
                    <div class="form-group" style="margin-bottom: 15px;">
                        <label style="display: block; font-size: 12px; margin-bottom: 6px; font-weight: 500;">Password</label>
                        <input type="password" id="login-password" class="form-input" style="width: 100%;" placeholder="Enter password" required autocomplete="current-password">
                    </div>
                    <div class="form-group" style="margin-bottom: 20px; display: flex; align-items: center; gap: 8px; justify-content: flex-start;">
                        <input type="checkbox" id="login-remember-me" style="cursor: pointer; width: 14px; height: 14px; accent-color: var(--color-primary);">
                        <label for="login-remember-me" style="font-size: 11.5px; color: var(--text-secondary); cursor: pointer; user-select: none; margin-bottom: 0;">Save Username</label>
                    </div>
                    <button type="submit" class="login-btn" id="login-submit-btn">Sign In</button>
                </form>
            </div>
        `;
        document.body.appendChild(overlay);
        
        // Load saved username
        const savedUsername = localStorage.getItem('helios_saved_username') || '';
        document.getElementById('login-username').value = savedUsername;
        if (savedUsername) {
            document.getElementById('login-remember-me').checked = true;
        }
        
        document.getElementById('login-form').addEventListener('submit', async function(e) {
            e.preventDefault();
            const usernameInput = document.getElementById('login-username');
            const passwordInput = document.getElementById('login-password');
            const submitBtn = document.getElementById('login-submit-btn');
            
            submitBtn.disabled = true;
            submitBtn.textContent = 'Signing in...';
            
            try {
                const res = await fetch('/api/login', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        username: usernameInput.value,
                        password: passwordInput.value
                    })
                });
                
                if (res.ok) {
                    const data = await res.json();
                    if (data.token) {
                        setStoredToken(data.token);
                    }
                    if (document.getElementById('login-remember-me').checked) {
                        localStorage.setItem('helios_saved_username', usernameInput.value);
                    } else {
                        localStorage.removeItem('helios_saved_username');
                    }
                    showToast('Welcome Back', 'Successfully signed in as ' + usernameInput.value, 'success');
                    overlay.classList.remove('active');
                    passwordInput.value = '';
                    checkAuth();
                } else {
                    showToast('Authentication Failed', 'Invalid username or password', 'error');
                }
            } catch (err) {
                showToast('Connection Failed', 'Could not connect to Spectrum server', 'error');
            } finally {
                submitBtn.disabled = false;
                submitBtn.textContent = 'Sign In';
            }
        });
    }

    async function checkAuth() {
        try {
            const res = await fetch('/api/auth/check');
            const data = await res.json();
            if (data.authenticated) {
                state.currentUser = data.username;
                updateUserProfile(data.username);
                
                const overlay = document.getElementById('login-overlay');
                if (overlay) overlay.classList.remove('active');
                
                if (!state.pollingInterval) {
                    Promise.all([loadStorageContainers(), loadAvailableImages(), refreshNetworksData()]).then(() => {
                        fetchStatus();
                    });
                    state.pollingInterval = setInterval(fetchStatus, 4000);
                    
                    fetchDagurSchedules();
                    fetchDagurRuns();
                    if (dagurSchedulesTbody) {
                        state.dagurInterval = setInterval(() => {
                            fetchDagurSchedules();
                            fetchDagurRuns();
                        }, 5000);
                    }
                }
            } else {
                handleUnauthorized();
            }
        } catch (err) {
            console.error("Auth check failed:", err);
            handleUnauthorized();
        }
    }

    function updateUserProfile(username) {
        const userProfile = document.querySelector('.user-profile');
        if (userProfile) {
            const firstLetter = username.charAt(0).toUpperCase();
            userProfile.innerHTML = `
                <div class="avatar" style="cursor: pointer;" id="btn-user-avatar">${firstLetter}</div>
                <span style="cursor: pointer;" id="btn-user-name">${username}</span>
                <button id="btn-logout" class="btn-secondary" style="margin-left: 10px; padding: 2px 6px; font-size: 10px; height: 24px; font-family: 'Space Grotesk', sans-serif; font-weight: 600;">Logout</button>
            `;
            userProfile.style.opacity = '1';
            const logoutBtn = document.getElementById('btn-logout');
            if (logoutBtn) {
                logoutBtn.addEventListener('click', handleLogout);
            }
        }
    }

    async function handleLogout() {
        try {
            await fetch('/api/auth/logout', { method: 'POST' });
        } catch (err) {
            console.error("Logout failed:", err);
        } finally {
            removeStoredToken();
            showToast('Logged Out', 'Successfully signed out', 'info');
            checkAuth();
        }
    }

    // ----------------------------------------------------
    // Tab switching for Settings Page
    // ----------------------------------------------------
    const settingsNavBtns = document.querySelectorAll('.settings-nav-btn');
    if (settingsNavBtns.length > 0) {
        settingsNavBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                const tab = btn.getAttribute('data-tab');
                
                settingsNavBtns.forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                
                const panes = document.querySelectorAll('.tab-content');
                panes.forEach(pane => pane.classList.remove('active'));
                
                const activePane = document.getElementById(`tab-pane-${tab}`);
                if (activePane) {
                    activePane.classList.add('active');
                }
                
                if (tab === 'users') {
                    loadUsersList();
                } else if (tab === 'node-ops') {
                    updateNodeOpsTable();
                } else if (tab === 'networks') {
                    initNetworksPage();
                }
            });
        });
    }

    // ----------------------------------------------------
    // Clickable Alerts Redirection (Event Delegation)
    // ----------------------------------------------------
    document.addEventListener('click', (e) => {
        const item = e.target.closest('.clickable-alert');
        if (item) {
            const check = item.getAttribute('data-check');
            const node = item.getAttribute('data-node');
            if (check) {
                window.location.href = `health.html?check=${encodeURIComponent(check)}&node=${encodeURIComponent(node)}`;
            }
        }
    });

    // ----------------------------------------------------
    // Cluster Maintenance Tab Operations
    // ----------------------------------------------------
    function initMaintenanceOperations() {
        const rebalanceBtn = document.getElementById('btn-maint-rebalance');
        const cleanupBtn = document.getElementById('btn-maint-cleanup');
        const dbCleanupBtn = document.getElementById('btn-maint-dbcleanup');

        if (rebalanceBtn) {
            rebalanceBtn.addEventListener('click', () => triggerMaintenanceTask('rebalance', rebalanceBtn));
        }
        if (cleanupBtn) {
            cleanupBtn.addEventListener('click', () => triggerMaintenanceTask('cleanup', cleanupBtn));
        }
        if (dbCleanupBtn) {
            dbCleanupBtn.addEventListener('click', () => triggerMaintenanceTask('dbcleanup', dbCleanupBtn));
        }
    }

    async function triggerMaintenanceTask(type, btn) {
        btn.disabled = true;
        const container = document.getElementById(`maint-${type}-progress-container`);
        const bar = document.getElementById(`maint-${type}-progress-bar`);
        const statusText = document.getElementById(`maint-${type}-status`);
        const percentText = document.getElementById(`maint-${type}-percent`);

        if (container) container.style.display = 'block';
        if (bar) bar.style.width = '0%';
        if (statusText) statusText.textContent = 'Submitting...';
        if (percentText) percentText.textContent = '0%';

        try {
            const res = await fetch(`/api/maintenance/${type}`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': 'Bearer ' + getStoredToken()
                }
            });
            if (res.ok) {
                const data = await res.json();
                const taskId = data.task_id;
                showToast('Maintenance Task', `Task submitted successfully.`, 'info');
                pollMaintenanceTask(taskId, type, btn);
            } else {
                const errData = await res.json().catch(() => ({}));
                showToast('Error', errData.error || 'Failed to submit maintenance task', 'error');
                btn.disabled = false;
                if (container) container.style.display = 'none';
            }
        } catch (err) {
            showToast('Error', 'Network error submitting maintenance task', 'error');
            btn.disabled = false;
            if (container) container.style.display = 'none';
        }
    }

    function pollMaintenanceTask(taskId, type, btn) {
        const bar = document.getElementById(`maint-${type}-progress-bar`);
        const statusText = document.getElementById(`maint-${type}-status`);
        const percentText = document.getElementById(`maint-${type}-percent`);

        let progressVal = 0;
        let pollCount = 0;

        const interval = setInterval(async () => {
            pollCount++;
            try {
                const res = await fetch('/api/catalyst/tasks', {
                    headers: {
                        'Authorization': 'Bearer ' + getStoredToken()
                    }
                });
                if (res.ok) {
                    const data = await res.json();
                    const tasks = data.tasks || [];
                    const task = tasks.find(t => t.task_id === taskId);
                    
                    if (task) {
                        const status = task.status;
                        const progress = task.progress !== undefined ? task.progress : 0;
                        const errorMsg = task.error_msg || '';

                        if (status === 'completed') {
                            clearInterval(interval);
                            if (bar) bar.style.width = '100%';
                            if (percentText) percentText.textContent = '100%';
                            if (statusText) statusText.textContent = 'Completed';
                            showToast('Maintenance Task', `Task '${type}' completed successfully`, 'success');
                            btn.disabled = false;
                        } else if (status === 'failed') {
                            clearInterval(interval);
                            if (statusText) statusText.textContent = 'Failed';
                            showToast('Maintenance Task Failed', errorMsg || `Task '${type}' failed`, 'error');
                            btn.disabled = false;
                        } else {
                            if (progress > 0) {
                                progressVal = progress;
                            } else {
                                if (progressVal < 95) {
                                    progressVal += Math.min(5, 95 - progressVal);
                                }
                            }
                            if (bar) bar.style.width = `${progressVal}%`;
                            if (percentText) percentText.textContent = `${progressVal}%`;
                            if (statusText) statusText.textContent = status.charAt(0).toUpperCase() + status.slice(1);
                        }
                    } else {
                        if (pollCount > 10) {
                            clearInterval(interval);
                            if (statusText) statusText.textContent = 'Task lost';
                            btn.disabled = false;
                        }
                    }
                }
            } catch (err) {
                console.error("Error polling maintenance task:", err);
            }
        }, 2000);
    }

    if (document.getElementById('btn-maint-rebalance')) {
        initMaintenanceOperations();
    }

    if (document.getElementById('networking-topology-container')) {
        initNetworkingPage();
    }

    // ----------------------------------------------------
    // Urbosa & Gatoway Virtual Networking Page Logic
    // ----------------------------------------------------
    function initNetworksPage() {
        initNetworkingPage();
    }

    async function initNetworkingPage() {
        setupNetworkFormListeners();
        await refreshNetworksData();
    }

    function setupNetworkFormListeners() {
        // Create segment form (standalone page)
        const createSegmentForm = document.getElementById('create-segment-form');
        if (createSegmentForm && !createSegmentForm.dataset.initialized) {
            createSegmentForm.dataset.initialized = 'true';
            
            createSegmentForm.addEventListener('submit', async (e) => {
                e.preventDefault();
                const name = document.getElementById('segment-name').value;
                const vlanInput = document.getElementById('segment-vlan-id');
                const vlanId = vlanInput ? parseInt(vlanInput.value) : null;
                
                await handleCreateNetwork(name, 'vlan', vlanId);
                createSegmentForm.reset();
            });
        }
        
        // Create network form (Settings page tab)
        const createNetworkForm = document.getElementById('create-network-form');
        if (createNetworkForm && !createNetworkForm.dataset.initialized) {
            createNetworkForm.dataset.initialized = 'true';
            
            const netType = document.getElementById('net-type-input');
            const netVlanGroup = document.getElementById('net-vlan-group');
            const netVlanInput = document.getElementById('net-vlan-input');
            
            if (netType && netVlanGroup) {
                // Initialize default state
                if (netType.value === 'vlan') {
                    netVlanGroup.style.display = 'block';
                    if (netVlanInput) netVlanInput.required = true;
                } else {
                    netVlanGroup.style.display = 'none';
                    if (netVlanInput) netVlanInput.required = false;
                }

                netType.addEventListener('change', () => {
                    if (netType.value === 'vlan') {
                        netVlanGroup.style.display = 'block';
                        if (netVlanInput) netVlanInput.required = true;
                    } else {
                        netVlanGroup.style.display = 'none';
                        if (netVlanInput) {
                            netVlanInput.required = false;
                            netVlanInput.value = '';
                        }
                    }
                });
            }
            
            createNetworkForm.addEventListener('submit', async (e) => {
                e.preventDefault();
                const name = document.getElementById('net-name-input').value;
                const type = netType ? netType.value : 'vlan';
                const vlanId = (type === 'vlan' && netVlanInput) ? parseInt(netVlanInput.value) : null;
                
                await handleCreateNetwork(name, type, vlanId);
                createNetworkForm.reset();
                if (netType) netType.value = 'vlan';
                if (netVlanGroup) netVlanGroup.style.display = 'block';
                if (netVlanInput) netVlanInput.required = true;
            });
        }
    }

    async function handleCreateNetwork(name, type, vlanId) {
        const payload = { name, type };
        if (type === 'vlan') {
            payload.vlan_id = vlanId;
        }
        
        try {
            const res = await fetch(`${state.apiHost}/api/networks/create`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await res.json();
            if (res.ok) {
                showToast('Success', data.message || `Network segment '${name}' created.`, 'success');
                await refreshNetworksData();
                loadNetworkSegmentsForDropdowns(['vm-network', 'edit-vm-network']);
            } else {
                showToast('Error', data.error || 'Failed to create network segment.', 'error');
            }
        } catch (err) {
            console.error('Network creation error:', err);
            showToast('Error', 'Network connection error creating segment.', 'error');
        }
    }

    async function handleDeleteNetwork(netId, netName) {
        if (!confirm(`Are you sure you want to delete the network segment '${netName}'?`)) {
            return;
        }
        
        try {
            const res = await fetch(`${state.apiHost}/api/networks/delete`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ net_id: netId })
            });
            const data = await res.json();
            if (res.ok) {
                showToast('Success', data.message || `Network segment '${netName}' deleted.`, 'success');
                await refreshNetworksData();
                loadNetworkSegmentsForDropdowns(['vm-network', 'edit-vm-network']);
            } else {
                showToast('Error', data.error || 'Failed to delete network segment.', 'error');
            }
        } catch (err) {
            console.error('Network deletion error:', err);
            showToast('Error', 'Network connection error deleting segment.', 'error');
        }
    }

    async function refreshNetworksData() {
        try {
            const res = await fetch(`${state.apiHost}/api/networks`);
            const data = await res.json();
            state.networks = data.networks || [];
            
            renderSettingsNetworksTable();
            renderStandaloneNetworksTable();
            renderTopology();
        } catch (err) {
            console.error('Error fetching networks:', err);
        }
    }

    function renderSettingsNetworksTable() {
        const tbody = document.getElementById('networks-tbody');
        if (!tbody) return;
        
        if (!state.networks || state.networks.length === 0) {
            tbody.innerHTML = `<tr><td colspan="5" style="text-align: center; color: var(--text-muted); padding: 20px;">No virtual networks defined.</td></tr>`;
            return;
        }
        
        tbody.innerHTML = '';
        state.networks.forEach(net => {
            const tr = document.createElement('tr');
            const isDefault = net.net_id === "7a68e0d6-11f8-4e89-9430-b3b44b8bc438";
            
            tr.innerHTML = `
                <td style="font-weight: 600; color: #fff;">${net.name}</td>
                <td style="font-family: 'Fira Code', monospace; font-size: 11px; color: var(--text-secondary);">${net.net_id}</td>
                <td>
                    <span class="badge ${net.type === 'vlan' ? 'badge-vlan' : 'badge-direct'}">
                        ${net.type === 'vlan' ? 'VLAN' : 'Direct'}
                    </span>
                </td>
                <td>${net.vlan_id !== null && net.vlan_id !== undefined ? net.vlan_id : 'N/A'}</td>
                <td style="text-align: right;">
                    ${isDefault ? 
                        `<span style="font-size: 11px; color: var(--text-muted); font-style: italic;">System Reserved</span>` : 
                        `<button class="btn btn-danger btn-delete-net" data-id="${net.net_id}" data-name="${net.name}" style="padding: 4px 8px; font-size: 11px; font-family: 'Space Grotesk', sans-serif;">Delete</button>`
                    }
                </td>
            `;
            
            const deleteBtn = tr.querySelector('.btn-delete-net');
            if (deleteBtn) {
                deleteBtn.addEventListener('click', () => {
                    handleDeleteNetwork(net.net_id, net.name);
                });
            }
            
            tbody.appendChild(tr);
        });
    }

    function renderStandaloneNetworksTable() {
        const tbody = document.getElementById('segments-table-body');
        if (!tbody) return;
        
        if (!state.networks || state.networks.length === 0) {
            tbody.innerHTML = `<tr><td colspan="5" class="table-loading" style="text-align: center;">No virtual network segments defined.</td></tr>`;
            return;
        }
        
        tbody.innerHTML = '';
        state.networks.forEach(net => {
            const tr = document.createElement('tr');
            const isDefault = net.net_id === "7a68e0d6-11f8-4e89-9430-b3b44b8bc438";
            
            tr.innerHTML = `
                <td style="font-weight: 600; color: #fff;">${net.name}</td>
                <td>
                    <span class="badge ${net.type === 'vlan' ? 'badge-vlan' : 'badge-direct'}">
                        ${net.type === 'vlan' ? 'VLAN' : 'Direct'}
                    </span>
                </td>
                <td>${net.vlan_id !== null && net.vlan_id !== undefined ? net.vlan_id : 'N/A'}</td>
                <td style="font-family: 'Fira Code', monospace; font-size: 12px; color: var(--text-secondary);">
                    ${net.type === 'vlan' ? `br-vlan-${net.vlan_id}` : 'br0'}
                </td>
                <td style="text-align: right;">
                    ${isDefault ? 
                        `<span style="font-size: 11px; color: var(--text-muted); font-style: italic;">System Reserved</span>` : 
                        `<button class="btn btn-secondary btn-edit-net" style="padding: 4px 8px; font-size: 11px; font-family: 'Space Grotesk', sans-serif; margin-right: 5px;">Edit</button><button class="btn btn-danger btn-delete-net" data-id="${net.net_id}" data-name="${net.name}" style="padding: 4px 8px; font-size: 11px; font-family: 'Space Grotesk', sans-serif;">Delete</button>`
                    }
                </td>
            `;
            
            const editBtn = tr.querySelector('.btn-edit-net');
            if (editBtn) {
                editBtn.addEventListener('click', () => {
                    handleEditNetworkModal(net.net_id, net.name, net.type, net.vlan_id);
                });
            }
            
            const deleteBtn = tr.querySelector('.btn-delete-net');
            if (deleteBtn) {
                deleteBtn.addEventListener('click', () => {
                    handleDeleteNetwork(net.net_id, net.name);
                });
            }
            
            tbody.appendChild(tr);
        });
    }

    function handleEditNetworkModal(netId, netName, netType, vlanId) {
        const existing = document.getElementById('edit-network-modal');
        if (existing) existing.remove();

        const modalDiv = document.createElement('div');
        modalDiv.id = 'edit-network-modal';
        modalDiv.className = 'modal-backdrop';
        modalDiv.style.display = 'flex';
        modalDiv.style.alignItems = 'center';
        modalDiv.style.justifyContent = 'center';
        modalDiv.style.zIndex = '2000';

        modalDiv.innerHTML = `
            <div class="modal-container" style="max-width: 500px; background: var(--bg-card); backdrop-filter: blur(20px); border: 1px solid var(--border-color); border-radius: 12px; padding: 20px; box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);">
                <div class="modal-header" style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; border-bottom: 1px solid var(--border-color); padding-bottom: 10px;">
                    <h3 style="font-family: 'Space Grotesk', sans-serif; font-size: 18px; font-weight: 600; color: #fff; margin: 0;">Edit Network Segment</h3>
                    <button id="btn-close-edit-network" style="background: none; border: none; color: var(--text-primary); font-size: 24px; cursor: pointer; line-height: 1; padding: 0;">&times;</button>
                </div>
                <div class="modal-body">
                    <form id="edit-network-form" style="display: flex; flex-direction: column; gap: 15px;">
                        <div class="form-group">
                            <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Segment Name <span class="required" style="color: var(--color-primary);">*</span></label>
                            <input type="text" id="edit-segment-name" class="form-input" required value="${netName}" style="width: 100%; box-sizing: border-box;">
                        </div>
                        ${netType === 'vlan' ? `
                        <div class="form-group">
                            <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">VLAN ID (1 - 4094) <span class="required" style="color: var(--color-primary);">*</span></label>
                            <input type="number" id="edit-segment-vlan-id" class="form-input" min="1" max="4094" required value="${vlanId}" style="width: 100%; box-sizing: border-box;">
                        </div>
                        ` : ''}
                        <div style="display: flex; justify-content: flex-end; gap: 10px; margin-top: 15px;">
                            <button type="button" id="btn-cancel-edit-network" class="btn btn-secondary" style="padding: 8px 16px; font-family: 'Space Grotesk', sans-serif;">Cancel</button>
                            <button type="submit" class="btn btn-primary" style="padding: 8px 16px; font-family: 'Space Grotesk', sans-serif;">Save Changes</button>
                        </div>
                    </form>
                </div>
            </div>
        `;

        document.body.appendChild(modalDiv);

        const close = () => modalDiv.remove();
        document.getElementById('btn-close-edit-network').addEventListener('click', close);
        document.getElementById('btn-cancel-edit-network').addEventListener('click', close);

        document.getElementById('edit-network-form').addEventListener('submit', async (e) => {
            e.preventDefault();
            const newName = document.getElementById('edit-segment-name').value.trim();
            const newVlanId = netType === 'vlan' ? parseInt(document.getElementById('edit-segment-vlan-id').value) : null;

            try {
                const res = await fetch('/api/networks/update', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ net_id: netId, name: newName, vlan_id: newVlanId })
                });
                const data = await res.json();
                if (res.ok) {
                    showToast('Success', data.message || 'Network segment updated successfully.', 'success');
                    close();
                    if (typeof fetchStatus === 'function') {
                        await fetchStatus();
                    }
                } else {
                    showToast('Error', data.error || 'Failed to update network segment.', 'error');
                }
            } catch (err) {
                showToast('Error', 'Network request failed.', 'error');
            }
        });
    }

    function renderTopology() {
        const container = document.getElementById('networking-topology-container');
        if (!container) return;
        
        if (!state.networks || state.networks.length === 0) {
            container.innerHTML = `<div class="table-loading">No networks loaded.</div>`;
            return;
        }
        
        container.innerHTML = '';
        
        const grid = document.createElement('div');
        grid.className = 'topology-grid';
        
        const defaultNetId = "7a68e0d6-11f8-4e89-9430-b3b44b8bc438";
        
        const netGroups = {};
        state.networks.forEach(net => {
            netGroups[net.net_id] = [];
        });
        
        const unassignedVms = [];
        
        state.vms.forEach(vm => {
            let nids = [];
            const nid = vm.network_id;
            if (nid) {
                const trimmed = nid.trim();
                if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
                    try {
                        nids = JSON.parse(trimmed);
                    } catch (e) {
                        nids = [nid];
                    }
                } else {
                    nids = [nid];
                }
            } else {
                nids = [];
            }

            if (nids.length === 0) {
                unassignedVms.push({ vm, nicIndex: -1, nids });
            } else {
                nids.forEach((netId, nicIndex) => {
                    if (netGroups[netId]) {
                        netGroups[netId].push({ vm, nicIndex, nids });
                    } else {
                        unassignedVms.push({ vm, nicIndex, nids });
                    }
                });
            }
        });
        
        state.networks.forEach(net => {
            const itemsInNet = netGroups[net.net_id] || [];
            const col = document.createElement('div');
            col.className = `topology-column type-${net.type}`;
            
            const numPorts = Math.max(8, Math.ceil((itemsInNet.length + 2) / 4) * 4);
            const portLeds = [];
            for (let i = 0; i < numPorts; i++) {
                if (i < itemsInNet.length) {
                    const isRunning = itemsInNet[i].vm.state && itemsInNet[i].vm.state.toLowerCase() === 'running';
                    portLeds.push(`<span class="switch-port-led ${isRunning ? 'active' : 'idle'}" title="${itemsInNet[i].vm.name} (NIC #${itemsInNet[i].nicIndex + 1} - ${itemsInNet[i].vm.state})"></span>`);
                } else {
                    portLeds.push(`<span class="switch-port-led"></span>`);
                }
            }
            
            const typeLabel = net.type === 'vlan' ? `VLAN ${net.vlan_id}` : 'Direct / Flat';
            const isDefault = net.net_id === defaultNetId;
            
            col.innerHTML = `
                <div class="topology-switch">
                    <div class="switch-info">
                        <span class="switch-name">${net.name}</span>
                        <span class="switch-meta">${typeLabel}</span>
                    </div>
                    <div class="switch-panel">
                        <div class="switch-ports">
                            ${portLeds.join('')}
                        </div>
                    </div>
                    ${!isDefault ? `
                        <div class="switch-actions">
                            <button class="btn btn-danger btn-delete-switch" data-id="${net.net_id}" data-name="${net.name}" style="padding: 2px 6px; font-size: 10px; font-family: 'Space Grotesk', sans-serif;">
                                Delete Segment
                            </button>
                        </div>
                    ` : ''}
                </div>
                <div class="topology-cable-bridge">
                    <div class="cable-line ${itemsInNet.some(item => item.vm.state && item.vm.state.toLowerCase() === 'running') ? 'active' : ''}"></div>
                </div>
                <div class="topology-vms"></div>
            `;
            
            const delBtn = col.querySelector('.btn-delete-switch');
            if (delBtn) {
                delBtn.addEventListener('click', () => {
                    handleDeleteNetwork(net.net_id, net.name);
                });
            }
            
            const vmsContainer = col.querySelector('.topology-vms');
            if (itemsInNet.length === 0) {
                vmsContainer.innerHTML = `<div style="text-align: center; color: var(--text-muted); font-size: 11px; padding: 20px 10px;">No VMs connected</div>`;
            } else {
                itemsInNet.forEach(item => {
                    const vm = item.vm;
                    const nicIndex = item.nicIndex;
                    const nids = item.nids;
                    
                    const vmCard = document.createElement('div');
                    vmCard.className = 'topology-vm-card';
                    vmCard.setAttribute('data-vm-name', vm.name);
                    const isRunning = vm.state && vm.state.toLowerCase() === 'running';
                    
                    let selectOptions = '';
                    state.networks.forEach(n => {
                        selectOptions += `<option value="${n.net_id}" ${n.net_id === net.net_id ? 'selected' : ''}>${n.name}</option>`;
                    });
                    selectOptions += `<option value="__remove__" style="color: var(--color-danger);">Disconnect / Remove NIC</option>`;
                    
                    vmCard.innerHTML = `
                        <div class="vm-card-header">
                            <span class="vm-card-title">${vm.name} <span style="font-size: 10px; color: var(--color-primary); margin-left: 6px; font-weight: normal;">NIC #${nicIndex + 1}</span></span>
                            <span class="vm-card-status ${isRunning ? 'running' : 'stopped'}" title="${vm.state}"></span>
                        </div>
                        <div class="vm-card-details">
                            <span>Node: ${formatNodeName(vm.host_ip)}</span>
                            <span>CPU: ${vm.vcpus || vm.vcpu || 1} vCPUs | RAM: ${(vm.memory / 1024).toFixed(1)} GB</span>
                        </div>
                        <div class="vm-card-net-selector">
                            <label>Connected Segment</label>
                            <select class="topology-vm-select" data-vm="${vm.name}" data-nic-index="${nicIndex}">
                                ${selectOptions}
                            </select>
                        </div>
                        <div style="display: flex; gap: 8px; margin-top: 8px; justify-content: flex-end;">
                            <button class="btn btn-secondary btn-add-nic-direct" style="padding: 2px 6px; font-size: 10px; font-family: 'Space Grotesk', sans-serif;">
                                + Add NIC
                            </button>
                        </div>
                    `;
                    
                    const selectEl = vmCard.querySelector('.topology-vm-select');
                    selectEl.addEventListener('change', async (e) => {
                        const newNetId = e.target.value;
                        let updatedNids = [...nids];
                        if (newNetId === "__remove__") {
                            updatedNids.splice(nicIndex, 1);
                        } else {
                            updatedNids[nicIndex] = newNetId;
                        }
                        await handleReassignVmNetwork(vm.name, JSON.stringify(updatedNids));
                    });

                    const addNicBtn = vmCard.querySelector('.btn-add-nic-direct');
                    if (addNicBtn) {
                        addNicBtn.addEventListener('click', async () => {
                            let updatedNids = [...nids];
                            updatedNids.push(defaultNetId);
                            await handleReassignVmNetwork(vm.name, JSON.stringify(updatedNids));
                        });
                    }

                    // Hover synchronized highlight listeners
                    vmCard.addEventListener('mouseenter', () => {
                        document.querySelectorAll(`.topology-vm-card[data-vm-name="${vm.name}"]`).forEach(card => {
                            card.classList.add('highlighted');
                            const parentColumn = card.closest('.topology-column');
                            if (parentColumn) {
                                const cable = parentColumn.querySelector('.cable-line');
                                if (cable) cable.classList.add('cable-highlighted');
                            }
                        });
                    });
                    vmCard.addEventListener('mouseleave', () => {
                        document.querySelectorAll(`.topology-vm-card[data-vm-name="${vm.name}"]`).forEach(card => {
                            card.classList.remove('highlighted');
                            const parentColumn = card.closest('.topology-column');
                            if (parentColumn) {
                                const cable = parentColumn.querySelector('.cable-line');
                                if (cable) cable.classList.remove('cable-highlighted');
                            }
                        });
                    });
                    
                    vmsContainer.appendChild(vmCard);
                });
            }
            
            grid.appendChild(col);
        });
        
        if (unassignedVms.length > 0) {
            const col = document.createElement('div');
            col.className = 'topology-column type-unassigned';
            
            col.innerHTML = `
                <div class="topology-switch" style="background: linear-gradient(135deg, #475569, #334155);">
                    <div class="switch-info">
                        <span class="switch-name">Isolated VMs</span>
                        <span class="switch-meta">No Network Assigned</span>
                    </div>
                </div>
                <div class="topology-cable-bridge">
                    <div class="cable-line"></div>
                </div>
                <div class="topology-vms"></div>
            `;
            
            const vmsContainer = col.querySelector('.topology-vms');
            unassignedVms.forEach(item => {
                const vm = item.vm;
                const nicIndex = item.nicIndex;
                const nids = item.nids;
                
                const vmCard = document.createElement('div');
                vmCard.className = 'topology-vm-card';
                vmCard.setAttribute('data-vm-name', vm.name);
                const isRunning = vm.state && vm.state.toLowerCase() === 'running';
                
                let selectOptions = '<option value="" selected disabled>Select Segment...</option>';
                state.networks.forEach(n => {
                    selectOptions += `<option value="${n.net_id}">${n.name}</option>`;
                });
                
                const nicLabel = nicIndex >= 0 ? `NIC #${nicIndex + 1}` : 'New NIC';
                
                vmCard.innerHTML = `
                    <div class="vm-card-header">
                        <span class="vm-card-title">${vm.name} <span style="font-size: 10px; color: var(--text-muted); margin-left: 6px; font-weight: normal;">${nicLabel}</span></span>
                        <span class="vm-card-status ${isRunning ? 'running' : 'stopped'}" title="${vm.state}"></span>
                    </div>
                    <div class="vm-card-details">
                        <span>Node: ${formatNodeName(vm.host_ip)}</span>
                    </div>
                    <div class="vm-card-net-selector">
                        <label>Connect to Segment</label>
                        <select class="topology-vm-select" data-vm="${vm.name}">
                            ${selectOptions}
                        </select>
                    </div>
                    <div style="display: flex; gap: 8px; margin-top: 8px; justify-content: flex-end;">
                        <button class="btn btn-secondary btn-add-nic-direct" style="padding: 2px 6px; font-size: 10px; font-family: 'Space Grotesk', sans-serif;">
                            + Add NIC
                        </button>
                        ${nicIndex >= 0 ? `
                        <button class="btn btn-danger btn-remove-nic-direct" style="padding: 2px 6px; font-size: 10px; font-family: 'Space Grotesk', sans-serif;">
                            Remove NIC
                        </button>
                        ` : ''}
                    </div>
                `;
                
                const selectEl = vmCard.querySelector('.topology-vm-select');
                selectEl.addEventListener('change', async (e) => {
                    const newNetId = e.target.value;
                    if (newNetId && newNetId !== "") {
                        let updatedNids = [...nids];
                        if (nicIndex === -1) {
                            updatedNids = [newNetId];
                        } else {
                            updatedNids[nicIndex] = newNetId;
                        }
                        await handleReassignVmNetwork(vm.name, JSON.stringify(updatedNids));
                    }
                });

                const addNicBtn = vmCard.querySelector('.btn-add-nic-direct');
                if (addNicBtn) {
                    addNicBtn.addEventListener('click', async () => {
                        let updatedNids = [...nids];
                        updatedNids.push(defaultNetId);
                        await handleReassignVmNetwork(vm.name, JSON.stringify(updatedNids));
                    });
                }

                const removeNicBtn = vmCard.querySelector('.btn-remove-nic-direct');
                if (removeNicBtn) {
                    removeNicBtn.addEventListener('click', async () => {
                        let updatedNids = [...nids];
                        if (nicIndex >= 0) {
                            updatedNids.splice(nicIndex, 1);
                            await handleReassignVmNetwork(vm.name, JSON.stringify(updatedNids));
                        }
                    });
                }

                // Hover synchronized highlight listeners
                vmCard.addEventListener('mouseenter', () => {
                    document.querySelectorAll(`.topology-vm-card[data-vm-name="${vm.name}"]`).forEach(card => {
                        card.classList.add('highlighted');
                        const parentColumn = card.closest('.topology-column');
                        if (parentColumn) {
                            const cable = parentColumn.querySelector('.cable-line');
                            if (cable) cable.classList.add('cable-highlighted');
                        }
                    });
                });
                vmCard.addEventListener('mouseleave', () => {
                    document.querySelectorAll(`.topology-vm-card[data-vm-name="${vm.name}"]`).forEach(card => {
                        card.classList.remove('highlighted');
                        const parentColumn = card.closest('.topology-column');
                        if (parentColumn) {
                            const cable = parentColumn.querySelector('.cable-line');
                            if (cable) cable.classList.remove('cable-highlighted');
                        }
                    });
                });
                
                vmsContainer.appendChild(vmCard);
            });
            
            grid.appendChild(col);
        }
        
        container.appendChild(grid);
    }

    async function handleReassignVmNetwork(vmName, newNetId) {
        try {
            const res = await fetch(`${state.apiHost}/api/vms/update`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    name: vmName,
                    network_id: newNetId
                })
            });
            const data = await res.json();
            if (res.ok) {
                showToast('Success', `VM '${vmName}' successfully reassigned to segment.`, 'success');
                const vm = state.vms.find(v => v.name === vmName);
                if (vm) {
                    vm.network_id = newNetId;
                }
                renderTopology();
            } else {
                showToast('Error', data.error || 'Failed to update VM network segment.', 'error');
                renderTopology();
            }
        } catch (err) {
            console.error('VM network reassignment error:', err);
            showToast('Error', 'Network connection error reassigning VM.', 'error');
            renderTopology();
        }
    }

    window.triggerNetworkingPageUpdate = async function() {
        try {
            const res = await fetch(`${state.apiHost}/api/networks`);
            const data = await res.json();
            state.networks = data.networks || [];
            
            renderSettingsNetworksTable();
            renderStandaloneNetworksTable();
            renderTopology();
        } catch (err) {
            console.error('Error in triggerNetworkingPageUpdate:', err);
        }
    };

    // ----------------------------------------------------
    // Dedicated Tasks Dashboard Page
    // ----------------------------------------------------

    function initTasksPage() {
        const tbody = document.getElementById('tasks-log-tbody');
        const searchInput = document.getElementById('tasks-search-input');
        const cleanupBtn = document.getElementById('tasks-page-cleanup-btn');
        
        let allTasks = [];
        
        async function fetchAndRender() {
            try {
                const res = await fetch('/api/catalyst/tasks', {
                    headers: {
                        'Authorization': 'Bearer ' + getStoredToken()
                    }
                });
                if (res.ok) {
                    const data = await res.json();
                    allTasks = data.tasks || [];
                    render();
                }
            } catch (err) {
                console.error("Error fetching tasks for dashboard:", err);
            }
        }

        function render() {
            if (!tbody) return;
            const query = searchInput ? searchInput.value.toLowerCase().trim() : '';
            
            const filtered = allTasks.filter(task => {
                if (!query) return true;
                const taskId = (task.task_id || '').toLowerCase();
                const service = (task.service || '').toLowerCase();
                const action = (task.action || '').toLowerCase();
                const status = (task.status || '').toLowerCase();
                const error = (task.error_msg || '').toLowerCase();
                
                let payloadStr = '';
                if (task.payload) {
                    payloadStr = (typeof task.payload === 'string' ? task.payload : JSON.stringify(task.payload)).toLowerCase();
                }
                
                return taskId.includes(query) || 
                       service.includes(query) || 
                       action.includes(query) || 
                       status.includes(query) || 
                       error.includes(query) ||
                       payloadStr.includes(query);
            });

            if (filtered.length === 0) {
                tbody.innerHTML = `
                    <tr>
                        <td colspan="7" style="padding: 30px; text-align: center; color: var(--text-muted);">
                            No tasks found matching query.
                        </td>
                    </tr>
                `;
                return;
            }

            const hierarchicalList = buildTaskTree(filtered);
            tbody.innerHTML = hierarchicalList.map(task => {
                const id = task.task_id || '';
                const shortId = id.substring(0, 8) + '...';
                const progress = task.progress !== undefined ? task.progress : 0;
                
                let taskName = `${task.service || 'system'} - ${task.action || 'task'}`;
                if (task.payload) {
                    try {
                        const payloadObj = typeof task.payload === 'string' ? jsonParseSafe(task.payload) : task.payload;
                        if (payloadObj && payloadObj.vm_name) {
                            taskName = `VM '${payloadObj.vm_name}' - ${task.action}`;
                        } else if (payloadObj && payloadObj.job_name) {
                            taskName = `Job '${payloadObj.job_name}' - execute`;
                        } else if (payloadObj && payloadObj.hostname) {
                            if (task.action === 'host_maintenance_enter') {
                                taskName = `Host '${payloadObj.hostname}' - Enter Maintenance`;
                            } else if (task.action === 'host_maintenance_leave') {
                                taskName = `Host '${payloadObj.hostname}' - Leave Maintenance`;
                            } else {
                                taskName = `Host '${payloadObj.hostname}' - ${task.action}`;
                            }
                        }
                    } catch (e) {}
                }

                // Status Badge & Progress Bar styling
                let badgeStyle = 'background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.08); color: var(--text-muted);';
                let progressBarBg = 'linear-gradient(90deg, #7f8c8d, #bdc3c7)';
                if (task.status === 'processing') {
                    badgeStyle = 'background: rgba(33, 150, 243, 0.1); border: 1px solid rgba(33, 150, 243, 0.3); color: #2196F3;';
                    progressBarBg = 'linear-gradient(90deg, #2196F3, #00BCD4)';
                } else if (task.status === 'completed') {
                    badgeStyle = 'background: rgba(46, 204, 113, 0.1); border: 1px solid rgba(46, 204, 113, 0.3); color: #2ecc71;';
                    progressBarBg = 'linear-gradient(90deg, #2ecc71, #27ae60)';
                } else if (task.status === 'failed') {
                    badgeStyle = 'background: rgba(231, 76, 60, 0.1); border: 1px solid rgba(231, 76, 60, 0.3); color: #e74c3c;';
                    progressBarBg = 'linear-gradient(90deg, #e74c3c, #d35400)';
                }

                // Formatted timestamps
                const created = task.created_at ? formatTimestamp(task.created_at) : 'N/A';
                const updated = task.updated_at ? formatTimestamp(task.updated_at) : 'N/A';
                
                // Detail text
                let detail = task.error_msg || '';
                if (!detail && task.payload) {
                    detail = typeof task.payload === 'string' ? task.payload : JSON.stringify(task.payload);
                    if (detail.length > 120) detail = detail.substring(0, 120) + '...';
                }

                const indent = (task.depth || 0) * 24;
                const rowBg = (task.depth || 0) > 0 ? 'background: rgba(255, 255, 255, 0.01);' : '';
                const rowBorderLeft = (task.depth || 0) > 0 ? `border-left: 3px solid var(--border-glow);` : '';
                const subtaskPrefix = (task.depth || 0) > 0 ? `<span style="color: var(--text-muted); margin-right: 8px; font-family: monospace; font-size: 13px;">↳</span> ` : '';

                return `
                    <tr style="border-bottom: 1px solid rgba(255,255,255,0.03); ${rowBg} ${rowBorderLeft}">
                        <td style="padding: 12px 16px; vertical-align: middle;">
                            <div style="display: flex; align-items: center; gap: 8px;">
                                <span style="font-family: 'Fira Code', monospace; font-size: 11px; background: rgba(255, 255, 255, 0.04); border: 1px solid rgba(255, 255, 255, 0.08); padding: 3px 6px; border-radius: 4px; color: var(--text-secondary);" title="${id}">
                                    ${id.substring(0, 8)}
                                </span>
                                <button class="btn-copy-id" data-id="${id}" style="background: none; border: none; padding: 2px; cursor: pointer; color: var(--text-muted); display: inline-flex; align-items: center; justify-content: center; transition: color 0.2s;" title="Copy ID">
                                    <svg viewBox="0 0 24 24" style="width: 12px; height: 12px; fill: currentColor;"><path d="M16 1H4c-1.1 0-2 .9-2 2v14h2V3h12V1zm3 4H8c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h11c1.1 0 2-.9 2-2V7c0-1.1-.9-2-2-2zm0 16H8V7h11v14z"/></svg>
                                </button>
                            </div>
                        </td>
                        <td style="padding: 12px 16px 12px ${16 + indent}px; font-weight: 500; color: #fff; vertical-align: middle; font-family: 'Space Grotesk', sans-serif; font-size: 13px;">${subtaskPrefix}${taskName}</td>
                        <td style="padding: 12px 16px; vertical-align: middle;">
                            <span style="display: inline-flex; align-items: center; gap: 6px; padding: 3px 8px; border-radius: 12px; font-size: 10px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; ${badgeStyle}">
                                <span style="width: 6px; height: 6px; border-radius: 50%; background-color: currentColor; display: inline-block;"></span>
                                ${task.status}
                            </span>
                        </td>
                        <td style="padding: 12px 16px; vertical-align: middle;">
                            <div style="display: flex; align-items: center; gap: 8px; width: 100%; min-width: 120px;">
                                <div style="flex-grow: 1; height: 6px; background: rgba(255,255,255,0.03); border-radius: 3px; overflow: hidden; border: 1px solid rgba(255,255,255,0.05);">
                                    <div style="width: ${progress}%; height: 100%; background: ${progressBarBg}; border-radius: 3px; transition: width 0.4s ease;"></div>
                                </div>
                                <span style="font-weight: 600; min-width: 32px; text-align: right; font-family: 'Space Grotesk', sans-serif; color: ${task.status === 'processing' ? '#2196F3' : 'var(--text-secondary)'};">${progress}%</span>
                            </div>
                        </td>
                        <td style="padding: 12px 16px; color: var(--text-muted); vertical-align: middle;">${created}</td>
                        <td style="padding: 12px 16px; color: var(--text-muted); vertical-align: middle;">${updated}</td>
                        <td style="padding: 12px 16px; vertical-align: middle; max-width: 300px;">
                            <div style="font-family: 'Fira Code', monospace; font-size: 11px; color: ${task.status === 'failed' ? '#ff6b6b' : 'var(--text-muted)'}; white-space: normal; word-break: break-word; line-height: 1.4; max-height: 50px; overflow-y: auto; scrollbar-width: none;" title="${task.error_msg || detail}">
                                ${task.error_msg || detail}
                            </div>
                        </td>
                    </tr>
                `;
            }).join('');
        }

        function formatTimestamp(ts) {
            try {
                const d = new Date(ts);
                if (isNaN(d.getTime())) return ts;
                return d.toLocaleString([], { hour12: false });
            } catch (e) {
                return ts;
            }
        }

        if (searchInput) {
            searchInput.addEventListener('input', () => render());
        }

        if (tbody) {
            tbody.addEventListener('click', (e) => {
                const copyBtn = e.target.closest('.btn-copy-id');
                if (copyBtn) {
                    const id = copyBtn.getAttribute('data-id');
                    navigator.clipboard.writeText(id).then(() => {
                        showToast('Copied', 'Task ID copied to clipboard', 'info');
                    }).catch(err => {
                        console.error('Failed to copy text: ', err);
                    });
                }
            });
        }

        if (cleanupBtn) {
            cleanupBtn.addEventListener('click', async () => {
                cleanupBtn.disabled = true;
                try {
                    const response = await fetch('/api/catalyst/tasks/cleanup', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'Authorization': 'Bearer ' + getStoredToken()
                        }
                    });
                    if (response.ok) {
                        showToast('Clean Up', 'Completed task workflows history cleaned successfully', 'info');
                        await fetchAndRender();
                    } else {
                        showToast('Error', 'Failed to clean tasks history', 'error');
                    }
                } catch (err) {
                    console.error("Cleanup tasks error:", err);
                    showToast('Error', 'Network error cleaning tasks', 'error');
                } finally {
                    cleanupBtn.disabled = false;
                }
            });
        }

        fetchAndRender();
        const pollInterval = setInterval(fetchAndRender, 3000);

        window.addEventListener('beforeunload', () => {
            clearInterval(pollInterval);
        });
    }

    if (document.getElementById('tasks-log-table')) {
        initTasksPage();
    }

    function loadSettings() {
        const dnsServersInput = document.getElementById('dns-servers-input');
        const dnsSearchInput = document.getElementById('dns-search-input');
        const dnsMtuInput = document.getElementById('dns-mtu-input');
        const urbosaEnabledInput = document.getElementById('urbosa-enabled-input');
        const ntpServersInput = document.getElementById('ntp-servers-input');
        const timezoneInput = document.getElementById('timezone-input');
        const clusterNameInput = document.getElementById('cluster-name-input');
        const clusterRegionInput = document.getElementById('cluster-region-input');
        const clusterSubnetInput = document.getElementById('cluster-subnet-input');
        const clusterIdInput = document.getElementById('cluster-id-input');
        const clusterVipInput = document.getElementById('cluster-vip-input');
        const replicationFactorInput = document.getElementById('replication-factor-input');
        const scrubIntervalInput = document.getElementById('scrub-interval-input');
        const passwordPolicyInput = document.getElementById('password-policy-input');
        const sessionTimeoutInput = document.getElementById('session-timeout-input');
        const rateLimitInput = document.getElementById('rate-limit-input');

        fetch(`${state.apiHost}/api/settings`)
        .then(res => res.json())
        .then(data => {
            if (dnsServersInput) dnsServersInput.value = data.dns_servers || '';
            if (dnsSearchInput) dnsSearchInput.value = data.dns_search_domains || '';
            if (dnsMtuInput) dnsMtuInput.value = data.dns_mtu || '1500';
            if (urbosaEnabledInput) urbosaEnabledInput.value = data.urbosa_enabled || 'false';
            if (ntpServersInput) ntpServersInput.value = data.ntp_servers || '';
            if (timezoneInput) timezoneInput.value = data.timezone || 'UTC';
            if (clusterNameInput) clusterNameInput.value = data.cluster_name || 'hci-01';
            if (clusterRegionInput) clusterRegionInput.value = data.cluster_region || 'dc-1';
            if (clusterSubnetInput) clusterSubnetInput.value = data.cluster_subnet || '';
            if (clusterIdInput) clusterIdInput.value = data.cluster_id || '';
            if (clusterVipInput) clusterVipInput.value = data.vip || '';
            if (replicationFactorInput) replicationFactorInput.value = data.replication_factor || '3';
            if (scrubIntervalInput) scrubIntervalInput.value = data.scrub_interval || 'weekly';
            if (passwordPolicyInput) passwordPolicyInput.value = data.password_policy || 'disabled';
            if (sessionTimeoutInput) sessionTimeoutInput.value = data.session_timeout || '30';
            if (rateLimitInput) rateLimitInput.value = data.rate_limit || '100';

            // Calculate FTT
            const fttLevelDisplay = document.getElementById('ftt-level-display');
            if (fttLevelDisplay) {
                const rf = parseInt(data.replication_factor || '3', 10);
                const ftt = Math.floor((rf - 1) / 2);
                fttLevelDisplay.textContent = `FTT-${ftt} (${ftt}-Node Failure Tolerant)`;
            }

            const brandDisplay = document.getElementById('cluster-name-brand-display');
            const headerDisplay = document.getElementById('cluster-name-display');
            if (brandDisplay) brandDisplay.textContent = data.cluster_name || 'hci-01';
            if (headerDisplay) headerDisplay.textContent = data.cluster_name || 'hci-01';
        })
        .catch(err => console.error("Error loading settings:", err));
    }

    if (document.getElementById('dns-servers-input')) {
        loadSettings();
    }

    async function updateSettings(payload, btn) {
        if (btn) {
            btn.disabled = true;
            btn.textContent = 'Saving...';
        }
        try {
            const res = await fetch(`${state.apiHost}/api/settings/update`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            if (res.ok) {
                const data = await res.json().catch(() => ({}));
                showToast('Settings Saved', 'Cluster configurations updated and propagating', 'success');
                loadSettings();
                if (data.task_id && typeof window.updateTasksList === 'function') {
                    window.updateTasksList();
                }
            } else {
                const errData = await res.json().catch(() => ({}));
                showToast('Error', errData.error || 'Failed to update settings', 'error');
            }
        } catch (err) {
            showToast('Error', 'Network error updating settings', 'error');
        } finally {
            if (btn) {
                btn.disabled = false;
                btn.textContent = 'Save Settings';
            }
        }
    }

    async function populateInterfaceDropdown(selectId, selectedValue = null, ipInputId = null, gwInputId = null) {
        const selectEl = document.getElementById(selectId);
        if (!selectEl) return;
        
        try {
            const res = await fetch(`${state.apiHost}/api/host/interfaces`);
            if (res.ok) {
                const data = await res.json();
                const interfaces = data.interfaces || [];
                selectEl.innerHTML = '';
                
                let selectVal = selectedValue || data.default_interface;
                
                interfaces.forEach(iface => {
                    const option = document.createElement('option');
                    option.value = iface;
                    option.textContent = iface;
                    if (selectVal && iface === selectVal) {
                        option.selected = true;
                    }
                    selectEl.appendChild(option);
                });
                
                if (ipInputId) {
                    const ipInput = document.getElementById(ipInputId);
                    if (ipInput && !ipInput.value && data.suggested_ip) {
                        ipInput.value = data.suggested_ip;
                    }
                }
                if (gwInputId) {
                    const gwInput = document.getElementById(gwInputId);
                    if (gwInput && !gwInput.value && data.default_gateway) {
                        gwInput.value = data.default_gateway;
                    }
                }
            }
        } catch (err) {
            console.error(`Error populating interface dropdown ${selectId}:`, err);
        }
    }

    function showDefaultT0Modal(settingsPayload) {
        const existing = document.getElementById('default-t0-prompt-modal');
        if (existing) existing.remove();

        const modalDiv = document.createElement('div');
        modalDiv.id = 'default-t0-prompt-modal';
        modalDiv.className = 'modal-backdrop';
        modalDiv.style.display = 'flex';
        modalDiv.style.alignItems = 'center';
        modalDiv.style.justifyContent = 'center';
        modalDiv.style.zIndex = '2000';

        modalDiv.innerHTML = `
            <div class="modal-container" style="width: 500px; max-width: 95vw; background: var(--bg-card); backdrop-filter: blur(20px); border: 1px solid var(--border-color); border-radius: 12px; padding: 20px; box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);">
                <div class="modal-header" style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; border-bottom: 1px solid var(--border-color); padding-bottom: 10px;">
                    <h3 style="font-family: 'Space Grotesk', sans-serif; font-size: 16px; font-weight: 600; color: var(--text-primary); margin: 0;">Configure Default Tier-0 SDN Gateway</h3>
                </div>
                <div class="modal-body">
                    <p style="font-size: 12px; color: var(--text-secondary); margin-bottom: 15px;">
                        Enabling Software Defined Networking requires configuring an active Tier-0 Edge Gateway to connect logical virtual networks to the physical uplink. Please specify the network details:
                    </p>
                    <form id="default-t0-form" style="display: flex; flex-direction: column; gap: 15px;">
                        <div class="form-group">
                            <label style="font-family: 'Space Grotesk', sans-serif; font-size: 12px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Gateway Name</label>
                            <input type="text" id="def-t0-name" required value="T0-Default-Edge" class="form-input" style="width: 100%; box-sizing: border-box;">
                        </div>
                        <div class="form-group">
                            <label style="font-family: 'Space Grotesk', sans-serif; font-size: 12px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Uplink Interface <span style="color: var(--color-primary);">*</span></label>
                            <select id="def-t0-interface" class="form-input" style="width: 100%; box-sizing: border-box;">
                                <option value="ens192" selected>ens192 (ESXi Default)</option>
                                <option value="ens3">ens3 (KVM/Proxmox Default)</option>
                                <option value="ens33">ens33 (VMware Workstation Default)</option>
                                <option value="eth0">eth0 (Standard Linux)</option>
                                <option value="eno1">eno1 (Bare Metal Default)</option>
                            </select>
                        </div>
                        <div class="form-group">
                            <label style="font-family: 'Space Grotesk', sans-serif; font-size: 12px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Uplink External IP / CIDR <span style="color: var(--color-primary);">*</span></label>
                            <input type="text" id="def-t0-ip" required placeholder="e.g. 10.10.102.250/24" class="form-input" style="width: 100%; box-sizing: border-box;">
                            <span class="input-hint" style="font-size: 9px; color: var(--text-muted); display: block; margin-top: 2px;">Assign a unique physical IP for VM traffic out-of-band masquerade routing.</span>
                        </div>
                        <div class="form-group">
                            <label style="font-family: 'Space Grotesk', sans-serif; font-size: 12px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Upstream Gateway IP <span style="color: var(--color-primary);">*</span></label>
                            <input type="text" id="def-t0-gateway" required placeholder="e.g. 10.10.102.1" class="form-input" style="width: 100%; box-sizing: border-box;">
                        </div>
                        <div style="display: flex; justify-content: flex-end; gap: 10px; margin-top: 15px; border-top: 1px solid var(--border-color); padding-top: 15px;">
                            <button type="button" id="btn-cancel-def-t0" class="btn btn-secondary" style="padding: 8px 16px; font-family: 'Space Grotesk', sans-serif; font-size: 13px;">Cancel</button>
                            <button type="submit" id="btn-submit-def-t0" class="btn btn-primary" style="padding: 8px 16px; font-family: 'Space Grotesk', sans-serif; font-size: 13px;">Configure & Enable</button>
                        </div>
                    </form>
                </div>
            </div>
        `;

        document.body.appendChild(modalDiv);
        populateInterfaceDropdown('def-t0-interface', null, 'def-t0-ip', 'def-t0-gateway');

        document.getElementById('btn-cancel-def-t0').addEventListener('click', () => {
            modalDiv.remove();
        });

        document.getElementById('default-t0-form').addEventListener('submit', async (e) => {
            e.preventDefault();
            const btnSubmit = document.getElementById('btn-submit-def-t0');
            btnSubmit.disabled = true;
            btnSubmit.textContent = 'Deploying topology...';

            try {
                // 1. Save settings
                const resSettings = await fetch(`${state.apiHost}/api/settings/update`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(settingsPayload)
                });
                if (!resSettings.ok) {
                    const errData = await resSettings.json().catch(() => ({}));
                    throw new Error(errData.error || 'Failed to enable Urbosa settings');
                }

                // 2. Create T0 gateway
                const t0Name = document.getElementById('def-t0-name').value.trim();
                const t0Iface = document.getElementById('def-t0-interface').value;
                const t0Ip = document.getElementById('def-t0-ip').value.trim();
                const t0Gw = document.getElementById('def-t0-gateway').value.trim();

                const resT0 = await fetch(`${state.apiHost}/api/urbosa/t0/create`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        name: t0Name,
                        uplink_interface: t0Iface,
                        uplink_ip: t0Ip,
                        gateway_ip: t0Gw,
                        nat_rules: { snat: true }
                    })
                });
                if (!resT0.ok) {
                    const errData = await resT0.json().catch(() => ({}));
                    throw new Error(errData.error || 'Failed to deploy default Tier-0 Gateway');
                }
                const t0Data = await resT0.json();
                const t0Id = t0Data.router_id;

                // 3. Create T1 router linked to T0
                const resT1 = await fetch(`${state.apiHost}/api/urbosa/t1/create`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        name: 'T1-Default-Router',
                        t0_link_id: t0Id,
                        dhcp_enabled: true
                    })
                });
                if (!resT1.ok) {
                    const errData = await resT1.json().catch(() => ({}));
                    throw new Error(errData.error || 'Failed to deploy default Tier-1 Router');
                }
                const t1Data = await resT1.json();
                const t1Id = t1Data.router_id;

                // 4. Create default Segment linked to T1
                const resSeg = await fetch(`${state.apiHost}/api/urbosa/segments/create`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        name: 'Default-Overlay-Segment',
                        vni: 10001,
                        t1_link_id: t1Id,
                        subnet_cidr: '10.0.1.0/24',
                        gateway_ip: '10.0.1.1',
                        dhcp_enabled: true,
                        dhcp_start: '10.0.1.100',
                        dhcp_end: '10.0.1.250'
                    })
                });
                if (!resSeg.ok) {
                    const errData = await resSeg.json().catch(() => ({}));
                    throw new Error(errData.error || 'Failed to deploy default Segment');
                }

                showToast('SDN Enabled', 'Urbosa enabled and default network topology deployed successfully.', 'success');
                modalDiv.remove();
                loadSettings();
                if (typeof window.updateTasksList === 'function') {
                    window.updateTasksList();
                }
            } catch (err) {
                showToast('SDN Deployment Error', err.message || err, 'error');
                btnSubmit.disabled = false;
                btnSubmit.textContent = 'Configure & Enable';
            }
        });
    }

    const dnsForm = document.getElementById('settings-dns-form');
    if (dnsForm) {
        dnsForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            const payload = {
                dns_servers: document.getElementById('dns-servers-input').value,
                dns_search_domains: document.getElementById('dns-search-input').value,
                dns_mtu: document.getElementById('dns-mtu-input') ? document.getElementById('dns-mtu-input').value : '1500',
                urbosa_enabled: document.getElementById('urbosa-enabled-input').value
            };
            
            if (payload.urbosa_enabled === 'true') {
                try {
                    const t0Res = await fetch(`${state.apiHost}/api/urbosa/t0`);
                    const t0Data = await t0Res.json();
                    const hasT0 = t0Data.routers && t0Data.routers.length > 0;
                    if (!hasT0) {
                        showDefaultT0Modal(payload);
                        return;
                    }
                } catch (err) {
                    console.error("Error checking T0 gateways status:", err);
                }
            }
            
            updateSettings(payload, document.getElementById('btn-save-dns'));
        });
    }

    const ntpForm = document.getElementById('settings-ntp-form');
    if (ntpForm) {
        ntpForm.addEventListener('submit', (e) => {
            e.preventDefault();
            const payload = {
                ntp_servers: document.getElementById('ntp-servers-input').value,
                timezone: document.getElementById('timezone-input').value
            };
            updateSettings(payload, document.getElementById('btn-save-ntp'));
        });
    }

    const clusterForm = document.getElementById('settings-cluster-form');
    if (clusterForm) {
        clusterForm.addEventListener('submit', (e) => {
            e.preventDefault();
            const payload = {
                cluster_name: document.getElementById('cluster-name-input').value,
                cluster_region: document.getElementById('cluster-region-input').value,
                cluster_subnet: document.getElementById('cluster-subnet-input').value,
                vip: document.getElementById('cluster-vip-input').value,
                replication_factor: document.getElementById('replication-factor-input').value,
                scrub_interval: document.getElementById('scrub-interval-input').value
            };
            updateSettings(payload, document.getElementById('btn-save-cluster'));
        });
    }

    const securityForm = document.getElementById('settings-security-form');
    if (securityForm) {
        securityForm.addEventListener('submit', (e) => {
            e.preventDefault();
            const payload = {
                password_policy: document.getElementById('password-policy-input').value,
                session_timeout: document.getElementById('session-timeout-input').value,
                rate_limit: document.getElementById('rate-limit-input').value
            };
            updateSettings(payload, document.getElementById('btn-save-security'));
        });
    }

    const sslForm = document.getElementById('settings-ssl-form');
    if (sslForm) {
        sslForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            const btn = document.getElementById('btn-save-ssl');
            btn.disabled = true;
            btn.textContent = 'Deploying...';
            
            const certVal = document.getElementById('ssl-cert-input').value;
            const keyVal = document.getElementById('ssl-key-input').value;
            
            try {
                const res = await fetch(`${state.apiHost}/api/settings/ssl/update`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ cert: certVal, key: keyVal })
                });
                
                if (res.ok) {
                    showToast('SSL Certificate Deployed', 'Spectrum will perform a brief restart to apply the SSL certificate.', 'success');
                    setTimeout(() => {
                        window.location.reload();
                    }, 5000);
                } else {
                    const data = await res.json().catch(() => ({}));
                    showToast('Failed to deploy SSL', data.error || 'Server error', 'error');
                }
            } catch (err) {
                showToast('Failed to deploy SSL', 'Could not connect to server', 'error');
            } finally {
                btn.disabled = false;
                btn.textContent = 'Deploy SSL Certificate';
            }
        });
    }

    function updateNodeOpsTable() {
        const tbody = document.getElementById('node-ops-table-body');
        if (!tbody) return;

        tbody.innerHTML = '';
        if (state.nodes.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="5" class="table-loading">No cluster nodes found.</td>
                </tr>
            `;
            return;
        }

        state.nodes.forEach(node => {
            const tr = document.createElement('tr');
            const isOnline = node.status === 'ONLINE';
            const inMaintenance = node.maintenance_mode === true;
            
            const maintActionHtml = inMaintenance ? `
                <button class="btn btn-secondary btn-maint-action" data-action="leave" data-ip="${node.ip}" data-name="${node.name}" style="height: 28px; font-size: 11px; padding: 0 10px; margin-right: 5px;">
                    Leave Maintenance
                </button>
            ` : `
                <button class="btn btn-secondary btn-maint-action" data-action="enter" data-ip="${node.ip}" data-name="${node.name}" ${!isOnline ? 'disabled' : ''} style="height: 28px; font-size: 11px; padding: 0 10px; margin-right: 5px;">
                    Enter Maintenance
                </button>
            `;

            tr.innerHTML = `
                <td><strong>${formatNodeName(node.name)}</strong></td>
                <td>${node.ip}</td>
                <td><span class="status-badge ${isOnline ? 'status-online' : 'status-offline'}">${node.status}</span></td>
                <td><span class="status-badge ${inMaintenance ? 'status-warning' : 'status-ok'}">${inMaintenance ? 'Yes' : 'No'}</span></td>
                <td>
                    ${maintActionHtml}
                    <button class="btn btn-secondary btn-reboot-node" data-ip="${node.ip}" data-name="${node.name}" ${!isOnline ? 'disabled' : ''} style="height: 28px; font-size: 11px; padding: 0 10px;">
                        Reboot Node
                    </button>
                </td>
            `;
            tbody.appendChild(tr);
        });

        // Add event listeners to maintenance action buttons
        tbody.querySelectorAll('.btn-maint-action').forEach(btn => {
            btn.addEventListener('click', async (e) => {
                const nodeName = btn.getAttribute('data-name');
                const action = btn.getAttribute('data-action');
                const actionVerb = action === 'enter' ? 'enter maintenance mode' : 'leave maintenance mode';
                if (!confirm(`Are you sure you want node '${nodeName}' to ${actionVerb}?`)) {
                    return;
                }

                btn.disabled = true;
                btn.textContent = action === 'enter' ? 'Entering...' : 'Leaving...';
                // showToast('Maintenance Transition', `Initiating maintenance ${action} for ${nodeName}`, 'info');

                try {
                    const res = await fetch(`${state.apiHost}/api/host/maintenance`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ hostname: nodeName, action: action })
                    });
                    
                    if (res.ok) {
                        fetchStatus();
                    } else {
                        const errData = await res.json().catch(() => ({}));
                        showToast('Transition Failed', errData.error || 'Request failed', 'error');
                        btn.disabled = false;
                        btn.textContent = action === 'enter' ? 'Enter Maintenance' : 'Leave Maintenance';
                    }
                } catch (err) {
                    showToast('Transition Failed', 'Connection error', 'error');
                    btn.disabled = false;
                    btn.textContent = action === 'enter' ? 'Enter Maintenance' : 'Leave Maintenance';
                }
            });
        });

        // Add event listeners to reboot buttons
        tbody.querySelectorAll('.btn-reboot-node').forEach(btn => {
            btn.addEventListener('click', async (e) => {
                const nodeName = btn.getAttribute('data-name');
                const nodeIp = btn.getAttribute('data-ip');
                if (!confirm(`Are you sure you want to safely reboot node '${nodeName}' (${nodeIp})?\nThis will enter maintenance mode, evacuate VMs, stop services, and reboot the node.`)) {
                    return;
                }

                btn.disabled = true;
                btn.textContent = 'Initiating...';
                // showToast('Reboot Initiated', `Safe reboot sequence triggered for ${nodeName}`, 'info');

                try {
                    const res = await fetch(`${state.apiHost}/api/host/reboot`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ hostname: nodeName })
                    });
                    
                    if (res.ok) {
                        fetchStatus();
                    } else {
                        const errData = await res.json().catch(() => ({}));
                        showToast('Reboot Failed', errData.error || 'Request failed', 'error');
                        btn.disabled = false;
                        btn.textContent = 'Reboot Node';
                    }
                } catch (err) {
                    showToast('Reboot Failed', 'Connection error', 'error');
                    btn.disabled = false;
                    btn.textContent = 'Reboot Node';
                }
            });
        });

        // Populate remove node select dropdown
        const removeSelect = document.getElementById('remove-node-select');
        if (removeSelect) {
            removeSelect.innerHTML = '<option value="">-- Select Node --</option>';
            state.nodes.forEach(node => {
                removeSelect.innerHTML += `<option value="${node.name}">${node.name} (${node.ip})</option>`;
            });
        }

        // Setup add cluster node click handler
        const btnAddNode = document.getElementById('btn-add-cluster-node');
        if (btnAddNode && !btnAddNode.getAttribute('data-bound')) {
            btnAddNode.setAttribute('data-bound', 'true');
            btnAddNode.addEventListener('click', async () => {
                const hnInput = document.getElementById('add-node-hostname');
                const ipInput = document.getElementById('add-node-ip');
                const hostname = hnInput ? hnInput.value.trim() : '';
                const ip = ipInput ? ipInput.value.trim() : '';

                if (!hostname || !ip) {
                    showToast('Validation Error', 'Hostname and IP address are required.', 'error');
                    return;
                }

                btnAddNode.disabled = true;
                btnAddNode.textContent = 'Adding...';

                try {
                    const token = getStoredToken();
                    const res = await fetch(`${state.apiHost}/api/cluster/nodes/add`, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'Authorization': token ? `Bearer ${token}` : ''
                        },
                        body: JSON.stringify({ hostname, ip })
                    });

                    if (res.ok) {
                        showToast('Node Added', `Node ${hostname} added to cluster successfully.`, 'success');
                        if (hnInput) hnInput.value = '';
                        if (ipInput) ipInput.value = '';
                        fetchStatus();
                    } else {
                        const errData = await res.json().catch(() => ({}));
                        showToast('Error', errData.error || 'Failed to add node to cluster.', 'error');
                    }
                } catch (err) {
                    showToast('Connection Error', 'Network error adding node.', 'error');
                } finally {
                    btnAddNode.disabled = false;
                    btnAddNode.textContent = 'Add Node to Cluster';
                }
            });
        }

        // Setup remove cluster node click handler
        const btnRemoveNode = document.getElementById('btn-remove-cluster-node');
        if (btnRemoveNode && !btnRemoveNode.getAttribute('data-bound')) {
            btnRemoveNode.setAttribute('data-bound', 'true');
            btnRemoveNode.addEventListener('click', async () => {
                const removeSelect = document.getElementById('remove-node-select');
                const hostname = removeSelect ? removeSelect.value : '';

                if (!hostname) {
                    showToast('Validation Error', 'Please select a node to remove.', 'error');
                    return;
                }

                if (!confirm(`Are you sure you want to remove node '${hostname}' from the cluster?\nThis action will update the cluster configuration.`)) {
                    return;
                }

                btnRemoveNode.disabled = true;
                btnRemoveNode.textContent = 'Removing...';

                try {
                    const token = getStoredToken();
                    const res = await fetch(`${state.apiHost}/api/cluster/nodes/remove`, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'Authorization': token ? `Bearer ${token}` : ''
                        },
                        body: JSON.stringify({ hostname })
                    });

                    if (res.ok) {
                        showToast('Node Removed', `Node ${hostname} removed successfully.`, 'success');
                        fetchStatus();
                    } else {
                        const errData = await res.json().catch(() => ({}));
                        showToast('Error', errData.error || 'Failed to remove node.', 'error');
                    }
                } catch (err) {
                    showToast('Connection Error', 'Network error removing node.', 'error');
                } finally {
                    btnRemoveNode.disabled = false;
                    btnRemoveNode.textContent = 'Remove Node from Cluster';
                }
            });
        }
    }

    function loadUsersList() {
        const tbody = document.getElementById('users-table-body');
        if (!tbody) return;

        fetch(`${state.apiHost}/api/users`)
        .then(res => res.json())
        .then(data => {
            const list = data.users || [];
            tbody.innerHTML = '';
            
            if (list.length === 0) {
                tbody.innerHTML = `
                    <tr>
                        <td colspan="4" class="table-loading">No users configured.</td>
                    </tr>
                `;
                return;
            }

            list.forEach(username => {
                const tr = document.createElement('tr');
                const isDefault = username === 'helios';
                const isSelf = username === state.currentUser;
                
                let deleteAction = '';
                if (isDefault) {
                    deleteAction = `<span style="font-size: 11px; color: var(--text-muted);">Default Account</span>`;
                } else if (isSelf) {
                    deleteAction = `<span style="font-size: 11px; color: var(--text-muted);">Active Session</span>`;
                } else {
                    deleteAction = `<button class="table-action-btn delete-user-btn" data-username="${username}" style="color: var(--color-danger); border-color: var(--color-danger);">Delete</button>`;
                }

                tr.innerHTML = `
                    <td><strong>${username}</strong></td>
                    <td><span class="status-indicator status-green">ACTIVE</span></td>
                    <td><span class="status-indicator status-green" style="background: rgba(16, 185, 129, 0.08); color: var(--color-success); border: 1px solid rgba(16, 185, 129, 0.2); padding: 2px 6px; border-radius: 4px; font-size: 10px;">REPLICATED</span></td>
                    <td style="text-align: right;">
                        <div class="vm-actions-cell" style="display:flex; justify-content:flex-end; align-items: center;">
                            <button class="table-action-btn change-pw-btn" data-username="${username}" style="margin-right: 8px;">Change Password</button>
                            ${deleteAction}
                        </div>
                    </td>
                `;
                tbody.appendChild(tr);
            });

            tbody.querySelectorAll('.delete-user-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    const username = btn.getAttribute('data-username');
                    if (confirm(`Are you sure you want to permanently delete user "${username}"?`)) {
                        btn.disabled = true;
                        fetch(`${state.apiHost}/api/users/delete`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ username: username })
                        })
                        .then(res => {
                            if (!res.ok) return res.json().then(e => { throw new Error(e.error || 'Failed to delete user') });
                            return res.json();
                        })
                        .then(() => {
                            showToast('User Deleted', `User "${username}" deleted successfully.`, 'success');
                            loadUsersList();
                        })
                        .catch(err => {
                            showToast('Deletion Failed', err.message, 'error');
                            btn.disabled = false;
                        });
                    }
                });
            });

            tbody.querySelectorAll('.change-pw-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    const username = btn.getAttribute('data-username');
                    const modal = document.getElementById('change-user-pw-modal');
                    const targetLabel = document.getElementById('change-pw-target-username');
                    if (modal && targetLabel) {
                        targetLabel.textContent = username;
                        document.getElementById('change-pw-new-input').value = '';
                        document.getElementById('change-pw-confirm-input').value = '';
                        modal.classList.add('open');
                    }
                });
            });
        })
        .catch(err => {
            tbody.innerHTML = `
                <tr>
                    <td colspan="4" class="table-loading" style="color: var(--color-danger);">Failed to query users: ${err.message}</td>
                </tr>
            `;
        });
    }

    const changeUserPwModal = document.getElementById('change-user-pw-modal');
    if (changeUserPwModal) {
        const btnClose = document.getElementById('btn-close-change-pw-modal');
        const btnCancel = document.getElementById('btn-cancel-change-pw');
        const form = document.getElementById('change-user-pw-form');

        const closeModal = () => {
            changeUserPwModal.classList.remove('open');
        };

        if (btnClose) btnClose.addEventListener('click', closeModal);
        if (btnCancel) btnCancel.addEventListener('click', closeModal);

        if (form) {
            form.addEventListener('submit', async (e) => {
                e.preventDefault();
                const username = document.getElementById('change-pw-target-username').textContent;
                const newPassword = document.getElementById('change-pw-new-input').value;
                const confirmPassword = document.getElementById('change-pw-confirm-input').value;
                const btnSave = document.getElementById('btn-save-change-pw');

                if (newPassword !== confirmPassword) {
                    showToast('Validation Error', 'New passwords do not match.', 'error');
                    return;
                }

                btnSave.disabled = true;
                btnSave.textContent = 'Saving...';

                try {
                    const res = await fetch(`${state.apiHost}/api/users/change-password`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            username: username,
                            password: newPassword
                        })
                    });

                    if (res.ok) {
                        showToast('Password Changed', `Password for user "${username}" has been updated.`, 'success');
                        closeModal();
                    } else {
                        const errData = await res.json().catch(() => ({}));
                        showToast('Change Failed', errData.error || 'Failed to change password.', 'error');
                    }
                } catch (err) {
                    showToast('Change Failed', err.message, 'error');
                } finally {
                    btnSave.disabled = false;
                    btnSave.textContent = 'Save Password';
                }
            });
        }
    }

    const createUserForm = document.getElementById('create-user-form');
    if (createUserForm) {
        createUserForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            const usernameInput = document.getElementById('new-username-input');
            const passwordInput = document.getElementById('new-user-password-input');
            const btn = document.getElementById('btn-create-user');

            btn.disabled = true;
            btn.textContent = 'Creating...';

            try {
                const res = await fetch(`${state.apiHost}/api/users/create`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        username: usernameInput.value,
                        password: passwordInput.value
                    })
                });

                if (res.status === 201) {
                    showToast('User Created', `Administrator account "${usernameInput.value}" registered.`, 'success');
                    usernameInput.value = '';
                    passwordInput.value = '';
                    loadUsersList();
                } else {
                    const errData = await res.json().catch(() => ({}));
                    showToast('Creation Failed', errData.error || 'Failed to register user', 'error');
                }
            } catch (err) {
                showToast('Error', 'Network error registering user', 'error');
            } finally {
                btn.disabled = false;
                btn.textContent = 'Create User';
            }
        });
    }


    // Change Password Form Handling (on settings.html)
    const passwordForm = document.getElementById('settings-password-form');
    if (passwordForm) {
        passwordForm.addEventListener('submit', async function(e) {
            e.preventDefault();
            const oldPassInput = document.getElementById('old-password-input');
            const newPassInput = document.getElementById('new-password-input');
            const confirmPassInput = document.getElementById('confirm-password-input');
            const changeBtn = document.getElementById('btn-change-password');

            if (newPassInput.value !== confirmPassInput.value) {
                showToast('Validation Error', 'New passwords do not match', 'error');
                return;
            }

            changeBtn.disabled = true;
            changeBtn.textContent = 'Updating...';

            try {
                const res = await fetch('/api/auth/change-password', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        old_password: oldPassInput.value,
                        new_password: newPassInput.value
                    })
                });
                if (res.ok) {
                    showToast('Success', 'Password changed successfully', 'success');
                    oldPassInput.value = '';
                    newPassInput.value = '';
                    confirmPassInput.value = '';
                } else {
                    const data = await res.json();
                    showToast('Error', data.error || 'Failed to change password', 'error');
                }
            } catch (err) {
                showToast('Error', 'Network error changing password', 'error');
            } finally {
                changeBtn.disabled = false;
                changeBtn.textContent = 'Change Password';
            }
        });
    }

    // ----------------------------------------------------
    // Dropdown Toggle Logic
    // ----------------------------------------------------
    if (dropdownBtn && dropdownContainer) {
        dropdownBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            dropdownContainer.classList.toggle('open');
        });
    }

    document.addEventListener('click', (e) => {
        if (dropdownContainer && !dropdownContainer.contains(e.target)) {
            dropdownContainer.classList.remove('open');
        }
    });

    // ----------------------------------------------------
    // Tasks Dropdown Toggle & Polling Logic (Nutanix Style)
    // ----------------------------------------------------
    const tasksDropdownBtn = document.getElementById('tasks-dropdown-btn');
    const tasksDropdownContainer = document.getElementById('tasks-dropdown-container');
    const tasksBadgeCount = document.getElementById('tasks-badge-count');
    const tasksMenuList = document.getElementById('tasks-menu-list');
    const tasksCleanupBtn = document.getElementById('tasks-cleanup-btn');

    if (tasksDropdownBtn && tasksDropdownContainer) {
        tasksDropdownBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            tasksDropdownContainer.classList.toggle('open');
            // If opening, fetch immediately
            if (tasksDropdownContainer.classList.contains('open')) {
                updateTasksList();
            }
        });
    }

    document.addEventListener('click', (e) => {
        if (tasksDropdownContainer && !tasksDropdownContainer.contains(e.target)) {
            tasksDropdownContainer.classList.remove('open');
        }
    });

    document.addEventListener('click', async (e) => {
        const btn = e.target.closest('#tasks-cleanup-btn');
        if (btn) {
            e.preventDefault();
            e.stopPropagation();
            if (btn.disabled) return;
            btn.disabled = true;
            const originalText = btn.textContent;
            btn.textContent = 'Cleaning...';
            try {
                const response = await fetch('/api/catalyst/tasks/cleanup', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': 'Bearer ' + getStoredToken()
                    }
                });
                if (response.ok) {
                    showToast('Clean Up', 'Tasks history cleaned up successfully', 'info');
                    sessionStorage.removeItem('helios_tasks_cache'); // Clear cache too
                    updateTasksList();
                } else {
                    showToast('Error', 'Failed to clean up tasks', 'error');
                }
            } catch (err) {
                console.error("Failed to clean up tasks:", err);
                showToast('Error', 'Network error cleaning tasks', 'error');
            } finally {
                btn.disabled = false;
                btn.textContent = originalText;
            }
        }
    });

    let knownTasks = null;
    let announcementTimeout = null;
    let announcementClearTimeout = null;

    function getTaskDisplayName(task) {
        let taskName = `${task.service || 'system'} - ${task.action || 'task'}`;
        if (task.payload) {
            try {
                const payloadObj = typeof task.payload === 'string' ? JSON.parse(task.payload) : task.payload;
                if (payloadObj && payloadObj.vm_name) {
                    taskName = `VM '${payloadObj.vm_name}' ${task.action}`;
                } else if (payloadObj && payloadObj.job_name) {
                    taskName = `Job '${payloadObj.job_name}'`;
                } else if (payloadObj && payloadObj.hostname) {
                    if (task.action === 'host_maintenance_enter') {
                        taskName = `Maint Enter: ${payloadObj.hostname}`;
                    } else if (task.action === 'host_maintenance_leave') {
                        taskName = `Maint Leave: ${payloadObj.hostname}`;
                    } else {
                        taskName = `Host '${payloadObj.hostname}' ${task.action}`;
                    }
                }
            } catch (e) {}
        }
        // Beautify task action/names
        taskName = taskName.replace(/_/g, ' ');
        return taskName;
    }

    function updateAnnouncementContent(message, isDual = false, subMessage = '') {
        const announcementEl = document.getElementById('tasks-trigger-announcement');
        if (!announcementEl) return;

        announcementEl.innerHTML = '';
        if (isDual) {
            announcementEl.classList.add('is-dual');
            const row1 = document.createElement('div');
            row1.className = 'announcement-row';
            row1.textContent = message;

            const row2 = document.createElement('div');
            row2.className = 'announcement-row subtask-row';
            row2.textContent = subMessage;

            announcementEl.appendChild(row1);
            announcementEl.appendChild(row2);
        } else {
            announcementEl.classList.remove('is-dual');
            const row = document.createElement('div');
            row.className = 'announcement-row';
            row.textContent = message;
            announcementEl.appendChild(row);
        }
    }

    function announceTaskEvent(message, keepActive = false, isDual = false, subMessage = '') {
        const announcementEl = document.getElementById('tasks-trigger-announcement');
        if (!announcementEl) return;

        // Clear any active timeouts
        if (announcementTimeout) clearTimeout(announcementTimeout);
        if (announcementClearTimeout) clearTimeout(announcementClearTimeout);

        // Populate content and expand
        updateAnnouncementContent(message, isDual, subMessage);
        announcementEl.classList.add('expanded');

        if (!keepActive) {
            // Shrink after 4 seconds
            announcementTimeout = setTimeout(() => {
                announcementEl.classList.remove('expanded');
                // Clear content after animation completes
                announcementClearTimeout = setTimeout(() => {
                    announcementEl.innerHTML = '';
                    announcementEl.classList.remove('is-dual');
                }, 500);
            }, 4000);
        }
    }

    function checkTaskTransitions(newTasksList) {
        if (!knownTasks) {
            // First load: just populate knownTasks
            knownTasks = {};
            newTasksList.forEach(t => {
                knownTasks[t.task_id] = { status: t.status, progress: t.progress, action: t.action, displayName: getTaskDisplayName(t) };
            });
        }

        const activeTasks = newTasksList.filter(t => t.status === 'pending' || t.status === 'processing');

        if (activeTasks.length > 0) {
            // Check if we have both an active parent task and an active subtask of that parent
            let parentTask = null;
            let subTask = null;

            activeTasks.forEach(t => {
                let parentId = null;
                if (t.payload) {
                    try {
                        const payloadObj = typeof t.payload === 'string' ? JSON.parse(t.payload) : t.payload;
                        if (payloadObj && payloadObj.parent_task_id) {
                            parentId = payloadObj.parent_task_id;
                        }
                    } catch (e) {}
                }

                if (parentId) {
                    // Check if parent is also in activeTasks
                    const parentActive = activeTasks.find(pt => pt.task_id === parentId);
                    if (parentActive) {
                        subTask = t;
                        parentTask = parentActive;
                    }
                }
            });

            if (parentTask && subTask) {
                const parentName = getTaskDisplayName(parentTask);
                const parentPrefix = parentTask.status === 'pending' ? 'Pending' : 'Running';
                const parentProgress = parentTask.progress !== undefined ? ` (${parentTask.progress}%)` : '';
                const parentMsg = `${parentPrefix}: ${parentName}${parentProgress}`;

                const subName = getTaskDisplayName(subTask);
                const subPrefix = subTask.status === 'pending' ? 'Pending' : 'Running';
                const subProgress = subTask.progress !== undefined ? ` (${subTask.progress}%)` : '';
                const subMsg = `↳ ${subPrefix}: ${subName}${subProgress}`;

                announceTaskEvent(parentMsg, true, true, subMsg);
            } else {
                // Otherwise show the most recent active task
                const actTask = activeTasks[0];
                const displayName = getTaskDisplayName(actTask);
                const prefix = actTask.status === 'pending' ? 'Pending' : 'Running';
                const progressStr = actTask.progress !== undefined ? ` (${actTask.progress}%)` : '';
                const msg = `${prefix}: ${displayName}${progressStr}`;
                announceTaskEvent(msg, true, false);
            }
        } else {
            // If no active tasks, see if a task just transitioned to completed/failed
            let finishedTask = null;
            newTasksList.forEach(t => {
                if (knownTasks && (t.task_id in knownTasks)) {
                    const prev = knownTasks[t.task_id];
                    if ((prev.status === 'pending' || prev.status === 'processing') && (t.status === 'completed' || t.status === 'failed')) {
                        let isSubtask = false;
                        if (t.payload) {
                            try {
                                const payloadObj = typeof t.payload === 'string' ? JSON.parse(t.payload) : t.payload;
                                if (payloadObj && payloadObj.parent_task_id) {
                                    isSubtask = true;
                                }
                            } catch (e) {}
                        }
                        if (!isSubtask) {
                            finishedTask = t;
                        }
                    }
                }
            });

            if (finishedTask) {
                const displayName = getTaskDisplayName(finishedTask);
                const resultPrefix = finishedTask.status === 'completed' ? 'Success' : 'Failed';
                announceTaskEvent(`${resultPrefix}: ${displayName}`, false);
            } else {
                // Shrink if it's currently showing an active/running task but there are none
                const announcementEl = document.getElementById('tasks-trigger-announcement');
                if (announcementEl && announcementEl.classList.contains('expanded')) {
                    const txt = announcementEl.textContent || '';
                    if (txt.includes('Running:') || txt.includes('Pending:')) {
                        announcementEl.classList.remove('expanded');
                        setTimeout(() => { 
                            announcementEl.innerHTML = ''; 
                            announcementEl.classList.remove('is-dual');
                        }, 500);
                    }
                }
            }
        }

        // Update knownTasks mapping for subsequent checks
        const currentMap = {};
        newTasksList.forEach(t => {
            currentMap[t.task_id] = { status: t.status, progress: t.progress, action: t.action, displayName: getTaskDisplayName(t) };
        });
        knownTasks = currentMap;
    }

    // Refactored UI rendering logic for tasks
    function renderTasksUI(tasks) {
        checkTaskTransitions(tasks);
        // 1. Determine State and update Badge & Progress Circle
        const activeTasks = tasks.filter(t => t.status === 'pending' || t.status === 'processing');
        const runningTasks = tasks.filter(t => t.status === 'processing');
        const failedTasks = tasks.filter(t => t.status === 'failed');
        const completedTasks = tasks.filter(t => t.status === 'completed');

        // 0. Update running task label and header count badge if elements exist
        const runningTaskLabel = document.getElementById('tasks-running-text');
        if (runningTaskLabel) {
            if (activeTasks.length > 0) {
                const actTask = activeTasks[0];
                let taskName = `${actTask.service || 'system'} - ${actTask.action || 'task'}`;
                if (actTask.payload) {
                    try {
                        const payloadObj = typeof actTask.payload === 'string' ? JSON.parse(actTask.payload) : actTask.payload;
                        if (payloadObj && payloadObj.vm_name) {
                            taskName = `VM '${payloadObj.vm_name}' ${actTask.action}`;
                        } else if (payloadObj && payloadObj.job_name) {
                            taskName = `Job '${payloadObj.job_name}'`;
                        } else if (payloadObj && payloadObj.hostname) {
                            if (actTask.action === 'host_maintenance_enter') {
                                taskName = `Maint Enter: ${payloadObj.hostname}`;
                            } else if (actTask.action === 'host_maintenance_leave') {
                                taskName = `Maint Leave: ${payloadObj.hostname}`;
                            } else {
                                taskName = `Host '${payloadObj.hostname}' ${actTask.action}`;
                            }
                        }
                    } catch (e) {}
                }
                const prefix = actTask.status === 'pending' ? 'Pending' : 'Running';
                runningTaskLabel.textContent = `${prefix}: ${taskName} (${actTask.progress || 0}%)`;
                runningTaskLabel.style.display = 'inline-block';
            } else {
                runningTaskLabel.textContent = '';
                runningTaskLabel.style.display = 'none';
            }
        }

        const headerRunningLabel = document.getElementById('tasks-header-running-label');
        if (headerRunningLabel) {
            headerRunningLabel.style.display = 'none';
        }

        const tasksHeaderCount = document.getElementById('tasks-header-count');
        if (tasksHeaderCount) {
            tasksHeaderCount.textContent = `(${tasks.length})`;
        }

        let stateClass = 'state-grey';
        let strokeColor = 'rgba(255, 255, 255, 0.15)';
        let dashoffset = 56.55;
        let displayCount = 0;
        let badgeBg = 'var(--text-muted)';

        if (activeTasks.length > 0) {
            stateClass = 'state-blue';
            strokeColor = '#2196F3'; // Blue
            displayCount = activeTasks.length;
            const progress = activeTasks[0].progress || 0;
            dashoffset = 56.55 - (progress / 100) * 56.55;
            badgeBg = 'var(--color-primary)';
        } else if (tasks.length > 0) {
            const latestTask = tasks[0];
            if (latestTask.status === 'failed') {
                stateClass = 'state-red';
                strokeColor = '#e74c3c'; // Red
                dashoffset = 0; // Full circle
                displayCount = tasks.length;
                badgeBg = '#e74c3c';
            } else if (latestTask.status === 'completed') {
                stateClass = 'state-green';
                strokeColor = '#2ecc71'; // Green
                dashoffset = 0; // Full circle
                displayCount = tasks.length;
                badgeBg = 'var(--color-success)';
            }
        }

        // Update button container class
        if (tasksDropdownBtn) {
            tasksDropdownBtn.className = `tasks-trigger ${stateClass}`;
        }

        // Update Progress Circle fill
        const progressCircleFill = document.getElementById('tasks-progress-circle-fill');
        if (progressCircleFill) {
            progressCircleFill.style.stroke = strokeColor;
            progressCircleFill.style.strokeDashoffset = dashoffset;
            progressCircleFill.style.fill = 'none';
        }

        // Calculate scale and update CSS variable --tasks-scale (gets bigger as task approaches completion)
        const progressCircleSvg = document.querySelector('.tasks-progress-svg');
        if (progressCircleSvg) {
            if (activeTasks.length > 0) {
                const progress = activeTasks[0].progress || 0;
                // Scale from 1.0 (at 0% progress) to 1.3 (at 100% progress)
                const scaleVal = 1.0 + (progress / 100) * 0.3;
                progressCircleSvg.style.setProperty('--tasks-scale', scaleVal.toFixed(2));
            } else {
                progressCircleSvg.style.setProperty('--tasks-scale', '1.0');
            }
        }

        // Update Badge Count
        if (tasksBadgeCount) {
            tasksBadgeCount.textContent = displayCount;
            tasksBadgeCount.style.display = 'inline-block';
            tasksBadgeCount.style.background = badgeBg;
        }

        // 2. Populate Dropdown List
        if (!tasksMenuList) return;
        if (tasks.length === 0) {
            tasksMenuList.innerHTML = '<div class="tasks-empty-state">No recent tasks</div>';
            return;
        }

        let html = '';
        // Limit to 5 most recent tasks in the dropdown (grouped hierarchically)
        const hierarchicalList = buildTaskTree(tasks);
        const recentTasks = hierarchicalList.slice(0, 5);
        recentTasks.forEach(task => {
            let taskName = `${task.service || 'system'} - ${task.action || 'task'}`;
            if (task.payload) {
                try {
                    const payloadObj = typeof task.payload === 'string' ? jsonParseSafe(task.payload) : task.payload;
                    if (payloadObj && payloadObj.vm_name) {
                        taskName = `VM '${payloadObj.vm_name}' - ${task.action}`;
                    } else if (payloadObj && payloadObj.job_name) {
                        taskName = `Job '${payloadObj.job_name}' - execute`;
                    } else if (payloadObj && payloadObj.hostname) {
                        if (task.action === 'host_maintenance_enter') {
                            taskName = `Host '${payloadObj.hostname}' - Enter Maintenance`;
                        } else if (task.action === 'host_maintenance_leave') {
                            taskName = `Host '${payloadObj.hostname}' - Leave Maintenance`;
                        } else {
                            taskName = `Host '${payloadObj.hostname}' - ${task.action}`;
                        }
                    }
                } catch (e) {}
            }

            // Status formatting
            let statusColor = 'var(--text-muted)';
            if (task.status === 'processing') statusColor = 'var(--color-primary)';
            if (task.status === 'completed') statusColor = 'var(--color-success)';
            if (task.status === 'failed') statusColor = 'var(--color-danger)';

            const progress = task.progress || 0;

            const paddingStyle = (task.depth || 0) > 0 ? `padding-left: ${16 + task.depth * 16}px;` : '';
            const itemBorderLeft = (task.depth || 0) > 0 ? `border-left: 2px solid var(--border-glow);` : '';
            const subtaskPrefix = (task.depth || 0) > 0 ? `<span style="color: var(--text-muted); margin-right: 6px; font-family: monospace;">↳</span> ` : '';

            html += `
                <div class="task-item status-${task.status}" style="${paddingStyle} ${itemBorderLeft}">
                    <div class="task-item-top">
                        <span class="task-item-name" title="${taskName}">${subtaskPrefix}${taskName}</span>
                        <span class="task-item-progress-val">
                            ${progress}%
                            <span class="task-item-status-dot" style="background-color: ${statusColor};"></span>
                        </span>
                    </div>
                    <div class="task-item-progress-bar-container">
                        <div class="task-item-progress-bar" style="width: ${progress}%;"></div>
                    </div>
                </div>
            `;
        });
        tasksMenuList.innerHTML = html;
    }

    async function updateTasksList() {
        try {
            const response = await fetch('/api/catalyst/tasks', {
                headers: {
                    'Authorization': 'Bearer ' + getStoredToken()
                }
            });
            if (!response.ok) return;
            const data = await response.json();
            const tasks = data.tasks || [];

            // Cache tasks to sessionStorage
            sessionStorage.setItem('helios_tasks_cache', JSON.stringify(tasks));

            renderTasksUI(tasks);
        } catch (err) {
            console.error("Error fetching tasks:", err);
        }
    }
    window.updateTasksList = updateTasksList;

    // Load cached tasks immediately for instant rendering on page load/switch
    try {
        const cachedTasksStr = sessionStorage.getItem('helios_tasks_cache');
        if (cachedTasksStr) {
            const cachedTasks = JSON.parse(cachedTasksStr);
            renderTasksUI(cachedTasks);
        }
    } catch (e) {
        console.error("Error rendering cached tasks on startup:", e);
    }

    function jsonParseSafe(str) {
        try {
            return JSON.parse(str);
        } catch (e) {
            return null;
        }
    }

    function buildTaskTree(tasksList) {
        function getTaskTime(t) {
            if (!t) return 0;
            const ts = t.updated_at || t.created_at || 0;
            if (!ts) return 0;
            if (typeof ts === 'number') return ts;
            try {
                const d = new Date(ts);
                return isNaN(d.getTime()) ? 0 : d.getTime();
            } catch (e) {
                return 0;
            }
        }

        function getMaxChangeTime(node) {
            let maxTime = getTaskTime(node);
            node.children.forEach(child => {
                const childTime = getMaxChangeTime(child);
                if (childTime > maxTime) maxTime = childTime;
            });
            return maxTime;
        }

        const taskNodes = tasksList.map(t => {
            let parentId = null;
            if (t.payload) {
                try {
                    const payloadObj = typeof t.payload === 'string' ? jsonParseSafe(t.payload) : t.payload;
                    if (payloadObj && payloadObj.parent_task_id) {
                        parentId = payloadObj.parent_task_id;
                    }
                } catch (e) {}
            }
            return { ...t, children: [], parent_task_id: parentId };
        });

        const nodeMap = {};
        taskNodes.forEach(node => {
            nodeMap[node.task_id] = node;
        });

        const roots = [];
        taskNodes.forEach(node => {
            if (node.parent_task_id && nodeMap[node.parent_task_id]) {
                nodeMap[node.parent_task_id].children.push(node);
            } else {
                roots.push(node);
            }
        });

        Object.values(nodeMap).forEach(node => {
            node.children.sort((a, b) => getTaskTime(a) - getTaskTime(b));
        });

        const flatList = [];
        function traverse(node, depth = 0) {
            node.depth = depth;
            flatList.push(node);
            node.children.forEach(child => traverse(child, depth + 1));
        }

        roots.sort((a, b) => getMaxChangeTime(b) - getMaxChangeTime(a));
        roots.forEach(root => traverse(root, 0));

        return flatList;
    }

    // Poll Catalyst tasks status every 5 seconds
    setInterval(updateTasksList, 5000);
    // Initial fetch
    setTimeout(updateTasksList, 1000);

    // ----------------------------------------------------
    // Wizard Modal Events (vms.html)
    // ----------------------------------------------------
    if (btnOpenCreateModal) {
        btnOpenCreateModal.addEventListener('click', openCreateModal);
    }
    if (btnCloseCreateModal) {
        btnCloseCreateModal.addEventListener('click', closeCreateModal);
    }
    /* Disable click-outside-to-close behavior as requested by user */
    /*
    if (createVmOverlay) {
        createVmOverlay.addEventListener('click', (e) => {
            if (e.target === createVmOverlay) {
                closeCreateModal();
            }
        });
    }
    */

    // Edit Modal Elements & Events
    const editVmOverlay = document.getElementById('edit-vm-overlay');
    const btnCloseEditModal = document.getElementById('btn-close-edit-modal');
    const btnCancelEdit = document.getElementById('btn-cancel-edit');
    const btnSaveEdit = document.getElementById('btn-save-edit');

    if (btnCloseEditModal) {
        btnCloseEditModal.addEventListener('click', closeEditModal);
    }
    if (btnCancelEdit) {
        btnCancelEdit.addEventListener('click', closeEditModal);
    }
    /* Disable click-outside-to-close behavior as requested by user */
    /*
    if (editVmOverlay) {
        editVmOverlay.addEventListener('click', (e) => {
            if (e.target === editVmOverlay) {
                closeEditModal();
            }
        });
    }
    */
    if (btnSaveEdit) {
        btnSaveEdit.addEventListener('click', saveVmEdit);
    }

    function openEditModal(name) {
        const vm = state.vms.find(v => v.name === name);
        if (!vm) return;

        if (editNicListContainer) {
            editNicListContainer.innerHTML = '';
            let nids = [];
            const nid = vm.network_id;
            if (nid) {
                const trimmed = nid.trim();
                if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
                    try {
                        nids = JSON.parse(trimmed);
                    } catch (e) {
                        nids = [nid];
                    }
                } else {
                    nids = [nid];
                }
            } else {
                nids = [];
            }
            if (nids.length > 0) {
                nids.forEach(networkId => {
                    addEditNicRow(networkId);
                });
            } else {
                addEditNicRow("7a68e0d6-11f8-4e89-9430-b3b44b8bc438");
            }
        }

        document.getElementById('edit-vm-name-hidden').value = vm.name;
        document.getElementById('edit-vm-name-display').value = vm.name;
        document.getElementById('edit-vm-vcpus').value = vm.vcpu || vm.vcpus;

        if (vm.memory % 1024 === 0) {
            document.getElementById('edit-vm-memory').value = vm.memory / 1024;
            document.getElementById('edit-vm-memory-unit').value = 'GB';
        } else {
            document.getElementById('edit-vm-memory').value = vm.memory;
            document.getElementById('edit-vm-memory-unit').value = 'MB';
        }

        document.getElementById('edit-vm-firmware').value = vm.firmware || 'bios';
        document.getElementById('edit-vm-boot-device').value = vm.boot_device || '';
        if (document.getElementById('edit-vm-cpu-model')) {
            document.getElementById('edit-vm-cpu-model').value = vm.cpu_model || '';
        }
        if (document.getElementById('edit-vm-audio-enabled')) {
            document.getElementById('edit-vm-audio-enabled').checked = !!vm.audio_enabled;
        }
        if (editCdromListContainer) {
            editCdromListContainer.innerHTML = '';
            if (vm.iso) {
                const isos = vm.iso.split(',');
                isos.forEach(isoName => {
                    addCdromRow('edit-cdrom-list-container', isoName.trim());
                });
            } else {
                addCdromRow('edit-cdrom-list-container');
            }
        }
        
        if (editDiskListContainer) {
            editDiskListContainer.innerHTML = '';
            if (vm.disks_list && vm.disks_list !== "NONE") {
                const disksPayload = vm.disks_list.split(",");
                disksPayload.forEach(d => {
                    let sizeVal = "20";
                    let unitVal = "GB";
                    let containerVal = "default-vm-container";
                    let busVal = "virtio";
                    
                    let sizePart = d;
                    if (d.includes(":")) {
                        const parts = d.split(":");
                        sizePart = parts[0];
                        containerVal = parts[1];
                        if (parts.length > 2) {
                            busVal = parts[2];
                        }
                    }
                    
                    if (sizePart.toUpperCase().endsWith("TB")) {
                        sizeVal = sizePart.substring(0, sizePart.length - 2);
                        unitVal = "TB";
                    } else if (sizePart.toUpperCase().endsWith("GB")) {
                        sizeVal = sizePart.substring(0, sizePart.length - 2);
                        unitVal = "GB";
                    } else if (sizePart.toUpperCase().endsWith("T")) {
                        sizeVal = sizePart.substring(0, sizePart.length - 1);
                        unitVal = "TB";
                    } else if (sizePart.toUpperCase().endsWith("G")) {
                        sizeVal = sizePart.substring(0, sizePart.length - 1);
                        unitVal = "GB";
                    } else {
                        sizeVal = sizePart;
                        unitVal = "GB";
                    }
                    
                    addEditDiskRow(sizeVal, unitVal, containerVal, busVal);
                });
            } else {
                addEditDiskRow("10", "GB", "default-vm-container", "virtio");
            }
        }

        if (editVmOverlay) {
            editVmOverlay.classList.add('open');
        }
    }

    function closeEditModal() {
        if (editVmOverlay) {
            editVmOverlay.classList.remove('open');
        }
    }

    function saveVmEdit(e) {
        e.preventDefault();
        const name = document.getElementById('edit-vm-name-hidden').value;
        const vcpus = parseInt(document.getElementById('edit-vm-vcpus').value);
        const memVal = parseInt(document.getElementById('edit-vm-memory').value);
        const memUnit = document.getElementById('edit-vm-memory-unit').value;
        const memory = memUnit === 'GB' ? memVal * 1024 : memVal;
        const firmware = document.getElementById('edit-vm-firmware').value;
        
        const cdromRows = editCdromListContainer.querySelectorAll('.cdrom-select');
        const isos = [];
        cdromRows.forEach(sel => {
            if (sel.value) isos.push(sel.value);
        });
        const iso = isos.join(',');

        const boot_device = document.getElementById('edit-vm-boot-device').value;
        const cpu_model = document.getElementById('edit-vm-cpu-model') ? document.getElementById('edit-vm-cpu-model').value : "";

        if (!vcpus || vcpus < 1) {
            showToast('Invalid Input', 'vCPUs must be at least 1.', 'danger');
            return;
        }
        if (!memory || memory < 256) {
            showToast('Invalid Input', 'Memory must be at least 256 MB.', 'danger');
            return;
        }

        const diskRows = document.querySelectorAll('#edit-disk-list-container .disk-row');
        const disks = [];
        diskRows.forEach(row => {
            const val = row.querySelector('.disk-size-val').value;
            const unit = row.querySelector('.disk-size-unit').value;
            const container = row.querySelector('.disk-container-val').value;
            const bus = row.querySelector('.disk-bus-val') ? row.querySelector('.disk-bus-val').value : 'virtio';
            disks.push(`${val}${unit}:${container}:${bus}`);
        });

        const nicRows = editNicListContainer ? editNicListContainer.querySelectorAll('.disk-row') : [];
        const networkIds = [];
        nicRows.forEach(row => {
            const netSelect = row.querySelector('.nic-network-select');
            const modelSelect = row.querySelector('.nic-model-select');
            if (netSelect && netSelect.value) {
                const model = modelSelect ? modelSelect.value : 'virtio';
                networkIds.push(`${netSelect.value}:${model}`);
            }
        });
        const networkIdValue = JSON.stringify(networkIds);

        btnSaveEdit.disabled = true;
        fetch(`${state.apiHost}/api/vms/update`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                name: name,
                vcpus: vcpus,
                memory: memory,
                firmware: firmware,
                disks: disks,
                iso: iso,
                boot_device: boot_device,
                network_id: networkIdValue,
                cpu_model: cpu_model,
                audio_enabled: document.getElementById('edit-vm-audio-enabled') ? document.getElementById('edit-vm-audio-enabled').checked : false
            })
        })
        .then(res => {
            if (!res.ok) return res.json().then(e => { throw new Error(e.error || 'Failed to update VM') });
            return res.json();
        })
        .then(data => {
            // showToast('VM Updated', `VM "${name}" updated successfully.`, 'success');
            closeEditModal();
            fetchStatus();
        })
        .catch(err => {
            showToast('Update Failed', err.message, 'danger');
        })
        .finally(() => {
            btnSaveEdit.disabled = false;
        });
        setTimeout(updateTasksList, 150);
    }

    function addDiskRowToContainer(containerEl, sizeVal = "20", unitVal = "GB", containerValue = "default-vm-container", busValue = "virtio") {
        if (!containerEl) return;

        const row = document.createElement('div');
        row.className = 'disk-row';
        row.style.marginBottom = '12px';

        let containerOptions = '';
        if (state.storageContainers && state.storageContainers.length > 0) {
            state.storageContainers.forEach(c => {
                containerOptions += `<option value="${c.name}" ${c.name === containerValue ? 'selected' : ''}>${c.name}</option>`;
            });
        } else {
            containerOptions = `<option value="${containerValue}" selected>${containerValue}</option>`;
        }

        row.innerHTML = `
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px;">
                <span class="disk-title" style="font-size: 12px; font-weight: 600; color: var(--color-primary);">Disk</span>
                <button type="button" class="btn-remove-disk" style="background: none; border: none; color: #ef4444; font-size: 18px; cursor: pointer; padding: 0; line-height: 1;">&times;</button>
            </div>
            <div style="display: flex; gap: 8px;">
                <input type="number" class="form-input disk-size-val" style="flex: 1;" value="${sizeVal}" min="1" required>
                <select class="form-input disk-size-unit" style="width: 80px;">
                    <option value="GB" ${unitVal === 'GB' ? 'selected' : ''}>GB</option>
                    <option value="TB" ${unitVal === 'TB' ? 'selected' : ''}>TB</option>
                </select>
                <select class="form-input disk-container-val" style="flex: 1.5;">
                    ${containerOptions}
                </select>
                <select class="form-input disk-bus-val" style="width: 100px;">
                    <option value="virtio" ${busValue === 'virtio' ? 'selected' : ''}>virtio</option>
                    <option value="sata" ${busValue === 'sata' ? 'selected' : ''}>sata</option>
                    <option value="scsi" ${busValue === 'scsi' ? 'selected' : ''}>scsi</option>
                </select>
            </div>
        `;

        const removeBtn = row.querySelector('.btn-remove-disk');
        removeBtn.addEventListener('click', () => {
            row.remove();
            recalculateDiskIndices(containerEl);
        });

        containerEl.appendChild(row);
        recalculateDiskIndices(containerEl);
    }

    function addDiskRow(sizeVal = "20", unitVal = "GB", containerValue = "default-vm-container", busValue = "virtio") {
        addDiskRowToContainer(diskListContainer, sizeVal, unitVal, containerValue, busValue);
    }

    function addEditDiskRow(sizeVal = "20", unitVal = "GB", containerValue = "default-vm-container", busValue = "virtio") {
        addDiskRowToContainer(editDiskListContainer, sizeVal, unitVal, containerValue, busValue);
    }

    if (btnAddDisk) {
        btnAddDisk.addEventListener('click', () => {
            addDiskRow();
        });
    }

    const btnEditAddDisk = document.getElementById('btn-edit-add-disk');
    if (btnEditAddDisk) {
        btnEditAddDisk.addEventListener('click', () => {
            addEditDiskRow();
        });
    }

    function recalculateDiskIndices(containerEl) {
        if (!containerEl) return;
        const rows = containerEl.querySelectorAll('.disk-row');
        rows.forEach((row, idx) => {
            const titleEl = row.querySelector('.disk-title');
            if (titleEl) {
                titleEl.textContent = idx === 0 ? `Disk #1 (Boot Disk)` : `Disk #${idx + 1}`;
            }
        });
    }



    if (btnAddCdrom) {
        btnAddCdrom.addEventListener('click', () => {
            addCdromRow('cdrom-list-container');
        });
    }

    if (btnEditAddCdrom) {
        btnEditAddCdrom.addEventListener('click', () => {
            addCdromRow('edit-cdrom-list-container');
        });
    }

    const nicListContainer = document.getElementById('nic-list-container');
    const btnAddNic = document.getElementById('btn-add-nic');

    function addNicRow(networkIdSpec = '') {
        if (!nicListContainer) return;

        let networkId = networkIdSpec;
        let nicModel = 'virtio';
        if (networkIdSpec.includes(':')) {
            const parts = networkIdSpec.split(':');
            networkId = parts[0];
            nicModel = parts[1];
        }

        const row = document.createElement('div');
        row.className = 'disk-row'; // layout compatibility
        row.style.display = 'flex';
        row.style.gap = '8px';
        row.style.alignItems = 'center';
        row.style.marginBottom = '8px';

        let selectOptions = '';
        if (state.networks && state.networks.length > 0) {
            state.networks.forEach(net => {
                let label;
                if (net.type === 'vlan') {
                    label = `${net.name} (VLAN ${net.vlan_id})`;
                } else if (net.type === 'overlay') {
                    label = `${net.name} (Overlay / VNI ${net.vni}${net.subnet_cidr ? ' — ' + net.subnet_cidr : ''})`;
                } else {
                    label = `${net.name} (Direct)`;
                }
                selectOptions += `<option value="${net.net_id}" ${net.net_id === networkId ? 'selected' : ''}>${label}</option>`;
            });
        } else {
            selectOptions = `<option value="7a68e0d6-11f8-4e89-9430-b3b44b8bc438" ${networkId === '7a68e0d6-11f8-4e89-9430-b3b44b8bc438' ? 'selected' : ''}>Physical-Direct (System)</option>`;
        }

        row.innerHTML = `
            <select class="form-input nic-network-select" style="flex: 1;">
                ${selectOptions}
            </select>
            <select class="form-input nic-model-select" style="width: 120px;">
                <option value="virtio" ${nicModel === 'virtio' ? 'selected' : ''}>virtio</option>
                <option value="e1000" ${nicModel === 'e1000' ? 'selected' : ''}>e1000</option>
                <option value="e1000e" ${nicModel === 'e1000e' ? 'selected' : ''}>e1000e</option>
                <option value="vmxnet3" ${nicModel === 'vmxnet3' ? 'selected' : ''}>vmxnet3</option>
                <option value="rtl8139" ${nicModel === 'rtl8139' ? 'selected' : ''}>rtl8139</option>
            </select>
            <button type="button" class="btn-remove-nic" style="background: none; border: none; color: #ef4444; font-size: 20px; cursor: pointer; padding: 0 4px; line-height: 1;">&times;</button>
        `;

        const removeBtn = row.querySelector('.btn-remove-nic');
        removeBtn.addEventListener('click', () => {
            row.remove();
        });

        nicListContainer.appendChild(row);
    }

    if (btnAddNic) {
        btnAddNic.addEventListener('click', () => {
            addNicRow();
        });
    }

    const editDiskListContainer = document.getElementById('edit-disk-list-container');
    const editNicListContainer = document.getElementById('edit-nic-list-container');
    const btnEditAddNic = document.getElementById('btn-edit-add-nic');

    function addEditNicRow(networkIdSpec = '') {
        if (!editNicListContainer) return;

        let networkId = networkIdSpec;
        let nicModel = 'virtio';
        if (networkIdSpec.includes(':')) {
            const parts = networkIdSpec.split(':');
            networkId = parts[0];
            nicModel = parts[1];
        }

        const row = document.createElement('div');
        row.className = 'disk-row'; // layout compatibility
        row.style.display = 'flex';
        row.style.gap = '8px';
        row.style.alignItems = 'center';
        row.style.marginBottom = '8px';

        let selectOptions = '';
        if (state.networks && state.networks.length > 0) {
            state.networks.forEach(net => {
                let label;
                if (net.type === 'vlan') {
                    label = `${net.name} (VLAN ${net.vlan_id})`;
                } else if (net.type === 'overlay') {
                    label = `${net.name} (Overlay / VNI ${net.vni}${net.subnet_cidr ? ' — ' + net.subnet_cidr : ''})`;
                } else {
                    label = `${net.name} (Direct)`;
                }
                selectOptions += `<option value="${net.net_id}" ${net.net_id === networkId ? 'selected' : ''}>${label}</option>`;
            });
        } else {
            selectOptions = `<option value="7a68e0d6-11f8-4e89-9430-b3b44b8bc438" ${networkId === '7a68e0d6-11f8-4e89-9430-b3b44b8bc438' ? 'selected' : ''}>Physical-Direct (System)</option>`;
        }

        row.innerHTML = `
            <select class="form-input nic-network-select" style="flex: 1;">
                ${selectOptions}
            </select>
            <select class="form-input nic-model-select" style="width: 120px;">
                <option value="virtio" ${nicModel === 'virtio' ? 'selected' : ''}>virtio</option>
                <option value="e1000" ${nicModel === 'e1000' ? 'selected' : ''}>e1000</option>
                <option value="e1000e" ${nicModel === 'e1000e' ? 'selected' : ''}>e1000e</option>
                <option value="vmxnet3" ${nicModel === 'vmxnet3' ? 'selected' : ''}>vmxnet3</option>
                <option value="rtl8139" ${nicModel === 'rtl8139' ? 'selected' : ''}>rtl8139</option>
            </select>
            <button type="button" class="btn-remove-nic" style="background: none; border: none; color: #ef4444; font-size: 20px; cursor: pointer; padding: 0 4px; line-height: 1;">&times;</button>
        `;

        const removeBtn = row.querySelector('.btn-remove-nic');
        removeBtn.addEventListener('click', () => {
            row.remove();
        });

        editNicListContainer.appendChild(row);
    }

    if (btnEditAddNic) {
        btnEditAddNic.addEventListener('click', () => {
            addEditNicRow();
        });
    }

    function openCreateModal() {
        resetWizard();
        if (diskListContainer) {
            diskListContainer.innerHTML = '';
            addDiskRow("20", "GB");
        }
        if (cdromListContainer) {
            cdromListContainer.innerHTML = '';
            addCdromRow('cdrom-list-container');
        }
        if (nicListContainer) {
            nicListContainer.innerHTML = '';
            addNicRow("7a68e0d6-11f8-4e89-9430-b3b44b8bc438"); // Default system network
        }
        createVmOverlay.classList.add('open');
    }

    function closeCreateModal() {
        createVmOverlay.classList.remove('open');
    }

    if (btnWizardPrev) {
        btnWizardPrev.addEventListener('click', (e) => {
            e.preventDefault();
            if (currentStep > 1) {
                goToStep(currentStep - 1);
            }
        });
    }

    if (btnWizardNext) {
        btnWizardNext.addEventListener('click', (e) => {
            e.preventDefault();
            
            // If on step 1, validate input
            if (currentStep === 1) {
                if (!inputVmName.value.trim()) {
                    showToast('Validation Error', 'VM Name is required.', 'error');
                    inputVmName.focus();
                    return;
                }
                const namePattern = /^[a-zA-Z0-9-]+$/;
                if (!namePattern.test(inputVmName.value.trim())) {
                    showToast('Validation Error', 'VM Name can only contain letters, numbers, and dashes.', 'error');
                    inputVmName.focus();
                    return;
                }
            }

            if (currentStep < 5) {
                goToStep(currentStep + 1);
            } else {
                submitCreateVm();
            }
        });
    }

    function goToStep(stepNumber) {
        currentStep = stepNumber;

        // Update step navigators
        wizardStepNavs.forEach(nav => {
            const navStep = parseInt(nav.getAttribute('data-step'));
            nav.classList.remove('active', 'done');
            
            if (navStep === stepNumber) {
                nav.classList.add('active');
            } else if (navStep < stepNumber) {
                nav.classList.add('done');
            }
        });

        // Update pane visibility
        wizardPanes.forEach(pane => {
            pane.classList.remove('active-pane');
        });
        const targetPane = document.getElementById(`pane-step-${stepNumber}`);
        if (targetPane) {
            targetPane.classList.add('active-pane');
        }

        // Update buttons state
        if (btnWizardPrev) btnWizardPrev.disabled = (stepNumber === 1);
        
        if (btnWizardNext) {
            if (stepNumber === 5) {
                btnWizardNext.textContent = 'Deploy';
                btnWizardNext.classList.add('btn-success');
                // Populate review fields
                if (reviewVmName) reviewVmName.textContent = inputVmName.value.trim();
                if (reviewVmVcpus) reviewVmVcpus.textContent = `${selectVmVcpus.value} Cores`;
                
                const memVal = parseInt(selectVmMemory.value);
                const memUnit = selectVmMemoryUnit.value;
                const memMB = memUnit === "GB" ? memVal * 1024 : memVal;
                const memGB = memUnit === "GB" ? memVal : (memVal / 1024).toFixed(1);
                if (reviewVmMemory) reviewVmMemory.textContent = `${memMB} MB (${memGB} GB)`;
                
                if (reviewVmFirmware) reviewVmFirmware.textContent = selectVmFirmware.value === 'uefi' ? 'UEFI' : 'Legacy BIOS';
                
                const diskRows = diskListContainer.querySelectorAll('.disk-row');
                const disks = [];
                diskRows.forEach(row => {
                    const val = row.querySelector('.disk-size-val').value;
                    const unit = row.querySelector('.disk-size-unit').value;
                    const container = row.querySelector('.disk-container-val').value;
                    disks.push(`${val}${unit} (${container})`);
                });
                if (reviewVmDisks) reviewVmDisks.textContent = disks.join(', ');
                
                const cdromRows = cdromListContainer.querySelectorAll('.cdrom-select');
                const isos = [];
                cdromRows.forEach(sel => {
                    if (sel.value) isos.push(sel.value);
                });
                const isoValue = isos.join(',');
                if (reviewVmIso) reviewVmIso.textContent = isoValue ? isoValue : 'None';

                const nicSelects = nicListContainer ? nicListContainer.querySelectorAll('.nic-network-select') : [];
                const netNames = [];
                nicSelects.forEach(sel => {
                    const option = sel.options[sel.selectedIndex];
                    if (option) netNames.push(option.text);
                });
                const reviewVmNetworks = document.getElementById('review-vm-networks');
                if (reviewVmNetworks) {
                    reviewVmNetworks.textContent = netNames.length > 0 ? netNames.join(', ') : 'None (Isolated)';
                }
            } else {
                btnWizardNext.textContent = 'Next';
                btnWizardNext.classList.remove('btn-success');
            }
        }
    }

    function resetWizard() {
        if (createVmForm) createVmForm.reset();
        goToStep(1);
    }

    // Submit VM definition request
    function submitCreateVm() {
        const memVal = parseInt(selectVmMemory.value);
        const memUnit = selectVmMemoryUnit.value;
        const memMB = memUnit === "GB" ? memVal * 1024 : memVal;

        const diskRows = diskListContainer.querySelectorAll('.disk-row');
        const disks = [];
        diskRows.forEach(row => {
            const val = row.querySelector('.disk-size-val').value;
            const unit = row.querySelector('.disk-size-unit').value;
            const container = row.querySelector('.disk-container-val').value;
            const bus = row.querySelector('.disk-bus-val') ? row.querySelector('.disk-bus-val').value : 'virtio';
            disks.push(`${val}${unit}:${container}:${bus}`);
        });

        const cdromRows = cdromListContainer.querySelectorAll('.cdrom-select');
        const isos = [];
        cdromRows.forEach(sel => {
            if (sel.value) isos.push(sel.value);
        });
        const isoValue = isos.join(',');

        const nicRows = nicListContainer ? nicListContainer.querySelectorAll('.disk-row') : [];
        const networkIds = [];
        nicRows.forEach(row => {
            const netSelect = row.querySelector('.nic-network-select');
            const modelSelect = row.querySelector('.nic-model-select');
            if (netSelect && netSelect.value) {
                const model = modelSelect ? modelSelect.value : 'virtio';
                networkIds.push(`${netSelect.value}:${model}`);
            }
        });
        const networkIdValue = JSON.stringify(networkIds);

        const payload = {
            name: inputVmName.value.trim(),
            vcpus: parseInt(selectVmVcpus.value),
            memory: memMB,
            firmware: selectVmFirmware.value,
            disks: disks,
            iso: isoValue,
            boot_device: document.getElementById('vm-boot-device') ? document.getElementById('vm-boot-device').value : "",
            network_id: networkIdValue,
            cpu_model: document.getElementById('vm-cpu-model') ? document.getElementById('vm-cpu-model').value : "",
            audio_enabled: document.getElementById('vm-audio-enabled') ? document.getElementById('vm-audio-enabled').checked : false
        };

        if (btnWizardNext) {
            btnWizardNext.disabled = true;
            btnWizardNext.textContent = 'Deploying...';
        }

        fetch(`${state.apiHost}/api/vms/create`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(payload)
        })
        .then(response => {
            if (!response.ok) {
                return response.json().then(err => { throw new Error(err.error || 'Server error'); });
            }
            return response.json();
        })
        .then(data => {
            // showToast('Deployment Initiated', `VM '${data.name}' successfully defined on node '${data.node}'.`, 'success');
            closeCreateModal();
            addEventLog(`ZooKeeper: Configured VM metadata for ${data.name}.`);
            fetchStatus();
        })
        .catch(error => {
            showToast('Deployment Failed', error.message, 'error');
        })
        .finally(() => {
            if (btnWizardNext) {
                btnWizardNext.disabled = false;
                btnWizardNext.textContent = 'Deploy';
            }
        });
        setTimeout(updateTasksList, 150);
    }

    // Toggle VM Power state
    function toggleVmPower(name, currentStatus) {
        const targetAction = currentStatus === 'running' ? 'stop' : 'start';
        triggerVmPowerAction(name, targetAction);
    }

    function triggerVmPowerAction(name, action) {
        const button = document.querySelector(`.power-btn[data-name="${name}"]`);
        if (button) button.disabled = true;

        fetch(`${state.apiHost}/api/vms/power`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ name: name, action: action })
        })
        .then(response => {
            if (!response.ok) {
                return response.json().then(err => { throw new Error(err.error || 'Power action failed'); });
            }
            return response.json();
        })
        .then(data => {
            // showToast('Power Action', `VM '${name}' transitioned state to '${data.status}'.`, 'success');
            addEventLog(`Valkyrie: VM ${name} state set to ${data.status}.`);
            fetchStatus();
        })
        .catch(error => {
            showToast('Power Action Failed', error.message, 'error');
            if (button) button.disabled = false;
        });
        setTimeout(updateTasksList, 150);
    }

    // Toast notification alerts
    function showToast(title, message, type = 'info') {
        if (!toastContainer) return;
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        
        toast.innerHTML = `
            <span class="toast-title">${title}</span>
            <span class="toast-msg">${message}</span>
        `;
        
        toastContainer.appendChild(toast);
        
        setTimeout(() => {
            toast.style.animation = 'toastIn 0.3s reverse forwards';
            setTimeout(() => {
                toast.remove();
            }, 300);
        }, 4000);
    }

    // Append to live console events log
    function addEventLog(desc) {
        if (!eventsContainer) return;
        const now = new Date();
        const timeStr = now.toTimeString().split(' ')[0];
        
        const li = document.createElement('li');
        li.className = 'event-item';
        li.innerHTML = `
            <span class="event-time">${timeStr}</span>
            <span class="event-desc">${desc}</span>
        `;
        eventsContainer.prepend(li);
        
        if (eventsContainer.children.length > 8) {
            eventsContainer.removeChild(eventsContainer.lastChild);
        }
    }

    // Inject connection error banner styles dynamically
    const styleEl = document.createElement('style');
    styleEl.textContent = `
        .connection-error-banner {
            background: rgba(239, 68, 68, 0.15);
            border-bottom: 1px solid rgba(239, 68, 68, 0.3);
            color: var(--color-danger);
            padding: 10px 20px;
            display: flex;
            align-items: center;
            gap: 10px;
            font-size: 13px;
            font-weight: 500;
            backdrop-filter: blur(10px);
            -webkit-backdrop-filter: blur(10px);
            animation: slideDown 0.3s ease-out;
            z-index: 1000;
            position: relative;
        }
        .connection-error-banner svg {
            width: 16px;
            height: 16px;
            fill: var(--color-danger);
            flex-shrink: 0;
        }
        @keyframes slideDown {
            from { transform: translateY(-100%); opacity: 0; }
            to { transform: translateY(0); opacity: 1; }
        }
    `;
    document.head.appendChild(styleEl);

    function showConnectionError() {
        let banner = document.getElementById('connection-error-banner');
        if (!banner) {
            banner = document.createElement('div');
            banner.id = 'connection-error-banner';
            banner.className = 'connection-error-banner';
            banner.innerHTML = `
                <svg viewBox="0 0 24 24">
                    <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-2h2v2zm0-4h-2V7h2v6z"/>
                </svg>
                <span>Lost connection to the Helios management service. Reconnecting...</span>
            `;
            const workspace = document.querySelector('.app-workspace');
            if (workspace) {
                workspace.parentNode.insertBefore(banner, workspace);
            } else {
                document.body.insertBefore(banner, document.body.firstChild);
            }
        }
        banner.style.display = 'flex';

        // Set metrics elements to error state to prevent showing stale placeholders
        const peCpuVal = document.getElementById('pe-cpu-val');
        if (peCpuVal) {
            peCpuVal.textContent = '---';
            const cpuSub = peCpuVal.parentNode.querySelector('.sub');
            if (cpuSub) cpuSub.textContent = 'Service Unreachable';
        }
        const peMemVal = document.getElementById('pe-mem-val');
        if (peMemVal) {
            peMemVal.textContent = '---';
            const memSub = peMemVal.parentNode.querySelector('.sub');
            if (memSub) memSub.textContent = 'Service Unreachable';
        }
        const peStorageCapacity = document.getElementById('pe-storage-capacity');
        if (peStorageCapacity) peStorageCapacity.textContent = 'Service Unreachable';
        const peStorageUsed = document.getElementById('pe-storage-used');
        if (peStorageUsed) peStorageUsed.textContent = '---';
        const peStorageBar = document.getElementById('pe-storage-bar');
        if (peStorageBar) peStorageBar.style.width = '0%';
        
        const storageUsedDisplay = document.getElementById('storage-used-display');
        if (storageUsedDisplay) storageUsedDisplay.innerHTML = 'Service Unreachable';
        const storageBar = document.getElementById('storage-bar');
        if (storageBar) storageBar.style.width = '0%';

        const peHypervisorVersion = document.getElementById('pe-hypervisor-version');
        if (peHypervisorVersion) peHypervisorVersion.textContent = '---';
    }

    function hideConnectionError() {
        const banner = document.getElementById('connection-error-banner');
        if (banner) {
            banner.style.display = 'none';
        }
    }

    // Real-time polling function
    function fetchStatus() {
        fetch(`${state.apiHost}/api/status`)
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            hideConnectionError();
            state.vms = data.vms.list || [];
            state.nodes = data.nodes || [];
            state.clusterName = data.cluster_name || 'unnamed-cluster';
            state.metrics = data.metrics || null;
            
            try {
                sessionStorage.setItem('status_api_cache', JSON.stringify(data));
            } catch (e) {}

            updateSharedHeader(data);
            updatePageSpecificContent(data);
            fetchDrsStatus();
        })
        .catch(err => {
            console.error('Error fetching cluster status:', err);
            showConnectionError();
        });
    }

    function fetchDrsStatus() {
        fetch(`${state.apiHost}/api/vms/drs`)
        .then(response => {
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            return response.json();
        })
        .then(data => {
            const drsBalanceScore = document.getElementById('drs-balance-score');
            const drsProgressBar = document.getElementById('drs-progress-bar');
            const drsStatusStr = document.getElementById('drs-status-str');
            const drsDeviationVal = document.getElementById('drs-deviation-val');
            const drsLastMigration = document.getElementById('drs-last-migration');
            
            const drsGaugeFill = document.getElementById('drs-gauge-fill');
            const drsBalanceScoreText = document.getElementById('drs-balance-score-text');

            const deviation = data.current_deviation !== undefined ? data.current_deviation : 0.0;
            const score = Math.max(0, Math.min(100, Math.round((1 - 2 * deviation) * 100)));
            const status = data.status_str || 'Balanced (happy)';

            if (drsBalanceScore) drsBalanceScore.textContent = `${score}%`;
            if (drsBalanceScoreText) drsBalanceScoreText.textContent = `${score}%`;
            if (drsStatusStr) drsStatusStr.textContent = status;
            if (drsDeviationVal) drsDeviationVal.textContent = deviation.toFixed(4);

            let color = '#10B981'; // Green
            if (score < 50) {
                color = '#EF4444'; // Red
            } else if (score < 80) {
                color = '#F59E0B'; // Orange/Yellow
            }

            if (drsProgressBar) {
                drsProgressBar.style.width = `${score}%`;
                drsProgressBar.style.backgroundColor = color;
            }

            if (drsGaugeFill) {
                const totalLength = 125.66;
                const offset = totalLength - (score / 100) * totalLength;
                drsGaugeFill.style.strokeDashoffset = offset;
                drsGaugeFill.style.stroke = color;
            }

            if (drsLastMigration) {
                const history = data.history || [];
                if (history.length > 0) {
                    const last = history[history.length - 1];
                    let dateStr = 'Just now';
                    if (last.event_time) {
                        const date = new Date(last.event_time);
                        dateStr = date.toLocaleTimeString();
                    }
                    drsLastMigration.textContent = `${last.vm_name} migrated from ${last.source_host} to ${last.target_host} (${dateStr})`;
                } else {
                    drsLastMigration.textContent = 'No recent migrations.';
                }
            }
        })
        .catch(err => {
            console.error('Error fetching DRS status:', err);
        });
    }

    // Bind Rebalance Button Click
    const btnRebalance = document.getElementById('btn-rebalance');
    if (btnRebalance) {
        btnRebalance.addEventListener('click', () => {
            btnRebalance.disabled = true;
            btnRebalance.textContent = 'Balancing...';
            fetch(`${state.apiHost}/api/vms/balance`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' }
            })
            .then(response => {
                if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
                return response.json();
            })
            .then(data => {
                showToast('DRS Action', data.message || 'DRS rebalancing initiated.', 'success');
                fetchDrsStatus();
            })
            .catch(err => {
                console.error('Error triggering DRS:', err);
                showToast('DRS Error', 'Failed to trigger DRS: ' + err.message, 'danger');
            })
            .finally(() => {
                btnRebalance.disabled = false;
                btnRebalance.textContent = 'Rebalance';
            });
        });
    }

    // Update Cluster status info in Header
    function updateSharedHeader(data) {
        if (clusterNameDisplay) {
            clusterNameDisplay.textContent = state.clusterName;
        }
        
        const brandDisplay = document.getElementById('cluster-name-brand-display');
        if (brandDisplay) {
            brandDisplay.textContent = state.clusterName;
        }
        
        const leaderNode = state.nodes.find(n => n.role === 'Leader') || state.nodes[0];
        if (leaderNodeDisplay) {
            if (leaderNode) {
                leaderNodeDisplay.textContent = `${formatNodeName(leaderNode.name)} (${leaderNode.role})`;
            } else {
                leaderNodeDisplay.textContent = 'Consensus Pending...';
            }
        }
    }

    // Update components based on active HTML page structures
    function updatePageSpecificContent(data) {
        const offlineHosts = state.nodes.filter(n => n.status !== 'ONLINE').length;
        // 1. Nutanix PE Dashboard Widgets (index.html)
        const peHvVmCount = document.getElementById('pe-hv-vm-count');
        if (peHvVmCount) {
            const activeVms = state.vms.filter(v => v.status === 'running');
            const stoppedVms = state.vms.filter(v => v.status === 'stopped');
            
            // VMs summary
            peHvVmCount.textContent = state.vms.length;
            const peHvVmOn = document.getElementById('pe-hv-vm-on');
            if (peHvVmOn) peHvVmOn.textContent = activeVms.length;
            const peHvVmOff = document.getElementById('pe-hv-vm-off');
            if (peHvVmOff) peHvVmOff.textContent = stoppedVms.length;
            
            // Storage Summary (Dashboard)
            const usedGb = data.storage.used_gb;
            const totalGb = data.storage.total_gb;
            const storagePercent = totalGb > 0 ? (usedGb / totalGb) * 100 : 0;
            
            let usedStr, totalStr;
            if (totalGb >= 1000) {
                usedStr = `${(usedGb / 1024).toFixed(2)} TiB`;
                totalStr = `${(totalGb / 1024).toFixed(2)} TiB`;
            } else {
                usedStr = `${usedGb.toFixed(1)} GB`;
                totalStr = `${totalGb.toFixed(1)} GB`;
            }
            
            const peStorageCapacity = document.getElementById('pe-storage-capacity');
            if (peStorageCapacity) peStorageCapacity.textContent = `${totalStr} Total Capacity`;
            const peStorageUsed = document.getElementById('pe-storage-used');
            if (peStorageUsed) peStorageUsed.textContent = `${usedStr} Used`;
            const peStorageBar = document.getElementById('pe-storage-bar');
            if (peStorageBar) {
                peStorageBar.style.width = `${storagePercent}%`;
                ensureHaMarker('pe-storage-bar');
            }
            
            // Hardware Summary
            const peHwHosts = document.getElementById('pe-hw-hosts');
            if (peHwHosts) peHwHosts.textContent = state.nodes.length;
            const peHwDisks = document.getElementById('pe-hw-disks');
            if (peHwDisks) {
                const totalDisks = state.nodes.reduce((acc, node) => acc + (node.disks || 0), 0);
                peHwDisks.textContent = totalDisks;
            }
            
            // Cluster CPU & Memory Usage
            const peCpuVal = document.getElementById('pe-cpu-val');
            const peMemVal = document.getElementById('pe-mem-val');
            
            if (state.metrics) {
                if (peCpuVal) peCpuVal.textContent = `${state.metrics.cpu_pct.toFixed(2)}%`;
                const cpuSub = document.getElementById('pe-cpu-sub');
                if (cpuSub) cpuSub.textContent = `OF ${state.metrics.cpu_cores} Cores (${state.metrics.total_cpu_ghz.toFixed(1)} GHz)`;
                
                if (peMemVal) peMemVal.textContent = `${state.metrics.mem_pct.toFixed(2)}%`;
                const memSub = document.getElementById('pe-mem-sub');
                if (memSub) memSub.textContent = `OF ${state.metrics.total_mem_gb.toFixed(1)} GiB`;
                
                // Append real-time metrics to chart histories
                if (state.charts) {
                    if (state.charts['chart-cpu']) state.charts['chart-cpu'].append(state.metrics.cpu_pct);
                    if (state.charts['chart-mem']) state.charts['chart-mem'].append(state.metrics.mem_pct);
                    if (state.charts['chart-iops']) state.charts['chart-iops'].append(state.metrics.iops || 0);
                    if (state.charts['chart-bw']) state.charts['chart-bw'].append((state.metrics.bw_kbps || 0) / 1024.0); // Convert KB/s to MB/s
                    if (state.charts['chart-latency']) state.charts['chart-latency'].append(state.metrics.latency_ms || 0);
                }
            } else {
                if (peCpuVal) peCpuVal.textContent = '0.00%';
                const cpuSub = document.getElementById('pe-cpu-sub');
                if (cpuSub) cpuSub.textContent = 'OF 0 Cores (0.0 GHz)';
                
                if (peMemVal) peMemVal.textContent = '0.00%';
                const memSub = document.getElementById('pe-mem-sub');
                if (memSub) memSub.textContent = 'OF 0.0 GiB';
            }
            

            
            // Cluster Resiliency / Fault Tolerance
            const peResiliencyFt = document.getElementById('pe-resiliency-ft');
            if (peResiliencyFt) {
                if (data.resiliency) {
                    peResiliencyFt.textContent = data.resiliency.ftt;
                    const resiliencyContainer = document.querySelector('.resiliency-ok');
                    if (resiliencyContainer) {
                        resiliencyContainer.textContent = data.resiliency.status;
                        if (data.resiliency.status === 'GOOD') {
                            resiliencyContainer.style.color = 'var(--color-success)';
                        } else if (data.resiliency.status === 'DEGRADED') {
                            resiliencyContainer.style.color = 'var(--color-warning)';
                        } else {
                            resiliencyContainer.style.color = 'var(--color-danger)';
                        }
                    }
                } else {
                    peResiliencyFt.textContent = offlineHosts > 0 ? '0' : '1';
                    const resiliencyContainer = document.querySelector('.resiliency-ok');
                    if (resiliencyContainer) {
                        if (offlineHosts > 0) {
                            resiliencyContainer.textContent = 'NOT GOOD';
                            resiliencyContainer.style.color = 'var(--color-danger)';
                        } else {
                            resiliencyContainer.textContent = 'GOOD';
                            resiliencyContainer.style.color = 'var(--color-success)';
                        }
                    }
                }
            }
            
            // Alerts and Warning Alerts
            const alertsList = document.getElementById('pe-critical-alerts-list');
            const alertsCount = document.getElementById('pe-critical-alerts-count');
            const warningAlertsCount = document.getElementById('pe-warning-alerts-count');
            const warningAlertsList = document.getElementById('pe-warning-alerts-list');
            
            const criticals = (data.alerts || []).filter(a => a.type === 'critical');
            const warnings = (data.alerts || []).filter(a => a.type === 'warning');
            const infos = (data.alerts || []).filter(a => a.type === 'info');
            
            if (alertsCount) alertsCount.textContent = `${criticals.length} Critical`;
            if (alertsList) {
                let newHtml = '';
                if (criticals.length === 0) {
                    newHtml = `
                        <div class="alert-item-pe">
                            <span class="alert-item-desc">No critical alerts detected in metadata store.</span>
                        </div>
                    `;
                } else {
                    newHtml = criticals.map(alt => `
                        <div class="alert-item-pe clickable-alert" data-check="${alt.check_name || ''}" data-node="${alt.node_ip || ''}" style="cursor: pointer;">
                            <div class="alert-item-desc">${alt.desc}</div>
                            <div class="alert-item-time">${alt.time}</div>
                        </div>
                    `).join('');
                }
                if (alertsList.innerHTML !== newHtml) {
                    alertsList.innerHTML = newHtml;
                }
            }
            
            if (warningAlertsCount) {
                warningAlertsCount.textContent = `${warnings.length} Warning`;
            }
            if (warningAlertsList) {
                let newHtml = '';
                if (warnings.length === 0) {
                    newHtml = `
                        <div class="alert-item-pe">
                            <span class="alert-item-desc">All background services reporting healthy.</span>
                        </div>
                    `;
                } else {
                    newHtml = warnings.map(alt => `
                        <div class="alert-item-pe clickable-alert" data-check="${alt.check_name || ''}" data-node="${alt.node_ip || ''}" style="cursor: pointer;">
                            <div class="alert-item-desc">${alt.desc}</div>
                            <div class="alert-item-time">${alt.time || 'Just now'}</div>
                        </div>
                    `).join('');
                }
                if (warningAlertsList.innerHTML !== newHtml) {
                    warningAlertsList.innerHTML = newHtml;
                }
            }
            
            const infoAlertsVal = document.getElementById('pe-info-alerts-val');
            if (infoAlertsVal) infoAlertsVal.textContent = infos.length;
            
            const eventsVal = document.getElementById('pe-events-val');
            if (eventsVal) eventsVal.textContent = (data.events || []).length;
        }

        // Original Sidebar / Fallback Widgets Updates
        if (vcpusStat || memoryStat || nodesContainer || storageUsedDisplay) {
            let totalVcpus = 0;
            let totalMem = 0;
            state.vms.forEach(vm => {
                if (vm.status === 'running') {
                    totalVcpus += parseInt(vm.vcpus || 1);
                    totalMem += parseInt(vm.memory || 1024);
                }
            });

            if (vcpusStat && vcpusBar) {
                vcpusStat.textContent = `${totalVcpus} Cores`;
                const cpuPercent = Math.min((totalVcpus / 32) * 100, 100);
                vcpusBar.style.width = `${cpuPercent}%`;
            }

            if (memoryStat && memoryBar) {
                const totalMemGb = (totalMem / 1024).toFixed(1);
                memoryStat.textContent = `${totalMemGb} GB`;
                const memPercent = Math.min((totalMem / (64 * 1024)) * 100, 100);
                memoryBar.style.width = `${memPercent}%`;
            }

            const usedGb = data.storage.used_gb;
            const totalGb = data.storage.total_gb;
            const storagePercent = totalGb > 0 ? (usedGb / totalGb) * 100 : 0;
            
            let usedStrSidebar, totalStrSidebar;
            if (totalGb >= 1000) {
                usedStrSidebar = `${(usedGb / 1000).toFixed(1)} TB`;
                totalStrSidebar = `${(totalGb / 1000).toFixed(1)} TB`;
            } else {
                usedStrSidebar = `${usedGb.toFixed(1)} GB`;
                totalStrSidebar = `${totalGb.toFixed(1)} GB`;
            }
            
            if (storageUsedDisplay) {
                storageUsedDisplay.innerHTML = `${usedStrSidebar} <span class="muted">/ ${totalStrSidebar} Used</span>`;
            }
            if (storageBar) {
                storageBar.style.width = `${storagePercent}%`;
                ensureHaMarker('storage-bar');
            }

            if (nodesContainer) {
                nodesContainer.innerHTML = '';
                state.nodes.forEach(node => {
                    const isOnline = node.status === 'ONLINE';
                    const nodeDiv = document.createElement('div');
                    nodeDiv.className = 'node-item';
                    
                    let metaP = `<p>${node.ip}</p>`;
                    if (isOnline && node.cpu_pct !== undefined && node.ram_total_gb !== undefined) {
                        metaP = `<p>${node.ip}<br>CPU: ${node.cpu_pct.toFixed(1)}% • RAM: ${node.ram_used_gb.toFixed(1)}/${node.ram_total_gb.toFixed(1)} GB</p>`;
                    }
                    
                    let dotClass = 'status-dot-offline';
                    if (isOnline) {
                        const maint = node.maintenance_status || 'NORMAL';
                        if (maint === 'IN_MAINTENANCE') {
                            dotClass = 'status-dot-purple';
                        } else if (maint === 'ENTERING_MAINTENANCE') {
                            dotClass = 'status-dot-orange';
                        } else {
                            dotClass = 'status-dot-online';
                        }
                    }
                    
                    nodeDiv.innerHTML = `
                        <div class="node-left">
                            <span class="node-status-dot ${dotClass}"></span>
                            <div class="node-meta">
                                <h4>${formatNodeName(node.name)}</h4>
                                ${metaP}
                            </div>
                        </div>
                        <span class="node-role-badge ${node.role === 'Leader' ? 'leader' : ''}">${node.role}</span>
                    `;
                    nodesContainer.appendChild(nodeDiv);
                });
            }

            if (document.getElementById('node-ops-table-body')) {
                updateNodeOpsTable();
            }
        }

        const sdnFabricNodesList = document.getElementById('sdn-fabric-nodes-list');
        if (sdnFabricNodesList) {
            sdnFabricNodesList.innerHTML = '';
            state.nodes.forEach(node => {
                const isOnline = node.status === 'ONLINE';
                const nodeDiv = document.createElement('div');
                nodeDiv.className = 'node-item';
                
                let dotClass = 'status-dot-offline';
                if (isOnline) {
                    const maint = node.maintenance_status || 'NORMAL';
                    if (maint === 'IN_MAINTENANCE') {
                        dotClass = 'status-dot-purple';
                    } else if (maint === 'ENTERING_MAINTENANCE') {
                        dotClass = 'status-dot-orange';
                    } else {
                        dotClass = 'status-dot-online';
                    }
                }
                
                nodeDiv.innerHTML = `
                    <div class="node-left">
                        <span class="node-status-dot ${dotClass}"></span>
                        <div class="node-meta">
                            <h4>${formatNodeName(node.name)}</h4>
                            <p>${node.ip}</p>
                        </div>
                    </div>
                    <span class="node-role-badge ${node.role === 'Leader' ? 'leader' : ''}">${node.role.toUpperCase()}</span>
                `;
                sdnFabricNodesList.appendChild(nodeDiv);
            });
        }

        // 2. VMs Page Table Grid (vms.html)
        if (vmsTableBody) {
            updateVmsTable();
        }

        // Storage Containers & Virtual Disks (storage.html)
        const storageContainersTableBody = document.getElementById('storage-containers-table-body');
        if (storageContainersTableBody) {
            updateStorageContainersTable();
        }
        const storageVirtualDisksTableBody = document.getElementById('storage-virtual-disks-table-body');
        if (storageVirtualDisksTableBody) {
            updateStorageVirtualDisksTable();
        }

        const servicesGrid = document.getElementById('mimir-services-grid');
        if (servicesGrid) {
            // Bind change listeners to Mimir health filter checkboxes
            ['filter-health-critical', 'filter-health-warning', 'filter-health-pass'].forEach(id => {
                const el = document.getElementById(id);
                if (el) {
                    el.addEventListener('change', () => {
                        applyMimirFiltersAndRender();
                    });
                }
            });
            fetchMimirResults();
        }

        // QEMU version binding
        const peHypervisorVersion = document.getElementById('pe-hypervisor-version');
        if (peHypervisorVersion && data.hypervisor_version) {
            peHypervisorVersion.textContent = data.hypervisor_version;
        }

        // Events Log Terminal binding
        if (eventsContainer && data.events) {
            eventsContainer.innerHTML = '';
            data.events.forEach(evt => {
                const li = document.createElement('li');
                li.className = 'event-item';
                li.innerHTML = `
                    <span class="event-time">${evt.time}</span>
                    <span class="event-desc">${evt.desc}</span>
                `;
                eventsContainer.appendChild(li);
            });
        }

        // Dynamic Physical Storage Pools binding
        const peStoragePoolsContainer = document.getElementById('pe-storage-pools-container');
        const pePhysicalDisksContainer = document.getElementById('pe-physical-disks-container');
        if (data.storage && data.storage.pools) {
            if (peStoragePoolsContainer) peStoragePoolsContainer.innerHTML = '';
            if (pePhysicalDisksContainer) pePhysicalDisksContainer.innerHTML = '';
            
            data.storage.pools.forEach(pool => {
                const isOnline = pool.status === 'ONLINE';
                const isWarning = pool.status === 'DEGRADED' || pool.status === 'UNKNOWN';
                const statusColor = isOnline ? 'var(--color-success)' : (isWarning ? 'var(--color-warning)' : 'var(--color-danger)');
                
                if (pool.name.startsWith("Physical Disk")) {
                    if (pePhysicalDisksContainer) {
                        const item = document.createElement('div');
                        item.className = 'physical-disk-item';
                        item.style.cssText = 'display: flex; align-items: center; justify-content: space-between; padding: 12px 16px; background: rgba(255, 255, 255, 0.01); border: 1px solid var(--border-color); border-radius: var(--border-radius); transition: all var(--transition-speed);';
                        
                        const isSsd = pool.type && pool.type.toLowerCase().includes('ssd');
                        const mediaIcon = isSsd 
                            ? `<svg viewBox="0 0 24 24" style="width: 20px; height: 20px; fill: currentColor;"><path d="M19 3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm-2 10h-4v-2h4v2zm0-4h-4V7h4v2zm-6 8H7v-2h4v2zm0-4H7v-2h4v2zm0-4H7V7h4v2z"/></svg>`
                            : `<svg viewBox="0 0 24 24" style="width: 20px; height: 20px; fill: currentColor;"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm0 18c-4.41 0-8-3.59-8-8s3.59-8 8-8 8 3.59 8 8-3.59 8-8 8zm0-11c-1.66 0-3 1.34-3 3s1.34 3 3 3 3-1.34 3-3-1.34-3-3-3z"/></svg>`;
                        
                        const badgeBg = isOnline ? 'rgba(16, 185, 129, 0.08)' : (isWarning ? 'rgba(245, 158, 11, 0.08)' : 'rgba(239, 68, 68, 0.08)');
                        const badgeBorder = isOnline ? 'rgba(16, 185, 129, 0.2)' : (isWarning ? 'rgba(245, 158, 11, 0.2)' : 'rgba(239, 68, 68, 0.2)');
                        
                        item.innerHTML = `
                            <div style="display: flex; align-items: center; gap: 14px;">
                                <div style="color: ${statusColor}; display: flex; align-items: center; justify-content: center; width: 36px; height: 36px; background: rgba(255,255,255,0.02); border: 1px solid var(--border-color); border-radius: 6px;">
                                    ${mediaIcon}
                                </div>
                                <div>
                                    <h4 style="margin: 0; font-size: 13.5px; font-weight: 600; color: var(--text-primary);">${pool.name}</h4>
                                    <p style="margin: 3px 0 0; font-size: 11.5px; color: var(--text-muted);">${pool.type} • Path: ${pool.path}</p>
                                </div>
                            </div>
                            <div style="display: flex; align-items: center; gap: 18px;">
                                <span style="background: ${badgeBg}; color: ${statusColor}; border: 1px solid ${badgeBorder}; padding: 3px 9px; border-radius: 4px; font-size: 10.5px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px;">${pool.status || 'UNKNOWN'}</span>
                                <span style="font-family: 'Space Grotesk', sans-serif; font-weight: 700; font-size: 13.5px; color: var(--color-primary-hover);">${pool.size}</span>
                            </div>
                        `;
                        pePhysicalDisksContainer.appendChild(item);
                    }
                } else {
                    if (peStoragePoolsContainer) {
                        const card = document.createElement('div');
                        card.className = 'disk-card';
                        card.style.cssText = 'flex-direction: column; align-items: stretch; gap: 14px; padding: 20px; background: rgba(255, 255, 255, 0.02); border: 1px solid var(--border-color); border-radius: var(--border-radius); transition: all var(--transition-speed);';
                        
                        const totalGb = pool.total_gb || 0;
                        const usedGb = pool.used_gb || 0;
                        const usedPercent = totalGb > 0 ? (usedGb / totalGb) * 100 : 0;
                        
                        const badgeBg = isOnline ? 'rgba(16, 185, 129, 0.08)' : (isWarning ? 'rgba(245, 158, 11, 0.08)' : 'rgba(239, 68, 68, 0.08)');
                        const badgeBorder = isOnline ? 'rgba(16, 185, 129, 0.2)' : (isWarning ? 'rgba(245, 158, 11, 0.2)' : 'rgba(239, 68, 68, 0.2)');
                        
                        const formatSizeLocal = (gb) => {
                            if (!gb) return '0 GB';
                            if (gb >= 1024) return `${(gb / 1024).toFixed(2)} TB`;
                            return `${gb} GB`;
                        };
                        
                        card.innerHTML = `
                            <div style="display: flex; align-items: center; justify-content: space-between; gap: 10px; width: 100%;">
                                <div style="display: flex; align-items: center; gap: 12px;">
                                    <div style="color: ${statusColor}; display: flex; align-items: center; justify-content: center; width: 40px; height: 40px; background: rgba(255, 255, 255, 0.03); border: 1px solid var(--border-color); border-radius: 8px;">
                                        <svg viewBox="0 0 24 24" style="width: 22px; height: 22px; fill: currentColor;"><path d="M19 13H5v-2h14v2zm-2-7H7v2h10V6zm2 14H5v-2h14v2zM12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2z"/></svg>
                                    </div>
                                    <div>
                                        <h4 style="margin: 0; font-size: 14px; font-weight: 600; color: var(--text-primary);">${pool.name}</h4>
                                        <p style="margin: 2px 0 0; font-size: 12px; color: var(--text-muted);">${pool.type}</p>
                                    </div>
                                </div>
                                <div style="text-align: right;">
                                    <span style="background: ${badgeBg}; color: ${statusColor}; border: 1px solid ${badgeBorder}; padding: 3px 8px; border-radius: 4px; font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px;">${pool.status || 'UNKNOWN'}</span>
                                </div>
                            </div>
                            
                            <div style="font-size: 12px; color: var(--text-secondary); display: flex; justify-content: space-between; margin-top: 4px; width: 100%;">
                                <span>Path: <code style="font-family: 'Fira Code', monospace; background: rgba(0,0,0,0.25); padding: 2px 5px; border-radius: 4px; font-size: 11px; color: var(--color-primary-hover);">${pool.path}</code></span>
                                <span style="font-weight: 700; color: var(--color-primary-hover);">${usedPercent.toFixed(1)}% Used</span>
                            </div>

                            <div class="progress-bar-container" style="height: 6px; background: rgba(255,255,255,0.05); border-radius: 3px; overflow: hidden; margin-top: 4px; width: 100%;">
                                <div class="progress-bar" style="width: ${usedPercent}%; height: 100%; background: ${statusColor}; border-radius: 3px; box-shadow: 0 0 8px ${statusColor};"></div>
                            </div>

                            <div style="font-size: 11.5px; color: var(--text-muted); display: flex; justify-content: space-between; width: 100%; margin-top: 2px;">
                                <span>${formatSizeLocal(usedGb)} Used</span>
                                <span>${formatSizeLocal(totalGb)} Total</span>
                            </div>
                        `;
                        peStoragePoolsContainer.appendChild(card);
                    }
                }
            });
        }

        // Dynamic Host Interfaces binding
        const hostInterfacesContainer = document.getElementById('host-interfaces-container');
        if (hostInterfacesContainer && data.interfaces) {
            hostInterfacesContainer.innerHTML = '';
            data.interfaces.forEach(iface => {
                const item = document.createElement('div');
                item.className = 'node-item';
                const isOnline = iface.status === 'ONLINE';
                item.innerHTML = `
                    <div class="node-left">
                        <span class="node-status-dot ${isOnline ? 'status-dot-online' : 'status-dot-offline'}"></span>
                        <div class="node-meta">
                            <h4>${iface.name}</h4>
                            <p>${iface.type} - Link ${isOnline ? 'Up' : 'Down'}</p>
                        </div>
                    </div>
                    <span class="node-role-badge ${iface.type === 'Bridge' ? 'leader' : ''}">${iface.type}</span>
                `;
                hostInterfacesContainer.appendChild(item);
            });
        }

        // Dynamic Bridge properties binding
        if (data.bridge) {
            const brName = document.getElementById('net-bridge-name');
            const brIp = document.getElementById('net-bridge-ip');
            const brMtu = document.getElementById('net-bridge-mtu');
            const brBind = document.getElementById('net-bridge-bind');
            if (brName) brName.textContent = data.bridge.name;
            if (brIp) brIp.textContent = data.bridge.ip;
            if (brMtu) brMtu.textContent = `${data.bridge.mtu} Bytes`;
            if (brBind) brBind.textContent = data.bridge.bind;
        }

        // Live Port Throughput speed binding
        const netRxVal = document.getElementById('net-rx-val');
        const netTxVal = document.getElementById('net-tx-val');
        if (netRxVal && state.metrics) {
            netRxVal.textContent = `${state.metrics.net_rx_mbps.toFixed(1)} Mbps`;
        }
        if (netTxVal && state.metrics) {
            netTxVal.textContent = `${state.metrics.net_tx_mbps.toFixed(1)} Mbps`;
        }

        // Dynamic Networking Page update
        if (document.getElementById('networking-topology-container')) {
            if (typeof triggerNetworkingPageUpdate === 'function') {
                triggerNetworkingPageUpdate();
            }
        }
    }

    let vncActiveTimeout = null;
    let consolePollInterval = null;

    function openVmConsole(vmName) {
        let consoleModal = document.getElementById('vm-console-modal');
        if (!consoleModal) {
            consoleModal = document.createElement('div');
            consoleModal.id = 'vm-console-modal';
            consoleModal.className = 'console-modal-overlay';
            consoleModal.innerHTML = `
                <div class="console-modal-card">
                    <!-- noVNC Left control panel menu slider -->
                    <div class="novnc-control-bar" id="novnc-control-bar">
                        <div class="novnc-control-bar-handle" id="novnc-bar-handle" title="Toggle noVNC Controls">☰</div>
                        <button class="novnc-control-btn" id="novnc-btn-kbd" title="Show/Hide Virtual Keyboard">⌨️</button>
                        <button class="novnc-control-btn" id="novnc-btn-clip" title="Show/Hide Clipboard Drawer">📋</button>
                        <button class="novnc-control-btn" id="novnc-btn-cdrom" title="Mount/Eject CD-ROM Image">💿</button>
                        <button class="novnc-control-btn" id="novnc-btn-power" title="Power Control VM">🔌</button>
                        <button class="novnc-control-btn" id="novnc-btn-cad" title="Send Ctrl-Alt-Delete">🔒</button>
                        <button class="novnc-control-btn" id="novnc-btn-fullscreen" title="Toggle Fullscreen">📺</button>
                        <button class="novnc-control-btn" id="novnc-btn-disconnect-toggle" title="Disconnect Session">❌</button>
                    </div>

                    <!-- noVNC Drawer widgets -->
                    <div class="novnc-drawer" id="novnc-drawer-kbd">
                        <h4>Virtual Keyboard</h4>
                        <div style="font-size:10px; color:#aaa; margin-bottom:8px;">Choose keyboard layout:</div>
                        <select class="form-input" style="height:30px; font-size:11px; padding:0 5px; background:#222; border-color:#444;">
                            <option value="us">English (US)</option>
                            <option value="uk">English (UK)</option>
                            <option value="de">German</option>
                            <option value="fr">French</option>
                        </select>
                        <button class="btn btn-primary" style="height:28px; font-size:11px; margin-top:10px; width:100%;" id="novnc-kbd-close">Done</button>
                    </div>

                    <div class="novnc-drawer" id="novnc-drawer-clip">
                        <h4>Sync Clipboard</h4>
                        <textarea id="novnc-clipboard-text" placeholder="Type text to send to VM..." style="width:100%; height:80px; background:#222; border:1px solid #444; color:#fff; border-radius:4px; font-size:11px; padding:5px; resize:none;"></textarea>
                        <button class="btn btn-primary" style="height:28px; font-size:11px; margin-top:5px; width:100%;" id="novnc-clip-sync">Send to VM</button>
                    </div>

                    <div class="novnc-drawer" id="novnc-drawer-cdrom">
                        <h4>CD-ROM Media</h4>
                        <div style="font-size:10px; color:#aaa; margin-bottom:8px;">Select ISO to mount:</div>
                        <select class="form-input" id="novnc-cdrom-select" style="height:30px; font-size:11px; padding:0 5px; background:#222; border-color:#444;">
                            <option value="">Empty Drive</option>
                        </select>
                        <button class="btn btn-primary" style="height:28px; font-size:11px; margin-top:10px; width:100%;" id="novnc-cdrom-mount">Apply</button>
                    </div>

                    <div class="novnc-drawer" id="novnc-drawer-power">
                        <h4>Power Control</h4>
                        <button class="btn btn-secondary" style="height:28px; font-size:11px; margin-bottom:5px; width:100%; text-align:left;" id="novnc-power-start">⚡ Power On</button>
                        <button class="btn btn-secondary" style="height:28px; font-size:11px; margin-bottom:5px; width:100%; text-align:left;" id="novnc-power-shutdown">🔌 Soft Shutdown</button>
                        <button class="btn btn-secondary" style="height:28px; font-size:11px; margin-bottom:5px; width:100%; text-align:left; color:var(--color-danger);" id="novnc-power-stop">🛑 Power Off (Force)</button>
                        <button class="btn btn-secondary" style="height:28px; font-size:11px; margin-bottom:5px; width:100%; text-align:left;" id="novnc-power-reboot">🔄 Soft Reboot</button>
                        <button class="btn btn-secondary" style="height:28px; font-size:11px; width:100%; text-align:left; color:var(--color-warning);" id="novnc-power-reset">⚡ Reset (Force)</button>
                    </div>

                    <!-- noVNC main screen area -->
                    <div class="novnc-workspace">
                        <div class="novnc-header">
                            <div style="display:flex; align-items:center; gap:8px;">
                                <span class="status-dot-inline" style="background-color: var(--color-primary); width: 8px; height: 8px; border-radius: 50%; display:inline-block;" id="vnc-header-status-dot"></span>
                                <h3>noVNC console: <span id="vnc-header-vm-name">${vmName}</span></h3>
                            </div>
                            <div style="display:flex; align-items:center; gap:12px;">
                                <span class="novnc-status-badge" id="vnc-status-badge" style="background:rgba(255,255,255,0.05); color:#aaa;">Connecting...</span>
                                <button class="btn-close-console" id="btn-close-console" style="font-size:24px; line-height:1; cursor:pointer; background:none; border:none; color:var(--text-muted);">&times;</button>
                            </div>
                        </div>

                        <div class="novnc-screen" id="novnc-screen">
                            <!-- Loading / Handshake state -->
                            <div class="novnc-loading-overlay" id="novnc-loading-overlay">
                                <div class="novnc-spinner"></div>
                                <div style="font-family:'Space Grotesk',sans-serif; font-size:14px; font-weight:600; color:#fff;">Connecting to VNC Console...</div>
                            </div>

                            <!-- noVNC HTML5 Graphics Canvas Container -->
                            <div id="novnc-canvas-container"></div>

                            <!-- Disconnected / Power Off state -->
                            <div class="novnc-loading-overlay" id="novnc-disconnected-overlay" style="display:none; background:#040507; color:var(--text-muted);">
                                <svg viewBox="0 0 24 24" style="width:48px; height:48px; fill:var(--color-danger); opacity:0.6;"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-2h2v2zm0-4h-2V7h2v6z"/></svg>
                                <div style="font-family:'Space Grotesk',sans-serif; font-size:15px; font-weight:600; color:#fff; margin-top:10px;" id="vnc-disconnect-reason">VM is Powered Off</div>
                                <div style="font-size:12px; margin-top:5px; text-align:center; max-width:300px;">Please ensure the virtual machine is running to attach the VNC graphics broker.</div>
                                <button class="btn btn-primary" style="height:32px; font-size:11px; margin-top:15px;" id="novnc-btn-reconnect">Reconnect VNC</button>
                            </div>
                        </div>
                    </div>
                </div>
            `;
            document.body.appendChild(consoleModal);
            
            // Wire up event listeners
            document.getElementById('btn-close-console').addEventListener('click', () => {
                if (activeRfb) {
                    const rfbRef = activeRfb;
                    activeRfb = null;
                    try { rfbRef.disconnect(); } catch(e){}
                }
                consoleModal.style.display = 'none';
            });

            // Handle slide-out controls menu
            const handle = document.getElementById('novnc-bar-handle');
            const controlBar = document.getElementById('novnc-control-bar');
            handle.addEventListener('click', () => {
                controlBar.classList.toggle('open');
            });

            // Toggle VNC sub-drawers
            const drawers = ['kbd', 'clip', 'cdrom', 'power'];
            drawers.forEach(d => {
                const btn = document.getElementById(`novnc-btn-${d}`);
                if (btn) {
                    btn.addEventListener('click', (e) => {
                        const curBtn = e.currentTarget;
                        const isActive = curBtn.classList.contains('active');
                        
                        // Close all drawers
                        drawers.forEach(dr => {
                            const drawerEl = document.getElementById(`novnc-drawer-${dr}`);
                            const buttonEl = document.getElementById(`novnc-btn-${dr}`);
                            if (drawerEl) drawerEl.style.display = 'none';
                            if (buttonEl) buttonEl.classList.remove('active');
                        });

                        if (!isActive) {
                            curBtn.classList.add('active');
                            const targetDrawer = document.getElementById(`novnc-drawer-${d}`);
                            if (targetDrawer) targetDrawer.style.display = 'flex';
                            
                            if (d === 'cdrom') {
                                // Populate CD-ROM list dynamically
                                fetchCDROMImagesForSelector();
                            }
                        }
                    });
                }
            });

            function fetchCDROMImagesForSelector() {
                const select = document.getElementById('novnc-cdrom-select');
                if (!select) return;
                fetch(`${state.apiHost}/api/images`)
                    .then(res => res.json())
                    .then(data => {
                        select.innerHTML = '<option value="">Empty Drive</option>';
                        const isos = data.images || data;
                        if (Array.isArray(isos)) {
                            isos.forEach(img => {
                                if (img.type === 'iso' || img.filename.endsWith('.iso')) {
                                    const opt = document.createElement('option');
                                    opt.value = img.name;
                                    opt.textContent = img.name;
                                    select.appendChild(opt);
                                }
                            });
                        }
                    })
                    .catch(err => console.error("Error populating console CD-ROM:", err));
            }

            // Drawer inner buttons
            document.getElementById('novnc-kbd-close').addEventListener('click', () => {
                document.getElementById('novnc-drawer-kbd').style.display = 'none';
                document.getElementById('novnc-btn-kbd').classList.remove('active');
                document.getElementById('novnc-control-bar').classList.remove('open');
            });
            document.getElementById('novnc-clip-sync').addEventListener('click', () => {
                const text = document.getElementById('novnc-clipboard-text').value;
                if (activeRfb) {
                    activeRfb.sendClipboard(text);
                    showToast('VNC Clipboard', 'Synced text bytes to hypervisor guest OS queue.', 'info');
                } else {
                    showToast('Error', 'VNC session not connected.', 'danger');
                }
                document.getElementById('novnc-drawer-clip').style.display = 'none';
                document.getElementById('novnc-btn-clip').classList.remove('active');
                document.getElementById('novnc-control-bar').classList.remove('open');
            });

            // CD-ROM Mount controls
            document.getElementById('novnc-cdrom-mount').addEventListener('click', () => {
                const vmNameCur = document.getElementById('vnc-header-vm-name').textContent;
                const isoVal = document.getElementById('novnc-cdrom-select').value;
                
                fetch(`${state.apiHost}/api/vms/cdrom`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({ name: vmNameCur, iso: isoVal })
                })
                .then(res => {
                    if (!res.ok) throw new Error('CD-ROM command failed');
                    return res.json();
                })
                .then(data => {
                    showToast('CD-ROM Updated', data.message, 'success');
                    document.getElementById('novnc-drawer-cdrom').style.display = 'none';
                    document.getElementById('novnc-btn-cdrom').classList.remove('active');
                    document.getElementById('novnc-control-bar').classList.remove('open');
                })
                .catch(err => {
                    showToast('CD-ROM Error', err.message, 'danger');
                });
                setTimeout(updateTasksList, 150);
            });

            // Fullscreen controls
            const btnFullscreen = document.getElementById('novnc-btn-fullscreen');
            if (btnFullscreen) {
                btnFullscreen.addEventListener('click', () => {
                    const workspace = consoleModal.querySelector('.novnc-workspace');
                    if (!document.fullscreenElement) {
                        workspace.requestFullscreen().catch(err => {
                            console.error(`Error attempting to enable fullscreen: ${err.message}`);
                        });
                    } else {
                        document.exitFullscreen();
                    }
                });
            }

            // Power button integrations
            document.getElementById('novnc-power-start').addEventListener('click', () => {
                const vmNameCur = document.getElementById('vnc-header-vm-name').textContent;
                triggerVmPowerAction(vmNameCur, 'start');
                document.getElementById('novnc-drawer-power').style.display = 'none';
                document.getElementById('novnc-btn-power').classList.remove('active');
                setTimeout(() => runVncHandshake(vmNameCur), 800);
            });
            document.getElementById('novnc-power-shutdown').addEventListener('click', () => {
                const vmNameCur = document.getElementById('vnc-header-vm-name').textContent;
                triggerVmPowerAction(vmNameCur, 'shutdown');
                document.getElementById('novnc-drawer-power').style.display = 'none';
                document.getElementById('novnc-btn-power').classList.remove('active');
            });
            document.getElementById('novnc-power-stop').addEventListener('click', () => {
                const vmNameCur = document.getElementById('vnc-header-vm-name').textContent;
                triggerVmPowerAction(vmNameCur, 'stop');
                document.getElementById('novnc-drawer-power').style.display = 'none';
                document.getElementById('novnc-btn-power').classList.remove('active');
            });
            document.getElementById('novnc-power-reboot').addEventListener('click', () => {
                const vmNameCur = document.getElementById('vnc-header-vm-name').textContent;
                triggerVmPowerAction(vmNameCur, 'reboot');
                document.getElementById('novnc-drawer-power').style.display = 'none';
                document.getElementById('novnc-btn-power').classList.remove('active');
                setTimeout(() => runVncHandshake(vmNameCur), 800);
            });
            document.getElementById('novnc-power-reset').addEventListener('click', () => {
                const vmNameCur = document.getElementById('vnc-header-vm-name').textContent;
                triggerVmPowerAction(vmNameCur, 'reset');
                document.getElementById('novnc-drawer-power').style.display = 'none';
                document.getElementById('novnc-btn-power').classList.remove('active');
                setTimeout(() => runVncHandshake(vmNameCur), 800);
            });
            
            // CAD button integration
            const btnCad = document.getElementById('novnc-btn-cad');
            if (btnCad) {
                btnCad.addEventListener('click', () => {
                    if (activeRfb) {
                        activeRfb.sendCtrlAltDel();
                        showToast('VNC Console', 'Sent Ctrl-Alt-Delete command to guest.', 'info');
                    } else {
                        showToast('Error', 'VNC session not connected.', 'danger');
                    }
                });
            }

            // Disconnect button
            document.getElementById('novnc-btn-disconnect-toggle').addEventListener('click', () => {
                const btn = document.getElementById('novnc-btn-disconnect-toggle');
                if (btn.classList.contains('active')) {
                    // Reconnect
                    const vmNameCur = document.getElementById('vnc-header-vm-name').textContent;
                    runVncHandshake(vmNameCur);
                } else {
                    // Disconnect
                    if (activeRfb) {
                        const rfbRef = activeRfb;
                        activeRfb = null;
                        try { rfbRef.disconnect(); } catch(e){}
                    }
                    btn.classList.add('active');
                    btn.title = "Reconnect Session";
                    btn.innerHTML = "🔌";
                    showVncDisconnected("Session Disconnected", "VNC graphics connection closed by client request.");
                }
            });

            // Reconnect overlay button
            document.getElementById('novnc-btn-reconnect').addEventListener('click', () => {
                const vmNameCur = document.getElementById('vnc-header-vm-name').textContent;
                runVncHandshake(vmNameCur);
            });
        }

        consoleModal.querySelector('#vnc-header-vm-name').textContent = vmName;
        consoleModal.style.display = 'flex';
        runVncHandshake(vmName);

        // Reset sidebar state
        document.getElementById('novnc-control-bar').classList.remove('open');
        document.getElementById('novnc-btn-disconnect-toggle').classList.remove('active');
        document.getElementById('novnc-btn-disconnect-toggle').title = "Disconnect Session";
        document.getElementById('novnc-btn-disconnect-toggle').innerHTML = "❌";

        // Hide drawers
        const drawers = ['kbd', 'clip', 'cdrom', 'power'];
        drawers.forEach(dr => {
            const drawerEl = document.getElementById(`novnc-drawer-${dr}`);
            if (drawerEl) drawerEl.style.display = 'none';
            const buttonEl = document.getElementById(`novnc-btn-${dr}`);
            if (buttonEl) buttonEl.classList.remove('active');
        });

        // Run connection handshake
        runVncHandshake(vmName);
    }

    function showVncDisconnected(reason, desc) {
        if (consolePollInterval) clearInterval(consolePollInterval);
        document.getElementById('novnc-loading-overlay').style.display = 'none';
        document.getElementById('novnc-canvas-container').style.display = 'none';
        document.getElementById('novnc-disconnected-overlay').style.display = 'flex';
        document.getElementById('vnc-disconnect-reason').textContent = reason;
        document.getElementById('novnc-disconnected-overlay').querySelector('div:nth-of-type(2)').textContent = desc;
        
        const badge = document.getElementById('vnc-status-badge');
        badge.textContent = "Disconnected";
        badge.style.background = "rgba(239, 68, 68, 0.15)";
        badge.style.color = "var(--color-danger)";

        document.getElementById('vnc-header-status-dot').style.backgroundColor = "var(--color-danger)";
    }

    function runVncHandshake(vmName, isRetry = false) {
        if (!isRetry) {
            vncRetryCount = 0;
        }

        if (vncRetryTimeout) {
            clearTimeout(vncRetryTimeout);
            vncRetryTimeout = null;
        }

        if (activeRfb) {
            try { activeRfb.disconnect(); } catch(e){}
            activeRfb = null;
        }

        // Reset overlays
        document.getElementById('novnc-loading-overlay').style.display = 'flex';
        document.getElementById('novnc-canvas-container').style.display = 'none';
        document.getElementById('novnc-disconnected-overlay').style.display = 'none';

        // Set default connecting text
        const loadingOverlay = document.getElementById('novnc-loading-overlay');
        if (loadingOverlay) {
            const textEl = loadingOverlay.querySelector('div:nth-of-type(2)');
            if (textEl) {
                textEl.textContent = "Connecting to VNC Console...";
            }
        }

        const badge = document.getElementById('vnc-status-badge');
        badge.textContent = "Connecting";
        badge.style.background = "rgba(251, 191, 36, 0.15)";
        badge.style.color = "var(--color-warning)";
        document.getElementById('vnc-header-status-dot').style.backgroundColor = "var(--color-warning)";

        // Check if VM is actually running
        const targetVm = state.vms.find(v => v.name === vmName);
        const isRunning = targetVm && targetVm.status === 'running';

        if (!isRunning) {
            setTimeout(() => {
                showVncDisconnected("VM is Powered Off", `The hypervisor target virtual machine '${vmName}' is stopped. Please start the VM to establish VNC connectivity.`);
            }, 600);
            return;
        }

        fetch(`/api/vms/console/token?name=${vmName}&type=vnc&token=${getStoredToken()}`)
        .then(res => {
            if (!res.ok) throw new Error("VNC Token fetch failed: " + res.statusText);
            return res.json();
        })
        .then(data => {
            import('./novnc/rfb.js?v=1.0.18')
            .then(module => {
                const RFB = module.default;
                const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                const wsUrl = `${protocol}//${window.location.host}/api/vms/console/ws?token=${data.token}`;
                const container = document.getElementById('novnc-canvas-container');
                container.innerHTML = ''; // Clear previous canvas

                const rfbInstance = new RFB(container, wsUrl);
                activeRfb = rfbInstance;
                activeRfb.scaleViewport = false;
                activeRfb.resizeSession = true;

                activeRfb.addEventListener('connect', () => {
                    if (rfbInstance !== activeRfb) return;
                    vncRetryCount = 0; // reset retry counter
                    document.getElementById('novnc-loading-overlay').style.display = 'none';
                    document.getElementById('novnc-canvas-container').style.display = 'block';
                    
                    badge.textContent = "Connected";
                    badge.style.background = "rgba(16, 185, 129, 0.15)";
                    badge.style.color = "var(--color-success)";
                    document.getElementById('vnc-header-status-dot').style.backgroundColor = "var(--color-success)";
                });

                activeRfb.addEventListener('disconnect', (e) => {
                    if (rfbInstance !== activeRfb && activeRfb !== null) {
                        return;
                    }
                    document.getElementById('novnc-canvas-container').style.display = 'none';
                    
                    // If activeRfb was set to null prior to disconnect, it is manual
                    if (activeRfb === null) {
                        showVncDisconnected("Session Disconnected", "VNC graphics connection closed by client request.");
                        return;
                    }
                    
                    // UEFI VMs take time to boot/initialize graphics, causing early disconnects.
                    // Reconnect up to 6 times at 2s intervals.
                    if (vncRetryCount < 6) {
                        vncRetryCount++;
                        
                        badge.textContent = `Retrying (${vncRetryCount}/6)`;
                        badge.style.background = "rgba(251, 191, 36, 0.15)";
                        badge.style.color = "var(--color-warning)";
                        document.getElementById('vnc-header-status-dot').style.backgroundColor = "var(--color-warning)";
                        
                        if (loadingOverlay) {
                            loadingOverlay.style.display = 'flex';
                            const textEl = loadingOverlay.querySelector('div:nth-of-type(2)');
                            if (textEl) {
                                textEl.textContent = `Guest initializing display... Retrying VNC connection (Attempt ${vncRetryCount}/6)`;
                            }
                        }
                        
                        vncRetryTimeout = setTimeout(() => {
                            runVncHandshake(vmName, true);
                        }, 2000);
                    } else {
                        showVncDisconnected("VNC Disconnected", e.detail.clean ? "Connection closed clean. Guest has not initialized the display (yet) in UEFI mode." : "Connection failed or reset.");
                    }
                });

                activeRfb.addEventListener('credentialsrequired', () => {
                    if (rfbInstance !== activeRfb) return;
                    activeRfb.respondCredentials({ password: '' });
                });
            })
            .catch(err => {
                console.error('Failed to load HTML5 RFB library:', err);
                showVncDisconnected("Library Load Error", "Could not load local noVNC dependencies: " + err + (err && err.stack ? " \nStack: " + err.stack : ""));
            });
        })
        .catch(err => {
            console.error('Failed to fetch VNC token:', err);
            showVncDisconnected("Connection Error", "Could not fetch target console token: " + err.message);
        });
    }

    // Render VMs Table content with DOM Reconciliation
    function updateVmsTable() {
        if (!vmsTableBody) return;
        
        const filterText = vmSearch ? vmSearch.value.toLowerCase().trim() : '';
        const filteredVms = state.vms.filter(vm => vm.name.toLowerCase().includes(filterText));

        // Update VM sidebar summaries if present
        let runningCount = 0;
        let stoppedCount = 0;
        state.vms.forEach(v => {
            if (v.status === 'running') runningCount++;
            else stoppedCount++;
        });
        
        if (sidebarTotalVms) sidebarTotalVms.textContent = `${state.vms.length} VMs`;
        if (sidebarRunningVms) sidebarRunningVms.textContent = `${runningCount} Running`;
        if (sidebarStoppedVms) sidebarStoppedVms.textContent = `${stoppedCount} Stopped`;

        if (filteredVms.length === 0) {
            vmsTableBody.innerHTML = `
                <tr>
                    <td colspan="9" class="table-loading">No active virtual machines found.</td>
                </tr>
            `;
            return;
        }

        // Remove loading row if present
        const loadingRow = vmsTableBody.querySelector('.table-loading');
        if (loadingRow) {
            vmsTableBody.innerHTML = '';
        }

        const existingRows = Array.from(vmsTableBody.querySelectorAll('tr[data-vm-name]'));
        const existingRowMap = {};
        existingRows.forEach(row => {
            existingRowMap[row.getAttribute('data-vm-name')] = row;
        });

        const renderedVmNames = new Set();

        filteredVms.forEach(vm => {
            const isRunning = vm.status === 'running';
            renderedVmNames.add(vm.name);
            
            let actionBtn = '';
            if (isRunning) {
                actionBtn = `
                    <button class="table-action-btn console-btn" data-name="${vm.name}" style="margin-right:5px; border-color:var(--color-primary); color:var(--color-primary);">
                        <svg viewBox="0 0 24 24" class="action-icon" style="fill:var(--color-primary);"><path d="M21 2H3c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h7l-2 3v1h8v-1l-2-3h7c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2zm0 14H3V4h18v12z"/></svg> Console
                    </button>
                    <button class="table-action-btn webgl-btn" data-name="${vm.name}" style="margin-right:5px; border-color:#0891b2; color:#0891b2;">
                        <svg viewBox="0 0 24 24" class="action-icon" style="fill:#0891b2;"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-1 17.93c-3.95-.49-7-3.85-7-7.93 0-.62.08-1.21.21-1.79L9 15v1c0 1.1.9 2 2 2v1.93zm6.9-2.53c-.26-.81-1-1.4-1.9-1.4h-1v-3c0-.55-.45-1-1-1h-6v-2h2c.55 0 1-.45 1-1V7h2c1.1 0 2-.9 2-2v-.41c2.93 1.19 5 4.06 5 7.41 0 2.08-.8 3.97-2.1 5.4z"/></svg> WebGL Console
                    </button>
                    <button class="table-action-btn edit-btn" data-name="${vm.name}" style="margin-right:5px; border-color:var(--color-primary); color:var(--color-primary);">
                        <svg viewBox="0 0 24 24" class="action-icon" style="fill:var(--color-primary);"><path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04c.39-.39.39-1.02 0-1.41l-2.34-2.34c-.39-.39-1.02-.39-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z"/></svg> Edit
                    </button>
                    <button class="table-action-btn power-btn power-btn-stop" data-name="${vm.name}" data-status="running" style="margin-right:5px;">
                        <svg viewBox="0 0 24 24" class="action-icon"><path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/></svg> Stop
                    </button>
                `;
            } else {
                actionBtn = `
                    <button class="table-action-btn power-btn" data-name="${vm.name}" data-status="stopped" style="margin-right:5px;">
                        <svg viewBox="0 0 24 24" class="action-icon"><path d="M8 5v14l11-7z"/></svg> Start
                    </button>
                    <button class="table-action-btn edit-btn" data-name="${vm.name}" style="margin-right:5px; border-color:var(--color-primary); color:var(--color-primary);">
                        <svg viewBox="0 0 24 24" class="action-icon" style="fill:var(--color-primary);"><path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04c.39-.39.39-1.02 0-1.41l-2.34-2.34c-.39-.39-1.02-.39-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z"/></svg> Edit
                    </button>
                    <button class="table-action-btn delete-btn" data-name="${vm.name}" style="color:var(--color-danger); border-color:var(--color-danger);">
                        <svg viewBox="0 0 24 24" class="action-icon" style="fill:var(--color-danger);"><path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/></svg> Delete
                    </button>
                `;
            }

            let vmCpu = '---';
            let vmMemStr = '---';
            let vmIopsLatStr = '---';

            if (isRunning) {
                if (vm.cpu_usage_pct !== undefined && vm.cpu_usage_pct !== null) {
                    vmCpu = `${vm.cpu_usage_pct.toFixed(1)}%`;
                } else {
                    vmCpu = '0.0%';
                }

                if (vm.mem_usage_mb !== undefined && vm.mem_usage_mb !== null) {
                    const pctStr = vm.mem_usage_pct !== undefined && vm.mem_usage_pct !== null ? ` (${vm.mem_usage_pct.toFixed(1)}%)` : '';
                    vmMemStr = `${Math.round(vm.mem_usage_mb)} MB${pctStr}`;
                } else {
                    vmMemStr = '0 MB (0.0%)';
                }

                if (vm.iops !== undefined && vm.iops !== null) {
                    const latVal = vm.latency_ms !== undefined && vm.latency_ms !== null ? vm.latency_ms.toFixed(2) : '0.00';
                    vmIopsLatStr = `${Math.round(vm.iops)} IOPS / ${latVal} ms`;
                } else {
                    vmIopsLatStr = '0 IOPS / 0.00 ms';
                }
            }

            const trHtml = `
                <td><strong>${vm.name}</strong></td>
                <td>
                    <span class="status-indicator ${isRunning ? 'status-green' : 'status-red'}">
                        ${isRunning ? 'ON' : 'OFF'}
                    </span>
                </td>
                <td>${formatNodeName(vm.node)}</td>
                <td>${vm.vcpus} vCPU / ${(vm.memory / 1024).toFixed(1)} GB</td>
                <td>${vm.disk} GB</td>
                <td>${vmCpu}</td>
                <td>${vmMemStr}</td>
                <td>${vmIopsLatStr}</td>
                <td style="text-align: right;">
                    <div class="vm-actions-cell" style="display:flex; justify-content:flex-end;">
                        ${actionBtn}
                    </div>
                </td>
            `;

            let tr = existingRowMap[vm.name];
            if (!tr) {
                tr = document.createElement('tr');
                tr.setAttribute('data-vm-name', vm.name);
                tr.setAttribute('data-vm-status', vm.status);
                tr.innerHTML = trHtml;
                vmsTableBody.appendChild(tr);
                bindRowButtons(tr, vm.name);
            } else {
                const prevStatus = tr.getAttribute('data-vm-status');
                if (prevStatus !== vm.status) {
                    tr.setAttribute('data-vm-status', vm.status);
                    tr.innerHTML = trHtml;
                    bindRowButtons(tr, vm.name);
                } else {
                    const cells = tr.cells;
                    if (cells.length >= 9) {
                        const nodeText = formatNodeName(vm.node);
                        const hwText = `${vm.vcpus} vCPU / ${(vm.memory / 1024).toFixed(1)} GB`;
                        
                        const isTableHovered = vmsTableBody.matches(':hover');
                        if (!isTableHovered) {
                            if (cells[2].textContent !== nodeText) cells[2].textContent = nodeText;
                            if (cells[3].textContent !== hwText) cells[3].textContent = hwText;
                            if (cells[5].textContent !== vmCpu) cells[5].textContent = vmCpu;
                            if (cells[6].textContent !== vmMemStr) cells[6].textContent = vmMemStr;
                            if (cells[7].textContent !== vmIopsLatStr) cells[7].textContent = vmIopsLatStr;
                        }
                    }
                }
            }
        });

        // Remove deleted rows
        existingRows.forEach(row => {
            const rowVmName = row.getAttribute('data-vm-name');
            if (!renderedVmNames.has(rowVmName)) {
                row.remove();
            }
        });
    }

    function bindRowButtons(row, vmName) {
        const consoleBtn = row.querySelector('.console-btn');
        if (consoleBtn) {
            consoleBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                e.preventDefault();
                openVmConsole(vmName);
            });
        }
        
        const webglBtn = row.querySelector('.webgl-btn');
        if (webglBtn) {
            webglBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                e.preventDefault();
                const url = `vnc_auto.html?name=${vmName}`;
                window.open(url, '_blank');
            });
        }
        
        const editBtn = row.querySelector('.edit-btn');
        if (editBtn) {
            editBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                e.preventDefault();
                openEditModal(vmName);
            });
        }
        
        const deleteBtn = row.querySelector('.delete-btn');
        if (deleteBtn) {
            deleteBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                e.preventDefault();
                if (confirm(`Are you sure you want to permanently delete virtual machine "${vmName}"?`)) {
                    deleteBtn.disabled = true;
                    fetch(`${state.apiHost}/api/vms/delete`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ name: vmName })
                    })
                    .then(res => {
                        if (!res.ok) return res.json().then(e => { throw new Error(e.error || 'Failed to delete VM') });
                        return res.json();
                    })
                    .then(data => {
                        // showToast('VM Deleted', `VM "${vmName}" deleted successfully.`, 'success');
                        state.vms = state.vms.filter(v => v.name !== vmName);
                        updateVmsTable();
                    })
                    .catch(err => {
                        showToast('Deletion Failed', err.message, 'danger');
                        deleteBtn.disabled = false;
                    });
                    setTimeout(updateTasksList, 150);
                }
            });
        }
        
        const powerBtn = row.querySelector('.power-btn');
        if (powerBtn) {
            powerBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                e.preventDefault();
                const status = powerBtn.getAttribute('data-status');
                toggleVmPower(vmName, status);
            });
        }
    }



    function updateStorageContainersTable() {
        const tbody = document.getElementById('storage-containers-table-body');
        if (!tbody) return;

        fetch(`${state.apiHost}/api/storage/containers`)
        .then(res => res.json())
        .then(data => {
            const list = data.containers || [];
            tbody.innerHTML = '';
            if (list.length === 0) {
                tbody.innerHTML = `
                    <tr>
                        <td colspan="5" class="table-loading">No storage containers configured.</td>
                    </tr>
                `;
                return;
            }
            list.forEach(c => {
                const tr = document.createElement('tr');
                const isDefault = c.name === 'default-vm-container' || c.name === 'default-image-container';
                const quotaStr = c.quota_bytes > 0 ? `${(c.quota_bytes / (1024**3)).toFixed(1)} GB` : 'Unlimited';
                
                let actions = '';
                if (isDefault) {
                    actions = `<button class="table-action-btn edit-container-btn" data-name="${c.name}">Edit</button>`;
                } else {
                    actions = `
                        <button class="table-action-btn edit-container-btn" data-name="${c.name}" style="margin-right: 5px;">Edit</button>
                        <button class="table-action-btn delete-container-btn" data-name="${c.name}" style="color: var(--color-danger); border-color: var(--color-danger);">Delete</button>
                    `;
                }

                tr.innerHTML = `
                    <td><strong>${c.name}</strong></td>
                    <td><span class="node-role-badge leader">${c.tier.toUpperCase()}</span></td>
                    <td>${quotaStr}</td>
                    <td><code>${c.path || 'undefined'}</code></td>
                    <td style="text-align: right;">
                        <div class="vm-actions-cell" style="display:flex; justify-content:flex-end;">
                            ${actions}
                        </div>
                    </td>
                `;
                tbody.appendChild(tr);
            });

            // Bind edit/delete handlers
            tbody.querySelectorAll('.edit-container-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    const name = btn.getAttribute('data-name');
                    const container = list.find(x => x.name === name);
                    if (container) {
                        openEditContainerModal(container);
                    }
                });
            });

            tbody.querySelectorAll('.delete-container-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    const name = btn.getAttribute('data-name');
                    if (confirm(`Are you sure you want to permanently delete storage container "${name}"? This will also stop and delete the associated Aether storage volume.`)) {
                        btn.disabled = true;
                        fetch(`${state.apiHost}/api/storage/containers/delete`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ name: name })
                        })
                        .then(res => {
                            if (!res.ok) return res.json().then(e => { throw new Error(e.error || 'Failed to delete container') });
                            return res.json();
                        })
                        .then(() => {
                            showToast('Container Deleted', `Storage container "${name}" deleted successfully.`, 'success');
                            updateStorageContainersTable();
                        })
                        .catch(err => {
                            showToast('Deletion Failed', err.message, 'error');
                            btn.disabled = false;
                        });
                    }
                });
            });
        })
        .catch(err => {
            tbody.innerHTML = `
                <tr>
                    <td colspan="6" class="table-loading" style="color: var(--color-danger);">Failed to query containers: ${err.message}</td>
                </tr>
            `;
        });
    }

    function updateStorageVirtualDisksTable() {
        const tbody = document.getElementById('storage-virtual-disks-table-body');
        if (!tbody) return;

        fetch(`${state.apiHost}/api/storage/disks`)
        .then(res => res.json())
        .then(data => {
            const list = data.disks || [];
            tbody.innerHTML = '';
            if (list.length === 0) {
                tbody.innerHTML = `
                    <tr>
                        <td colspan="5" class="table-loading">No logical volumes allocated.</td>
                    </tr>
                `;
                return;
            }
            list.forEach(d => {
                const dateStr = d.timestamp ? new Date(d.timestamp * 1000).toLocaleString() : 'N/A';
                const tr = document.createElement('tr');
                tr.innerHTML = `
                    <td><strong>${d.name}</strong></td>
                    <td><code>${d.container}</code></td>
                    <td><span class="status-indicator status-green">${d.size}</span></td>
                    <td><code style="font-size:11px;">${d.disk_path}</code></td>
                    <td>${dateStr}</td>
                `;
                tbody.appendChild(tr);
            });
        })
        .catch(err => {
            tbody.innerHTML = `
                <tr>
                    <td colspan="5" class="table-loading" style="color: var(--color-danger);">Failed to query virtual disks: ${err.message}</td>
                </tr>
            `;
        });
    }



    // Table Search Filter if present
    if (vmSearch) {
        vmSearch.addEventListener('input', updateVmsTable);
    }

    // Events log is updated dynamically from status polls

    // ----------------------------------------------------
    // Settings Page Forms & Themes Logic
    // ----------------------------------------------------
    const settingsDnsForm = document.getElementById('settings-dns-form');
    const settingsNtpForm = document.getElementById('settings-ntp-form');
    const themeCards = document.querySelectorAll('.theme-card');

    // Set active card based on local storage theme
    let activeTheme = 'sapphire';
    try {
        activeTheme = localStorage.getItem('helios-theme') || 'sapphire';
    } catch (e) {}
    themeCards.forEach(card => {
        if (card.getAttribute('data-theme') === activeTheme) {
            card.classList.add('active');
        } else {
            card.classList.remove('active');
        }

        card.addEventListener('click', () => {
            const selectedTheme = card.getAttribute('data-theme');
            
            // Remove previous theme classes
            document.documentElement.classList.remove('theme-sapphire', 'theme-emerald', 'theme-ruby', 'theme-amethyst', 'theme-light', 'theme-grey', 'theme-sunset', 'theme-ocean', 'theme-midnight', 'theme-forest', 'theme-cyberpunk', 'theme-nord', 'theme-dracula');
            
            // Add new class if not default (sapphire)
            if (selectedTheme !== 'sapphire') {
                document.documentElement.classList.add('theme-' + selectedTheme);
            }
            
            // Update active card states
            themeCards.forEach(c => c.classList.remove('active'));
            card.classList.add('active');
            
            // Save settings
            try {
                localStorage.setItem('helios-theme', selectedTheme);
            } catch (e) {}
            showToast('Theme Updated', `Switched dashboard style to ${selectedTheme} theme.`, 'success');
        });
    });

    // Storage Container Drawer Modals
    const createContainerOverlay = document.getElementById('create-container-overlay');
    const btnOpenCreateContainerModal = document.getElementById('btn-open-create-container-modal');
    const btnCloseCreateContainerModal = document.getElementById('btn-close-create-container-modal');
    const btnCancelContainer = document.getElementById('btn-cancel-container');
    const btnSubmitContainer = document.getElementById('btn-submit-container');
    const createContainerForm = document.getElementById('create-container-form');
    const containerModalTitle = document.getElementById('container-modal-title');
    
    // Inputs
    const inputContainerName = document.getElementById('container-name');
    const selectContainerTier = document.getElementById('container-tier');
    const inputContainerQuota = document.getElementById('container-quota');
    
    let isEditingContainer = false;

    if (btnOpenCreateContainerModal) {
        btnOpenCreateContainerModal.addEventListener('click', () => {
            isEditingContainer = false;
            if (containerModalTitle) containerModalTitle.textContent = 'Create Storage Container';
            if (inputContainerName) {
                inputContainerName.disabled = false;
                inputContainerName.value = '';
            }
            if (selectContainerTier) {
                selectContainerTier.disabled = false;
                
                // Auto-detect default tier based on physical disks
                let hasSsd = false;
                const physicalItems = document.querySelectorAll('.physical-disk-item');
                physicalItems.forEach(item => {
                    if (item.innerText.toLowerCase().includes('ssd')) {
                        hasSsd = true;
                    }
                });
                selectContainerTier.value = hasSsd ? 'SSD' : 'HDD';
            }
            if (inputContainerQuota) inputContainerQuota.value = '0';
            if (createContainerOverlay) createContainerOverlay.classList.add('open');
        });
    }

    function closeContainerModal() {
        if (createContainerOverlay) createContainerOverlay.classList.remove('open');
    }

    if (btnCloseCreateContainerModal) btnCloseCreateContainerModal.addEventListener('click', closeContainerModal);
    if (btnCancelContainer) btnCancelContainer.addEventListener('click', closeContainerModal);

    function openEditContainerModal(container) {
        isEditingContainer = true;
        if (containerModalTitle) containerModalTitle.textContent = 'Edit Storage Container';
        if (inputContainerName) {
            inputContainerName.value = container.name;
            inputContainerName.disabled = true;
        }
        if (selectContainerTier) {
            selectContainerTier.value = container.tier.toUpperCase();
            selectContainerTier.disabled = true;
        }
        if (inputContainerQuota) {
            // quota_bytes to GB
            inputContainerQuota.value = container.quota_bytes > 0 ? Math.round(container.quota_bytes / (1024**3)) : 0;
        }
        if (createContainerOverlay) createContainerOverlay.classList.add('open');
    }

    if (btnSubmitContainer) {
        btnSubmitContainer.addEventListener('click', () => {
            const name = inputContainerName.value.trim();
            const tier = selectContainerTier.value;
            const quotaGb = parseFloat(inputContainerQuota.value) || 0;
            const quotaBytes = quotaGb > 0 ? Math.round(quotaGb * (1024**3)) : 0;

            if (!name) {
                showToast('Validation Error', 'Container Name is required.', 'error');
                inputContainerName.focus();
                return;
            }

            btnSubmitContainer.disabled = true;
            btnSubmitContainer.textContent = 'Saving...';

            const url = isEditingContainer 
                ? `${state.apiHost}/api/storage/containers/update`
                : `${state.apiHost}/api/storage/containers/create`;

            const payload = isEditingContainer
                ? { name, quota_bytes: quotaBytes }
                : { name, tier, quota_bytes: quotaBytes, ftt: 1 };

            fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            })
            .then(res => {
                if (!res.ok) return res.json().then(e => { throw new Error(e.error || 'Failed to save container') });
                return res.json();
            })
            .then(data => {
                showToast(isEditingContainer ? 'Container Updated' : 'Container Created', `Storage container '${name}' successfully saved.`, 'success');
                closeContainerModal();
                updateStorageContainersTable();
            })
            .catch(err => {
                showToast('Action Failed', err.message, 'error');
            })
            .finally(() => {
                btnSubmitContainer.disabled = false;
                btnSubmitContainer.textContent = 'Save Container';
            });
        });
    }

    function ensureHaMarker(barId) {
        const bar = document.getElementById(barId);
        if (!bar) return;
        const container = bar.parentElement;
        if (!container) return;
        
        let marker = container.querySelector('.ha-limit-marker');
        if (!marker) {
            marker = document.createElement('div');
            marker.className = 'ha-limit-marker';
            container.appendChild(marker);
        }
        
        const nodeCount = (state.nodes && state.nodes.length > 0) ? state.nodes.length : 3;
        const haLimit = ((nodeCount - 1) / nodeCount) * 100;
        marker.style.left = `${haLimit.toFixed(2)}%`;
        marker.title = `HA Safe Limit: ${haLimit.toFixed(1)}% (Based on ${nodeCount}-node topology)`;
    }

    // ----------------------------------------------------
    // Mimir Diagnostics Frontend Logic
    // ----------------------------------------------------
    function getCheckCategory(checkName) {
        const storageChecks = [
            'aether_storage_pools', 'aether_storage_pools_space', 'aether_volume', 'aether_peers', 
            'aether_heal_pending', 'aether_split_brain', 'storage_capacity', 
            'storage_mount_options', 'storage_volume_writable', 'fstab_safety_check',
            'orphaned_disks_check'
        ];
        const hardwareChecks = [
            'cpu_load', 'disk_space', 'ram_usage', 'host_virtualization', 
            'hostname_resolution', 'dns_ntp_sync_check', 'ntp_sync', 
            'mtls_cert_expiration', 'security_config_audit', 'firmware_upgrades', 
            'auth_seeding_check', 'maintenance_mode_check', 'libvirt_responsiveness',
            'spectrum_privilege_check'
        ];
        if (storageChecks.includes(checkName)) {
            return 'storage';
        } else if (hardwareChecks.includes(checkName)) {
            return 'hardware';
        } else {
            return 'services';
        }
    }

    function formatNodeNameByIp(ip) {
        if (state.nodes) {
            const found = state.nodes.find(n => n.ip === ip);
            if (found) return found.name;
        }
        return ip;
    }

    function fetchMimirResults() {
        fetch(`${state.apiHost}/api/mimir/results`)
        .then(res => res.json())
        .then(data => {
            const results = data.results || [];
            state.mimirRawResults = results;
            
            // Auto-open log check from URL query params
            const urlParams = new URLSearchParams(window.location.search);
            const autoCheck = urlParams.get('check');
            const autoNode = urlParams.get('node');
            if (autoCheck) {
                // Ensure the check's status is enabled in filters
                const targetCheck = results.find(r => r.check_name === autoCheck && (!autoNode || r.node_ip === autoNode));
                if (targetCheck) {
                    if (targetCheck.status === 'FAIL') {
                        const cb = document.getElementById('filter-health-critical');
                        if (cb && !cb.checked) { cb.checked = true; }
                    } else if (targetCheck.status === 'WARN') {
                        const cb = document.getElementById('filter-health-warning');
                        if (cb && !cb.checked) { cb.checked = true; }
                    } else if (targetCheck.status === 'PASS') {
                        const cb = document.getElementById('filter-health-pass');
                        if (cb && !cb.checked) { cb.checked = true; }
                    }
                }
            }
            
            applyMimirFiltersAndRender();

            if (autoCheck) {
                setTimeout(() => {
                    const btn = document.querySelector(`.view-log-btn[data-check="${autoCheck}"]`);
                    let matchedBtn = autoNode ? document.querySelector(`.view-log-btn[data-check="${autoCheck}"][data-node="${autoNode}"]`) : btn;
                    
                    if (!matchedBtn) {
                        matchedBtn = btn;
                    }
                    
                    if (matchedBtn) {
                        matchedBtn.scrollIntoView({ behavior: 'smooth', block: 'center' });
                        matchedBtn.click();
                    }
                    // Clean up URL query parameters
                    window.history.replaceState({}, document.title, window.location.pathname);
                }, 200);
            }
        })
        .catch(err => console.error('Failed to load Mimir results:', err));
        
        fetch(`${state.apiHost}/api/mimir/schedules`)
        .then(res => res.json())
        .then(data => {
            const list = data.schedules || [];
            const hourly = list.find(s => s.schedule_name === 'hourly_checks');
            if (hourly) {
                const label = document.getElementById('mimir-schedule-policy-label');
                if (label) {
                    label.textContent = hourly.enabled ? 'Hourly' : 'Disabled';
                }
            }
        })
        .catch(err => console.error('Failed to load Mimir schedules:', err));
    }

    function applyMimirFiltersAndRender() {
        if (!state.mimirRawResults) return;
        
        const showCritical = document.getElementById('filter-health-critical')?.checked ?? true;
        const showWarning = document.getElementById('filter-health-warning')?.checked ?? true;
        const showPass = document.getElementById('filter-health-pass')?.checked ?? false;
        
        const filteredResults = state.mimirRawResults.filter(r => {
            if (r.status === 'FAIL') return showCritical;
            if (r.status === 'WARN') return showWarning;
            if (r.status === 'PASS') return showPass;
            return true;
        });
        
        const services = filteredResults.filter(r => getCheckCategory(r.check_name) === 'services');
        const storage = filteredResults.filter(r => getCheckCategory(r.check_name) === 'storage');
        const hardware = filteredResults.filter(r => getCheckCategory(r.check_name) === 'hardware');
        
        renderServicesGrid('mimir-services-grid', services);
        renderMimirTable('mimir-storage-tbody', storage);
        renderMimirTable('mimir-hardware-tbody', hardware);
        
        updateMimirSummary(state.mimirRawResults);
    }

    function renderServicesGrid(containerId, list) {
        const container = document.getElementById(containerId);
        if (!container) return;
        
        if (list.length === 0) {
            if (state.mimirRawResults && state.mimirRawResults.length > 0) {
                container.innerHTML = `
                    <div style="grid-column: 1 / -1; text-align: center; color: var(--color-success); font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 500; padding: 30px;">
                        <svg viewBox="0 0 24 24" style="width: 32px; height: 32px; fill: none; stroke: currentColor; stroke-width: 2; margin-bottom: 8px; color: var(--color-success); display: inline-block;"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path><polyline points="22 4 12 14.01 9 11.01"></polyline></svg>
                        <div>All service checks passed! No warnings or critical errors detected.</div>
                    </div>
                `;
            } else {
                container.innerHTML = `
                    <div class="table-loading">No service diagnostics recorded. Click "Run Diagnostics Now" to start.</div>
                `;
            }
            return;
        }
        
        const nodeIps = [...new Set(list.map(r => r.node_ip))];
        container.innerHTML = '';
        
        const serviceChecks = [
            'spark-daemon_status',
            'zookeeper_status',
            'hydra-db_status',
            'daruk_status',
            'aether_status',
            'spectrum_status',
            'catalyst_status',
            'bifrost_status',
            'dagur_status',
            'mimir_status',
            'vali_status',
            'gatoway_status',
            'urbosa_status',
            'logos_status',
            'mipha_status',
            'agahnim_status',
            'slate_status',
            'libvirtd_status'
        ];
        
        const serviceFriendlyNames = {
            'spark-daemon_status': 'Spark',
            'zookeeper_status': 'ZooKeeper',
            'hydra-db_status': 'Hydra DB',
            'daruk_status': 'Daruk DB Proxy',
            'aether_status': 'Aether Engine',
            'spectrum_status': 'Spectrum Web',
            'catalyst_status': 'Catalyst Task',
            'bifrost_status': 'Bifrost VIP',
            'dagur_status': 'Dagur Cron',
            'mimir_status': 'Mimir Health',
            'vali_status': 'Vali DRS',
            'gatoway_status': 'Gatoway Sync',
            'urbosa_status': 'Urbosa SDN',
            'logos_status': 'Logos Metrics',
            'mipha_status': 'Mipha HA',
            'agahnim_status': 'Agahnim Proxy',
            'slate_status': 'Slate Ingress',
            'libvirtd_status': 'Libvirtd'
        };
        
        nodeIps.forEach(ip => {
            const nodeName = formatNodeNameByIp(ip);
            const card = document.createElement('div');
            card.className = 'node-service-card glass-card';
            
            let headerHtml = `
                <div class="node-service-card-header">
                    <span class="node-service-card-title">${nodeName}</span>
                    <span class="node-service-card-ip">${ip}</span>
                </div>
            `;
            
            const nodeChecks = list.filter(r => r.node_ip === ip);
            if (!state.activeServiceLogs) state.activeServiceLogs = {};
            const activeSvc = state.activeServiceLogs[ip];
            
            let pillsHtml = '<div class="services-pills-grid">';
            serviceChecks.forEach(svc => {
                const check = nodeChecks.find(r => r.check_name === svc);
                const label = serviceFriendlyNames[svc];
                
                let dotClass = 'pending';
                let statusText = 'PENDING';
                let output = 'No logs available.';
                
                if (check) {
                    output = check.output || 'No output log.';
                    if (check.status === 'PASS') {
                        dotClass = 'pass';
                        statusText = 'PASS';
                    } else if (check.status === 'WARN') {
                        dotClass = 'warn';
                        statusText = 'WARN';
                    } else {
                        dotClass = 'fail';
                        statusText = 'FAIL';
                    }
                }
                
                const isActive = svc === activeSvc;
                pillsHtml += `
                    <div class="service-pill view-log-btn ${isActive ? 'active-pill' : ''}" data-check="${svc}" data-node="${ip}" data-name="${label} (${nodeName})" data-log="${encodeURIComponent(output)}">
                        <div class="service-pill-left">
                            <span class="service-pill-dot ${dotClass}"></span>
                            <span>${label}</span>
                        </div>
                        <span class="service-pill-logs-btn">LOGS</span>
                    </div>
                `;
            });
            pillsHtml += '</div>';
            
            const otherChecks = nodeChecks.filter(r => !serviceChecks.includes(r.check_name));
            let otherHtml = '';
            if (otherChecks.length > 0) {
                otherHtml = '<div class="other-checks-list">';
                otherHtml += '<div style="font-size:10px; text-transform:uppercase; letter-spacing:0.5px; color:var(--text-muted); margin-bottom:5px;">Other Checks</div>';
                otherChecks.forEach(c => {
                    let statusBadge = '';
                    if (c.status === 'PASS') {
                        statusBadge = '<span class="status-indicator status-green" style="font-size: 9px; padding: 1px 4px; line-height:1;">PASS</span>';
                    } else if (c.status === 'WARN') {
                        statusBadge = '<span class="status-indicator status-yellow" style="color:var(--color-warning); border-color:var(--color-warning); font-size: 9px; padding: 1px 4px; line-height:1;">WARN</span>';
                    } else {
                        statusBadge = '<span class="status-indicator status-red" style="font-size: 9px; padding: 1px 4px; line-height:1;">FAIL</span>';
                    }
                    
                    const isOtherActive = c.check_name === activeSvc;
                    otherHtml += `
                        <div class="other-check-item view-log-btn ${isOtherActive ? 'active-pill' : ''}" data-check="${c.check_name}" data-node="${ip}" data-name="${c.check_name} (${nodeName})" data-log="${encodeURIComponent(c.output || '')}" style="cursor: pointer; display:flex; justify-content:space-between; align-items:center; padding: 4px 6px; border-radius: 4px; background:rgba(255,255,255,0.01); border: 1px solid rgba(255,255,255,0.02); margin-bottom:3px;">
                            <span class="other-check-name">${c.check_name}</span>
                            ${statusBadge}
                        </div>
                    `;
                });
                otherHtml += '</div>';
            }
            
            let logPanelHtml = '';
            if (activeSvc) {
                const activeCheck = nodeChecks.find(r => r.check_name === activeSvc);
                const activeLabel = serviceFriendlyNames[activeSvc] || getFriendlyCheckName(activeSvc);
                const activeOutput = activeCheck ? (activeCheck.output || 'No output log.') : 'No logs available.';
                const rawLog = encodeURIComponent(activeOutput);
                logPanelHtml = `
                    <div class="node-service-log-panel" data-active-service="${activeSvc}" style="display: block;">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px;">
                            <span class="log-panel-title" style="font-size: 11px; font-weight: 600; color: var(--text-secondary);">Logs: ${activeLabel} (${nodeName})</span>
                            <button class="btn btn-secondary btn-copy-log" data-log="${rawLog}" style="padding: 1px 6px; font-size: 9px; height: 18px;">Copy</button>
                        </div>
                        <pre class="log-panel-content">${activeOutput}</pre>
                    </div>
                `;
            }

            card.innerHTML = headerHtml + pillsHtml + otherHtml + logPanelHtml;
            container.appendChild(card);

            // Move initial active log panel right after active pill
            const initialActivePill = card.querySelector('.view-log-btn.active-pill');
            const initialLogPanel = card.querySelector('.node-service-log-panel');
            if (initialActivePill && initialLogPanel) {
                initialActivePill.parentNode.insertBefore(initialLogPanel, initialActivePill.nextSibling);
            }

            const preRenderedCopyBtn = card.querySelector('.node-service-log-panel .btn-copy-log');
            if (preRenderedCopyBtn) {
                preRenderedCopyBtn.addEventListener('click', (ev) => {
                    ev.stopPropagation();
                    const lText = decodeURIComponent(ev.target.getAttribute('data-log'));
                    navigator.clipboard.writeText(lText).then(() => {
                        showToast('Copied', 'Service logs copied to clipboard', 'info');
                    });
                });
            }

            card.querySelectorAll('.view-log-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    const svc = btn.getAttribute('data-check');
                    const checkName = btn.getAttribute('data-name');
                    const rawLog = btn.getAttribute('data-log');
                    const logText = decodeURIComponent(rawLog);
                    
                    let logPanel = card.querySelector('.node-service-log-panel');
                    if (logPanel) {
                        const currentSvc = state.activeServiceLogs[ip];
                        if (currentSvc === svc && logPanel.style.display !== 'none') {
                            logPanel.style.display = 'none';
                            btn.classList.remove('active-pill');
                            delete state.activeServiceLogs[ip];
                        } else {
                            state.activeServiceLogs[ip] = svc;
                            logPanel.setAttribute('data-active-service', svc);
                            logPanel.style.display = 'block';
                            logPanel.querySelector('.log-panel-title').textContent = `Logs: ${checkName}`;
                            logPanel.querySelector('.log-panel-content').textContent = logText;
                            logPanel.querySelector('.btn-copy-log').setAttribute('data-log', rawLog);
                            
                            // Insert logPanel directly after the clicked pill
                            btn.parentNode.insertBefore(logPanel, btn.nextSibling);
                            
                            card.querySelectorAll('.view-log-btn').forEach(p => p.classList.remove('active-pill'));
                            btn.classList.add('active-pill');
                        }
                    } else {
                        state.activeServiceLogs[ip] = svc;
                        logPanel = document.createElement('div');
                        logPanel.className = 'node-service-log-panel';
                        logPanel.setAttribute('data-active-service', svc);
                        logPanel.innerHTML = `
                            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px;">
                                <span class="log-panel-title" style="font-size: 11px; font-weight: 600; color: var(--text-secondary);">Logs: ${checkName}</span>
                                <button class="btn btn-secondary btn-copy-log" data-log="${rawLog}" style="padding: 1px 6px; font-size: 9px; height: 18px;">Copy</button>
                            </div>
                            <pre class="log-panel-content">${logText}</pre>
                        `;
                        // Insert logPanel directly after the clicked pill
                        btn.parentNode.insertBefore(logPanel, btn.nextSibling);
                        
                        card.querySelectorAll('.view-log-btn').forEach(p => p.classList.remove('active-pill'));
                        btn.classList.add('active-pill');
                        
                        logPanel.querySelector('.btn-copy-log').addEventListener('click', (ev) => {
                            ev.stopPropagation();
                            const lText = decodeURIComponent(ev.target.getAttribute('data-log'));
                            navigator.clipboard.writeText(lText).then(() => {
                                showToast('Copied', 'Service logs copied to clipboard', 'info');
                            });
                        });
                    }
                });
            });
        });
    }

    function getFriendlyCheckName(checkName) {
        const friendly = {
            'aether_heal_pending': 'Aether Heal Pending',
            'aether_split_brain': 'Aether Split Brain',
            'storage_capacity': 'Storage Capacity Check',
            'storage_mount_options': 'Storage Mount Options',
            'storage_volume_writable': 'Storage Volume Writable Check',
            'fstab_safety_check': 'Fstab Safety Check',
            'cpu_load': 'CPU Load Check',
            'disk_space': 'Host Disk Space',
            'ram_usage': 'RAM Usage Check',
            'host_virtualization': 'Host Virtualization Capability',
            'hostname_resolution': 'Hostname Resolution Check',
            'dns_ntp_sync_check': 'DNS/NTP Configuration Sync',
            'ntp_sync': 'NTP Time Sync Check',
            'mtls_cert_expiration': 'mTLS Cert Expiration Check',
            'security_config_audit': 'Security Audit Check',
            'firmware_upgrades': 'Firmware Upgrade Status',
            'auth_seeding_check': 'Auth Seeding status',
            'maintenance_mode_check': 'Maintenance Mode status',
            'libvirt_responsiveness': 'Libvirt Responsiveness',
            'spectrum_privilege_check': 'Spectrum Server Permissions',
            'virsh_power_off_check': 'Powered Off Virsh VMs Check',
            'stuck_tasks_check': 'Stuck Tasks Diagnosis',
            'orphaned_disks_check': 'Orphaned Disks (Orphan Check)'
        };
        return friendly[checkName] || checkName;
    }

    function renderMimirTable(tbodyId, list) {
        const tbody = document.getElementById(tbodyId);
        if (!tbody) return;
        
        if (list.length === 0) {
            if (state.mimirRawResults && state.mimirRawResults.length > 0) {
                tbody.innerHTML = `
                    <tr>
                        <td colspan="5" style="text-align: center; color: var(--color-success); font-family: 'Space Grotesk', sans-serif; font-size: 12px; padding: 20px;">
                            <svg viewBox="0 0 24 24" style="width: 20px; height: 20px; fill: none; stroke: currentColor; stroke-width: 2; vertical-align: middle; margin-right: 6px; color: var(--color-success); display: inline-block;"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path><polyline points="22 4 12 14.01 9 11.01"></polyline></svg>
                            All checks passed! No warnings or critical errors detected.
                        </td>
                    </tr>
                `;
            } else {
                tbody.innerHTML = `
                    <tr>
                        <td colspan="5" class="table-loading">No diagnostic results recorded. Click "Run Diagnostics Now" to start.</td>
                    </tr>
                `;
            }
            return;
        }
        
        // Group list items by check_name, status, and output to deduplicate identical tasks across nodes
        const groups = {};
        list.forEach(item => {
            const key = `${item.check_name}||${item.status}||${item.output || ''}`;
            if (!groups[key]) {
                groups[key] = [];
            }
            groups[key].push(item);
        });

        const groupedList = [];
        for (const key in groups) {
            const items = groups[key];
            const base = items[0];
            const ips = items.map(it => it.node_ip);
            const latestTimestamp = Math.max(...items.map(it => new Date(it.timestamp).getTime()));
            
            groupedList.push({
                check_name: base.check_name,
                status: base.status,
                output: base.output,
                node_ips: ips,
                timestamp: latestTimestamp
            });
        }

        tbody.innerHTML = '';
        groupedList.forEach(r => {
            const tr = document.createElement('tr');
            
            // Format node cell: show "All Nodes" if matched on all 3 nodes, or list node names
            let nodeCellText = '';
            if (r.node_ips.length === 1) {
                const nodeName = formatNodeNameByIp(r.node_ips[0]);
                nodeCellText = `${nodeName} (${r.node_ips[0]})`;
            } else if (r.node_ips.length >= 3) {
                nodeCellText = 'All Nodes';
            } else {
                nodeCellText = r.node_ips.map(ip => formatNodeNameByIp(ip)).join(', ');
            }
            
            let statusBadge = '';
            if (r.status === 'PASS') {
                statusBadge = '<span class="status-indicator status-green">PASS</span>';
            } else if (r.status === 'WARN') {
                statusBadge = '<span class="status-indicator status-yellow" style="color:var(--color-warning); border-color:var(--color-warning);">WARN</span>';
            } else {
                statusBadge = '<span class="status-indicator status-red">FAIL</span>';
            }
            
            const friendlyName = getFriendlyCheckName(r.check_name);
            const logKey = `table-${tbodyId}-${r.check_name}-${r.node_ips[0]}`;
            if (!state.expandedLogs) state.expandedLogs = new Set();
            const isExpanded = state.expandedLogs.has(logKey);

            tr.innerHTML = `
                <td><strong>${friendlyName}</strong></td>
                <td>${nodeCellText}</td>
                <td>${statusBadge}</td>
                <td>${new Date(r.timestamp).toLocaleString()}</td>
                <td style="text-align: right;">
                    <button class="table-action-btn view-log-btn" data-check="${r.check_name}" data-name="${r.check_name} (${nodeCellText})" data-log="${encodeURIComponent(r.output || '')}">${isExpanded ? 'Hide Logs' : 'View Logs'}</button>
                </td>
            `;
            tbody.appendChild(tr);

            const detailTr = document.createElement('tr');
            detailTr.className = 'mimir-detail-row';
            detailTr.id = `detail-row-${r.check_name}-${r.node_ips[0]}`;
            detailTr.style.display = isExpanded ? 'table-row' : 'none';
            const rawLog = r.output || 'No output log.';
            detailTr.innerHTML = `
                <td colspan="5" style="padding: 12px; background: rgba(0, 0, 0, 0.2); border-bottom: 1px solid rgba(255, 255, 255, 0.05);">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                        <span style="font-weight: 600; color: var(--text-secondary);">Log output for ${r.check_name} (${nodeCellText})</span>
                        <button class="btn btn-secondary btn-copy-log" data-log="${encodeURIComponent(rawLog)}" style="padding: 2px 8px; font-size: 10px; height: 20px;">Copy Logs</button>
                    </div>
                    <pre class="mimir-detail-log">${rawLog}</pre>
                </td>
            `;
            tbody.appendChild(detailTr);

            const btn = tr.querySelector('.view-log-btn');
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                if (detailTr.style.display === 'none') {
                    detailTr.style.display = 'table-row';
                    btn.textContent = 'Hide Logs';
                    state.expandedLogs.add(logKey);
                } else {
                    detailTr.style.display = 'none';
                    btn.textContent = 'View Logs';
                    state.expandedLogs.delete(logKey);
                }
            });

            detailTr.querySelector('.btn-copy-log').addEventListener('click', (ev) => {
                ev.stopPropagation();
                const logText = decodeURIComponent(ev.target.getAttribute('data-log'));
                navigator.clipboard.writeText(logText).then(() => {
                    showToast('Copied', 'Log output copied to clipboard', 'info');
                });
            });
        });
    }

    function updateMimirSummary(results) {
        const stateEl = document.getElementById('mimir-summary-state');
        const countEl = document.getElementById('mimir-summary-count');
        const timeEl = document.getElementById('mimir-summary-time');
        
        if (!stateEl || !countEl || !timeEl) return;
        
        if (results.length === 0) {
            stateEl.textContent = 'Unknown';
            stateEl.style.color = 'var(--text-muted)';
            countEl.textContent = '0 Checks';
            timeEl.textContent = 'Never';
            return;
        }
        
        const passed = results.filter(r => r.status === 'PASS').length;
        const total = results.length;
        const failed = results.filter(r => r.status === 'FAIL').length;
        
        countEl.textContent = `${passed} / ${total} Passed`;
        
        if (failed > 0) {
            stateEl.textContent = 'Critical Alert';
            stateEl.style.color = 'var(--color-danger)';
        } else if (passed < total) {
            stateEl.textContent = 'Warning';
            stateEl.style.color = 'var(--color-warning)';
        } else {
            stateEl.textContent = 'Healthy';
            stateEl.style.color = 'var(--color-success)';
        }
        
        const latestTime = Math.max(...results.map(r => new Date(r.timestamp).getTime()));
        timeEl.textContent = new Date(latestTime).toLocaleString();
    }

    function openMimirLogModal(name, log) {
        const modal = document.getElementById('mimir-log-modal');
        const title = document.getElementById('mimir-log-title');
        const content = document.getElementById('mimir-log-content');
        
        if (!modal || !title || !content) return;
        
        title.textContent = name;
        content.textContent = log;
        modal.style.display = 'flex';
        setTimeout(() => modal.classList.add('open'), 10);
    }

    // Modal close buttons
    const btnCloseMimirLog = document.getElementById('btn-close-mimir-log');
    const btnMimirLogDone = document.getElementById('btn-mimir-log-done');
    const mimirLogModal = document.getElementById('mimir-log-modal');
    if (btnCloseMimirLog) {
        btnCloseMimirLog.addEventListener('click', () => {
            if (mimirLogModal) {
                mimirLogModal.classList.remove('open');
                setTimeout(() => { mimirLogModal.style.display = 'none'; }, 200);
            }
        });
    }
    if (btnMimirLogDone) {
        btnMimirLogDone.addEventListener('click', () => {
            if (mimirLogModal) {
                mimirLogModal.classList.remove('open');
                setTimeout(() => { mimirLogModal.style.display = 'none'; }, 200);
            }
        });
    }

    // Diagnostics run button
    const btnRunMimir = document.getElementById('btn-run-mimir-checks');
    if (btnRunMimir) {
        btnRunMimir.addEventListener('click', () => {
            btnRunMimir.disabled = true;
            btnRunMimir.textContent = 'Running Diagnostics...';

            const container = document.getElementById('mimir-run-progress-container');
            const bar = document.getElementById('mimir-run-progress-bar');
            const statusText = document.getElementById('mimir-run-status');
            const percentText = document.getElementById('mimir-run-percent');

            if (container) container.style.display = 'block';
            if (bar) bar.style.width = '0%';
            if (statusText) statusText.textContent = 'Contacting nodes and starting agent probes...';
            if (percentText) percentText.textContent = '0%';

            fetch(`${state.apiHost}/api/mimir/run`, { method: 'POST' })
            .then(res => {
                if (res.status === 202) {
                    showToast('Diagnostics Triggered', 'Mimir cluster-wide diagnostics running in background.', 'success');
                    
                    const durationMs = 35000;
                    const startTime = Date.now();
                    
                    const progressInterval = setInterval(() => {
                        const elapsed = Date.now() - startTime;
                        const pct = Math.min(100, Math.floor((elapsed / durationMs) * 100));
                        
                        if (bar) bar.style.width = `${pct}%`;
                        if (percentText) percentText.textContent = `${pct}%`;
                        
                        if (statusText) {
                            if (pct < 15) {
                                statusText.textContent = 'Contacting nodes and starting agent probes...';
                            } else if (pct < 30) {
                                statusText.textContent = 'Checking systemd service states on all nodes...';
                            } else if (pct < 55) {
                                statusText.textContent = 'Querying ScyllaDB ring status and keyspace consensus...';
                            } else if (pct < 75) {
                                statusText.textContent = 'Verifying local and remote virtual machine storage mounts...';
                            } else if (pct < 92) {
                                statusText.textContent = 'Checking local virtualization hypervisor and socket activity...';
                            } else {
                                statusText.textContent = 'Consolidating cluster-wide diagnostic reports...';
                            }
                        }

                        if (elapsed >= durationMs) {
                            clearInterval(progressInterval);
                            if (statusText) statusText.textContent = 'Completed';
                            if (bar) bar.style.width = '100%';
                            if (percentText) percentText.textContent = '100%';
                            
                            setTimeout(() => {
                                if (container) container.style.display = 'none';
                                fetchMimirResults();
                                btnRunMimir.disabled = false;
                                btnRunMimir.textContent = 'Run Diagnostics Now';
                            }, 1000);
                        }
                    }, 200);
                } else {
                    throw new Error('Failed to start diagnostics');
                }
            })
            .catch(err => {
                showToast('Diagnostics Error', err.message, 'error');
                btnRunMimir.disabled = false;
                btnRunMimir.textContent = 'Run Diagnostics Now';
                if (container) container.style.display = 'none';
            });
        });
    }

    // Diagnostics schedule configuration settings form
    const settingsMimirForm = document.getElementById('settings-mimir-form');
    const selectMimirSchedule = document.getElementById('mimir-schedule-select');
    
    // Fetch initial schedule state on Settings page load
    if (selectMimirSchedule) {
        fetch(`${state.apiHost}/api/mimir/schedules`)
        .then(res => res.json())
        .then(data => {
            const list = data.schedules || [];
            const hourly = list.find(s => s.schedule_name === 'hourly_checks');
            if (hourly) {
                selectMimirSchedule.value = hourly.enabled ? 'hourly' : 'disabled';
            }
        })
        .catch(err => console.error('Failed to fetch Mimir schedule state:', err));
    }
    
    if (settingsMimirForm) {
        settingsMimirForm.addEventListener('submit', (e) => {
            e.preventDefault();
            const val = selectMimirSchedule.value;
            const enabled = (val === 'hourly' || val === 'daily');
            
            const btn = document.getElementById('btn-save-mimir-schedule');
            if (btn) btn.disabled = true;
            
            fetch(`${state.apiHost}/api/mimir/schedule/update`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    schedule_name: 'hourly_checks',
                    enabled: enabled
                })
            })
            .then(res => {
                if (!res.ok) throw new Error('Failed to save settings');
                return res.json();
            })
            .then(() => {
                showToast('Schedule Saved', `Diagnostics scheduling configured to: ${val.toUpperCase()}`, 'success');
            })
            .catch(err => {
                showToast('Save Error', err.message, 'error');
            })
            .finally(() => {
                if (btn) btn.disabled = false;
            });
        });
    }

    // Dagur central task runner scheduler logic
    const dagurSchedulesTbody = document.getElementById('dagur-schedules-tbody');
    const dagurRunsTbody = document.getElementById('dagur-runs-tbody');
    const dagurLogModal = document.getElementById('dagur-log-modal');
    const dagurLogTitle = document.getElementById('dagur-log-title');
    const dagurLogContent = document.getElementById('dagur-log-content');
    const btnCloseDagurLog = document.getElementById('btn-close-dagur-log');
    const btnDagurLogDone = document.getElementById('btn-dagur-log-done');



    function fetchDagurSchedules() {
        if (!dagurSchedulesTbody) return;
        fetch(`${state.apiHost}/api/dagur/schedules`)
        .then(res => res.json())
        .then(data => {
            const list = data.schedules || [];
            dagurSchedulesTbody.innerHTML = '';
            if (list.length === 0) {
                dagurSchedulesTbody.innerHTML = '<tr><td colspan="6" class="text-center">No schedules configured.</td></tr>';
                return;
            }
            list.forEach(s => {
                const tr = document.createElement('tr');
                
                // Interval formatted
                let intervalStr = `${s.interval_seconds}s`;
                if (s.interval_seconds >= 3600) {
                    intervalStr = `${s.interval_seconds / 3600} Hour(s)`;
                } else if (s.interval_seconds >= 60) {
                    intervalStr = `${s.interval_seconds / 60} Minute(s)`;
                }
                
                const enabledChecked = s.enabled ? 'checked' : '';
                
                tr.innerHTML = `
                    <td><strong>${s.job_name}</strong></td>
                    <td><span class="badge badge-grey" style="color:#000;">${s.task_type}</span></td>
                    <td>${intervalStr}</td>
                    <td><code>${s.command}</code></td>
                    <td>
                        <label class="switch-container">
                            <input type="checkbox" class="schedule-toggle-checkbox" data-name="${s.job_name}" ${enabledChecked}>
                            <span class="switch-slider"></span>
                        </label>
                    </td>
                    <td style="text-align: right;">
                        <button class="btn btn-secondary btn-sm btn-trigger-dagur" data-name="${s.job_name}" style="height:28px; font-size:10px; padding:0 8px;">
                            Run Now
                        </button>
                    </td>
                `;
                dagurSchedulesTbody.appendChild(tr);
            });
            
            // Add toggle event listeners
            document.querySelectorAll('.schedule-toggle-checkbox').forEach(cb => {
                cb.replaceWith(cb.cloneNode(true)); // remove old listeners
            });
            document.querySelectorAll('.schedule-toggle-checkbox').forEach(cb => {
                cb.addEventListener('change', (e) => {
                    const name = e.target.getAttribute('data-name');
                    const enabled = e.target.checked;
                    updateDagurScheduleState(name, enabled);
                });
            });
            
            // Add trigger event listeners
            document.querySelectorAll('.btn-trigger-dagur').forEach(btn => {
                btn.replaceWith(btn.cloneNode(true));
            });
            document.querySelectorAll('.btn-trigger-dagur').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const name = e.currentTarget.getAttribute('data-name');
                    triggerDagurJob(name);
                });
            });
        })
        .catch(err => console.error('Failed to load Dagur schedules:', err));
    }

    function updateDagurScheduleState(name, enabled) {
        fetch(`${state.apiHost}/api/dagur/schedule/update`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ job_name: name, enabled: enabled })
        })
        .then(res => {
            if (!res.ok) throw new Error('Failed to update schedule status');
            return res.json();
        })
        .then(() => {
            showToast('Schedule Updated', `Job '${name}' has been ${enabled ? 'enabled' : 'disabled'}.`, 'success');
        })
        .catch(err => {
            showToast('Error', err.message, 'error');
            fetchDagurSchedules(); // revert check
        });
    }

    function triggerDagurJob(name) {
        fetch(`${state.apiHost}/api/dagur/schedule/trigger`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ job_name: name })
        })
        .then(res => {
            if (!res.ok) throw new Error('Failed to trigger job');
            return res.json();
        })
        .then(() => {
            showToast('Job Triggered', `Central task runner started '${name}' in background.`, 'success');
            setTimeout(fetchDagurRuns, 1000);
        })
        .catch(err => {
            showToast('Error', err.message, 'error');
        });
    }

    function fetchDagurRuns() {
        if (!dagurRunsTbody) return;
        fetch(`${state.apiHost}/api/dagur/runs`)
        .then(res => res.json())
        .then(data => {
            const list = data.runs || [];
            dagurRunsTbody.innerHTML = '';
            if (list.length === 0) {
                dagurRunsTbody.innerHTML = '<tr><td colspan="6" class="text-center text-muted">No execution history found.</td></tr>';
                return;
            }
            list.sort((a, b) => b.start_time - a.start_time);
            
            list.forEach(r => {
                const tr = document.createElement('tr');
                const startStr = new Date(r.start_time).toLocaleString();
                const endStr = r.end_time ? new Date(r.end_time).toLocaleString() : 'Running...';
                
                let statusBadge = '';
                if (r.status === 'SUCCESS') {
                    statusBadge = '<span class="status-badge status-good">Success</span>';
                } else if (r.status === 'FAILED') {
                    statusBadge = '<span class="status-badge status-critical">Failed</span>';
                } else {
                    statusBadge = '<span class="status-badge status-amber">Running</span>';
                }
                
                const exitCodeStr = r.exit_code !== undefined && r.exit_code !== -1 ? r.exit_code : 'N/A';
                
                tr.innerHTML = `
                    <td><strong>${r.job_name}</strong></td>
                    <td>${startStr}</td>
                    <td>${endStr}</td>
                    <td>${statusBadge}</td>
                    <td><code>${exitCodeStr}</code></td>
                    <td style="text-align: right;">
                        <button class="btn btn-secondary btn-sm btn-view-dagur-log" data-name="${r.job_name}" data-output="${btoa(unescape(encodeURIComponent(r.output || '')))}" style="height:28px; font-size:10px; padding:0 8px;">
                            View Logs
                        </button>
                    </td>
                `;
                dagurRunsTbody.appendChild(tr);
            });
            
            // Add view log listeners
            document.querySelectorAll('.btn-view-dagur-log').forEach(btn => {
                btn.replaceWith(btn.cloneNode(true));
            });
            document.querySelectorAll('.btn-view-dagur-log').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const name = e.currentTarget.getAttribute('data-name');
                    const outputB64 = e.currentTarget.getAttribute('data-output');
                    let output = '';
                    try {
                        output = decodeURIComponent(escape(atob(outputB64)));
                    } catch(err) {
                        output = 'Error decoding log output.';
                    }
                    openDagurLogModal(name, output);
                });
            });
        })
        .catch(err => console.error('Failed to load Dagur execution logs:', err));
    }

    function openDagurLogModal(name, log) {
        if (!dagurLogModal) return;
        dagurLogTitle.textContent = name;
        dagurLogContent.textContent = log || 'No output log recorded.';
        dagurLogModal.style.display = 'flex';
        setTimeout(() => dagurLogModal.classList.add('open'), 10);
    }

    if (btnCloseDagurLog) {
        btnCloseDagurLog.addEventListener('click', () => {
            if (dagurLogModal) {
                dagurLogModal.classList.remove('open');
                setTimeout(() => { dagurLogModal.style.display = 'none'; }, 200);
            }
        });
    }
    if (btnDagurLogDone) {
        btnDagurLogDone.addEventListener('click', () => {
            if (dagurLogModal) {
                dagurLogModal.classList.remove('open');
                setTimeout(() => { dagurLogModal.style.display = 'none'; }, 200);
            }
        });
    }

    // ----------------------------------------------------
    // Valhalla Images Page Logic
    // ----------------------------------------------------
    const imagesTableBody = document.getElementById('images-table-body');
    const dropZone = document.getElementById('drop-zone');
    const fileInput = document.getElementById('file-input');
    const uploadProgressContainer = document.getElementById('upload-progress-container');
    const uploadFilename = document.getElementById('upload-filename');
    const uploadPercent = document.getElementById('upload-percent');
    const uploadBar = document.getElementById('upload-bar');
    const btnRefreshImages = document.getElementById('btn-refresh-images');
    const imagesSummaryText = document.getElementById('images-summary-text');
    const isoCountDisplay = document.getElementById('iso-count-display');
    const templateCountDisplay = document.getElementById('template-count-display');

    if (imagesTableBody) {
        loadValhallaImages();
        
        if (btnRefreshImages) {
            btnRefreshImages.addEventListener('click', loadValhallaImages);
        }
        
        if (dropZone && fileInput) {
            dropZone.addEventListener('click', () => fileInput.click());
            
            dropZone.addEventListener('dragover', (e) => {
                e.preventDefault();
                dropZone.style.borderColor = 'var(--color-primary, #3b82f6)';
                dropZone.style.background = 'rgba(59, 130, 246, 0.05)';
            });
            
            dropZone.addEventListener('dragleave', () => {
                dropZone.style.borderColor = 'rgba(255,255,255,0.15)';
                dropZone.style.background = 'none';
            });
            
            dropZone.addEventListener('drop', (e) => {
                e.preventDefault();
                dropZone.style.borderColor = 'rgba(255,255,255,0.15)';
                dropZone.style.background = 'none';
                
                if (e.dataTransfer.files.length > 0) {
                    handleImageUpload(e.dataTransfer.files[0]);
                }
            });
            
            fileInput.addEventListener('change', () => {
                if (fileInput.files.length > 0) {
                    handleImageUpload(fileInput.files[0]);
                }
            });
        }
    }

    function loadValhallaImages() {
        if (!imagesTableBody) return;
        
        fetch(`${state.apiHost}/api/images`)
            .then(res => res.json())
            .then(data => {
                imagesTableBody.innerHTML = '';
                const images = data.images || [];
                
                if (images.length === 0) {
                    imagesTableBody.innerHTML = `
                        <tr>
                            <td colspan="6" style="text-align: center; padding: 2rem; color: var(--color-text-muted);">
                                No images registered in the Valhalla catalog yet. Drag and drop an ISO to upload!
                            </td>
                        </tr>
                    `;
                    if (imagesSummaryText) imagesSummaryText.textContent = '0 Images Registered';
                    if (isoCountDisplay) isoCountDisplay.textContent = '0';
                    if (templateCountDisplay) templateCountDisplay.textContent = '0';
                    return;
                }
                
                let isoCount = 0;
                let templateCount = 0;
                
                images.forEach(img => {
                    if (img.type === 'iso') isoCount++;
                    else templateCount++;
                    
                    const sizeGB = (img.size_bytes / (1024*1024*1024)).toFixed(2);
                    const regDate = img.created_at ? new Date(img.created_at).toLocaleDateString() : 'Unknown';
                    
                    const row = document.createElement('tr');
                    row.innerHTML = `
                        <td><strong style="color: var(--color-text);">${img.name}</strong></td>
                        <td><span style="font-family: monospace; font-size: 0.85rem;">${img.filename}</span></td>
                        <td><span class="badge ${img.type === 'iso' ? 'badge-primary' : 'badge-secondary'}" style="text-transform: uppercase;">${img.type}</span></td>
                        <td>${sizeGB} GB</td>
                        <td>${regDate}</td>
                        <td>
                            <button class="btn btn-danger btn-sm btn-delete-image" data-name="${img.name}" style="padding: 4px 8px; font-size: 0.8rem;">Delete</button>
                        </td>
                    `;
                    
                    const btnDelete = row.querySelector('.btn-delete-image');
                    btnDelete.addEventListener('click', () => {
                        if (confirm(`Are you sure you want to delete the image '${img.name}'?`)) {
                            deleteValhallaImage(img.name);
                        }
                    });
                    
                    imagesTableBody.appendChild(row);
                });
                
                if (imagesSummaryText) imagesSummaryText.textContent = `${images.length} Images Registered`;
                if (isoCountDisplay) isoCountDisplay.textContent = isoCount;
                if (templateCountDisplay) templateCountDisplay.textContent = templateCount;
            })
            .catch(err => {
                console.error("Error loading Valhalla images:", err);
                imagesTableBody.innerHTML = `
                    <tr>
                        <td colspan="6" style="text-align: center; padding: 2rem; color: var(--color-danger);">
                            Error querying Valhalla images catalog: ${err.message}
                        </td>
                    </tr>
                `;
            });
    }

    function deleteValhallaImage(name) {
        fetch(`${state.apiHost}/api/images/delete`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ name })
        })
        .then(res => {
            if (!res.ok) throw new Error('Failed to delete image');
            return res.json();
        })
        .then(data => {
            showToast('Image Deleted', `Successfully deleted Valhalla image '${name}'.`, 'success');
            loadValhallaImages();
        })
        .catch(err => {
            showToast('Delete Failed', err.message, 'danger');
        });
    }

    function handleImageUpload(file) {
        if (!file) return;
        if (!file.name.toLowerCase().endsWith('.iso')) {
            showToast('Invalid File', 'Only .iso image files are supported in the Valhalla Image Registry.', 'danger');
            return;
        }
        
        if (uploadProgressContainer && uploadFilename && uploadPercent && uploadBar) {
            uploadFilename.textContent = file.name;
            uploadPercent.textContent = '0%';
            uploadBar.style.width = '0%';
            uploadProgressContainer.style.display = 'block';
        }
        
        const xhr = new XMLHttpRequest();
        xhr.open('POST', `${state.apiHost}/api/images/upload?name=${encodeURIComponent(file.name)}`, true);
        
        const token = getStoredToken();
        if (token) {
            xhr.setRequestHeader('Authorization', `Bearer ${token}`);
        }
        
        xhr.setRequestHeader('X-File-Name', file.name);
        xhr.setRequestHeader('Content-Type', 'application/octet-stream');
        
        xhr.upload.onprogress = (e) => {
            if (e.lengthComputable) {
                const percent = Math.round((e.loaded / e.total) * 100);
                if (uploadPercent) uploadPercent.textContent = `${percent}%`;
                if (uploadBar) uploadBar.style.width = `${percent}%`;
            }
        };
        
        xhr.onload = () => {
            if (xhr.status === 200) {
                showToast('Upload Complete', `Valhalla image '${file.name}' uploaded and cataloged.`, 'success');
                loadValhallaImages();
                if (uploadProgressContainer) uploadProgressContainer.style.display = 'none';
            } else {
                showToast('Upload Failed', `Server returned error ${xhr.status}`, 'danger');
                if (uploadProgressContainer) uploadProgressContainer.style.display = 'none';
            }
        };
        
        xhr.onerror = () => {
            showToast('Upload Failed', 'A network connection error occurred.', 'danger');
            if (uploadProgressContainer) uploadProgressContainer.style.display = 'none';
        };
        
        xhr.send(file);
    }

    if (vmsTableBody) {
        vmsTableBody.addEventListener('mouseenter', () => { state.isHoveringVmsTable = true; });
        vmsTableBody.addEventListener('mouseleave', () => { state.isHoveringVmsTable = false; });

        // Table action buttons are now bound directly via bindRowButtons() during render.
    }

    // Metric Chart Class for Nutanix-style live graphs
    class MetricChart {
        constructor(canvasId, options = {}) {
            this.canvas = document.getElementById(canvasId);
            if (!this.canvas) return;
            this.ctx = this.canvas.getContext('2d');
            this.options = Object.assign({
                suffix: '',
                divider: 1,
                fixedDecimals: 0,
                baseValue: 0,
                noiseRange: 0,
                maxVal: null,
                valueDisplayId: null
            }, options);
            
            // Pre-populate with 60 historical data points (baseline placeholders)
            this.history = [];
            const now = Date.now();
            for (let i = 59; i >= 0; i--) {
                const noise = (Math.random() - 0.5) * this.options.noiseRange;
                this.history.push({
                    time: now - i * 1500,
                    value: Math.max(0, this.options.baseValue + noise)
                });
            }
            
            this.draw();
        }
        
        append(value) {
            this.history.push({
                time: Date.now(),
                value: value
            });
            if (this.history.length > 60) {
                this.history.shift();
            }
            this.draw();
        }
        
        draw() {
            if (!this.canvas) return;
            const ctx = this.ctx;
            const width = this.canvas.clientWidth;
            const height = this.canvas.clientHeight;
            
            if (this.canvas.width !== width || this.canvas.height !== height) {
                this.canvas.width = width;
                this.canvas.height = height;
            }
            
            ctx.clearRect(0, 0, width, height);
            if (this.history.length === 0) return;
            
            let minVal = 0;
            let maxVal = Math.max(...this.history.map(d => d.value));
            
            if (this.options.maxVal !== null && this.options.maxVal !== undefined) {
                maxVal = this.options.maxVal;
            } else if (this.options.minRange !== undefined) {
                if (maxVal < this.options.minRange) {
                    maxVal = this.options.minRange;
                }
            } else {
                if (maxVal < 10) {
                    maxVal = 10;
                }
            }
            
            const range = maxVal - minVal;
            const baseId = this.canvas.id.replace('chart-', '');
            
            // Update Max Y-axis text
            const maxLabelEl = document.getElementById('pe-chart-' + baseId + '-max') || document.getElementById('pe-' + baseId + '-max');
            if (maxLabelEl) {
                maxLabelEl.textContent = `${(maxVal / this.options.divider).toFixed(this.options.fixedDecimals)}${this.options.suffix}`;
            }
            
            // Update Current Val text
            const currentVal = this.history[this.history.length - 1].value;
            const displayId = this.options.valueDisplayId || ('pe-chart-' + baseId + '-val');
            const valEl = document.getElementById(displayId);
            if (valEl) {
                valEl.textContent = `${(currentVal / this.options.divider).toFixed(this.options.fixedDecimals)}${this.options.suffix}`;
            }
            
            const points = this.history.map((d, index) => {
                const x = (index / (this.history.length - 1)) * width;
                const y = height - 10 - ((d.value - minVal) / range) * (height - 20);
                return { x, y };
            });
            
            // Draw thin grid guides
            const isLightTheme = document.documentElement.classList.contains('theme-light');
            ctx.strokeStyle = isLightTheme ? 'rgba(0, 0, 0, 0.05)' : 'rgba(255, 255, 255, 0.04)';
            ctx.lineWidth = 1;
            ctx.setLineDash([3, 3]);
            
            for (let i = 0; i < this.history.length; i += 10) {
                const gx = (i / (this.history.length - 1)) * width;
                ctx.beginPath();
                ctx.moveTo(gx, 0);
                ctx.lineTo(gx, height);
                ctx.stroke();
            }
            
            ctx.beginPath();
            ctx.moveTo(0, height / 2);
            ctx.lineTo(width, height / 2);
            ctx.stroke();
            
            ctx.setLineDash([]);
            
            // Draw smooth Bézier curve gradient fill
            ctx.beginPath();
            ctx.moveTo(points[0].x, height);
            ctx.lineTo(points[0].x, points[0].y);
            for (let i = 0; i < points.length - 1; i++) {
                const p0 = points[i];
                const p1 = points[i + 1];
                const cpX1 = p0.x + (p1.x - p0.x) / 2;
                const cpY1 = p0.y;
                const cpX2 = p0.x + (p1.x - p0.x) / 2;
                const cpY2 = p1.y;
                ctx.bezierCurveTo(cpX1, cpY1, cpX2, cpY2, p1.x, p1.y);
            }
            ctx.lineTo(width, height);
            ctx.closePath();
            
            const gradColor0 = isLightTheme ? 'rgba(99, 102, 241, 0.25)' : 'rgba(56, 189, 248, 0.2)';
            const gradColor1 = isLightTheme ? 'rgba(99, 102, 241, 0.0)' : 'rgba(56, 189, 248, 0.0)';
            const strokeColor = isLightTheme ? 'hsl(245, 75%, 60%)' : '#38bdf8';

            const grad = ctx.createLinearGradient(0, 0, 0, height);
            grad.addColorStop(0, gradColor0);
            grad.addColorStop(1, gradColor1);
            ctx.fillStyle = grad;
            ctx.fill();
            
            // Draw smooth Bézier curve line path
            ctx.beginPath();
            ctx.moveTo(points[0].x, points[0].y);
            for (let i = 0; i < points.length - 1; i++) {
                const p0 = points[i];
                const p1 = points[i + 1];
                const cpX1 = p0.x + (p1.x - p0.x) / 2;
                const cpY1 = p0.y;
                const cpX2 = p0.x + (p1.x - p0.x) / 2;
                const cpY2 = p1.y;
                ctx.bezierCurveTo(cpX1, cpY1, cpX2, cpY2, p1.x, p1.y);
            }
            ctx.strokeStyle = strokeColor;
            ctx.lineWidth = 2.0;
            ctx.lineJoin = 'round';
            ctx.lineCap = 'round';
            ctx.stroke();
            
            // Update time labels
            const startTime = new Date(this.history[0].time).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
            const middleTime = new Date(this.history[Math.floor(this.history.length / 2)].time).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
            const endTime = new Date(this.history[this.history.length - 1].time).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
            
            const startEl = document.getElementById('pe-chart-' + baseId + '-time-start') || document.getElementById('pe-' + baseId + '-time-start');
            const middleEl = document.getElementById('pe-chart-' + baseId + '-time-middle') || document.getElementById('pe-' + baseId + '-time-middle');
            const endEl = document.getElementById('pe-chart-' + baseId + '-time-end') || document.getElementById('pe-' + baseId + '-time-end');
            
            if (startEl) startEl.textContent = startTime;
            if (middleEl) middleEl.textContent = middleTime;
            if (endEl) endEl.textContent = endTime;
        }
    }

    // Initialize charts on index.html if canvas elements are present
    state.charts = {};
    if (document.getElementById('Pe-chart-cpu') || document.getElementById('chart-cpu')) {
        state.charts['chart-cpu'] = new MetricChart('chart-cpu', { suffix: '%', fixedDecimals: 2, baseValue: 15, noiseRange: 5, valueDisplayId: 'pe-cpu-val', maxVal: 100 });
    }
    if (document.getElementById('Pe-chart-mem') || document.getElementById('Pe-mem-val') || document.getElementById('chart-mem')) {
        state.charts['chart-mem'] = new MetricChart('chart-mem', { suffix: '%', fixedDecimals: 2, baseValue: 60, noiseRange: 2, valueDisplayId: 'pe-mem-val', maxVal: 100 });
    }
    if (document.getElementById('chart-iops')) {
        state.charts['chart-iops'] = new MetricChart('chart-iops', { suffix: ' IOPS', fixedDecimals: 0, baseValue: 12, noiseRange: 3, minRange: 50 });
    }
    if (document.getElementById('pe-chart-bw') || document.getElementById('chart-bw')) {
        state.charts['chart-bw'] = new MetricChart('chart-bw', { suffix: ' MBps', fixedDecimals: 2, baseValue: 0.18, noiseRange: 0.05, minRange: 5.0 });
    }
    if (document.getElementById('pe-chart-latency') || document.getElementById('chart-latency')) {
        state.charts['chart-latency'] = new MetricChart('chart-latency', { suffix: ' ms', fixedDecimals: 2, baseValue: 0.95, noiseRange: 0.15, minRange: 5.0 });
    }

    // Fetch metrics history from backend on startup to pre-populate the charts
    fetch('/api/metrics/history')
        .then(response => response.json())
        .then(data => {
            if (data.history && data.history.length > 0) {
                const cpuChart = state.charts['chart-cpu'];
                const memChart = state.charts['chart-mem'];
                const iopsChart = state.charts['chart-iops'];
                const bwChart = state.charts['chart-bw'];
                const latencyChart = state.charts['chart-latency'];
                
                if (cpuChart) cpuChart.history = data.history.map(d => ({ time: d.time, value: d.cpu_pct }));
                if (memChart) memChart.history = data.history.map(d => ({ time: d.time, value: d.mem_pct }));
                if (iopsChart) iopsChart.history = data.history.map(d => ({ time: d.time, value: d.iops }));
                if (bwChart) bwChart.history = data.history.map(d => ({ time: d.time, value: d.bw_kbps / 1024.0 }));
                if (latencyChart) latencyChart.history = data.history.map(d => ({ time: d.time, value: d.latency_ms }));
                
                Object.values(state.charts).forEach(c => c.draw());
            }
        })
        .catch(err => console.error("Error loading metrics history:", err));

    // ----------------------------------------------------
    // Urbosa SDN Console Logic
    // ----------------------------------------------------
    state.urbosaT0 = [];
    state.urbosaT1 = [];
    state.urbosaSegments = [];
    state.urbosaFirewall = [];

    async function initUrbosaSdnPage() {
        // Tab switching logic
        const tabButtons = document.querySelectorAll('.sdn-tab-btn');
        tabButtons.forEach(btn => {
            btn.addEventListener('click', () => {
                tabButtons.forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                
                const tabId = btn.getAttribute('data-sdn-tab');
                document.querySelectorAll('.tab-content').forEach(pane => {
                    if (pane.id.startsWith('sdn-pane-')) {
                        pane.classList.remove('active');
                    }
                });
                const activePane = document.getElementById(`sdn-pane-${tabId}`);
                if (activePane) activePane.classList.add('active');
                
                // If switching to dashboard, redraw SVG topology
                if (tabId === 'dashboard') {
                    renderUrbosaTopology();
                }
            });
        });

        // BGP section toggle in T0 wizard
        const bgpToggle = document.getElementById('t0-bgp-enabled');
        const bgpSection = document.getElementById('t0-bgp-section');
        if (bgpToggle && bgpSection) {
            bgpToggle.addEventListener('change', () => {
                bgpSection.style.display = bgpToggle.checked ? 'block' : 'none';
            });
        }

        // Check activation status
        try {
            const res = await fetch(`${state.apiHost}/api/settings`);
            const settings = await res.json();
            const urbosaEnabled = settings.urbosa_enabled === 'true';
            
            const overlay = document.getElementById('urbosa-disabled-overlay');
            if (overlay) {
                overlay.style.display = urbosaEnabled ? 'none' : 'flex';
            }
            
            const sidebarStatus = document.getElementById('urbosa-sidebar-status');
            if (sidebarStatus) {
                sidebarStatus.innerHTML = urbosaEnabled 
                    ? '<span class="status-indicator status-green" style="font-size: 10px;">Active</span>' 
                    : '<span class="status-indicator status-red" style="font-size: 10px;">Deactivated</span>';
            }

            const mtuInput = document.getElementById('urbosa-mtu-input');
            if (mtuInput) {
                mtuInput.value = settings.dns_mtu || '1500';
            }

            const btnSaveMtu = document.getElementById('btn-save-urbosa-mtu');
            if (btnSaveMtu && !btnSaveMtu.dataset.listenerBound) {
                btnSaveMtu.dataset.listenerBound = 'true';
                btnSaveMtu.addEventListener('click', async () => {
                    btnSaveMtu.disabled = true;
                    const mtuVal = document.getElementById('urbosa-mtu-input').value;
                    try {
                        const sRes = await fetch(`${state.apiHost}/api/settings/update`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ dns_mtu: mtuVal })
                        });
                        const sData = await sRes.json();
                        if (sRes.ok) {
                            showToast('Success', 'Overlay MTU updated successfully.', 'success');
                            if (sData.task_id && typeof window.updateTasksList === 'function') {
                                window.updateTasksList();
                            }
                        } else {
                            showToast('Error', sData.error || 'Failed to update MTU.', 'error');
                        }
                    } catch (err) {
                        showToast('Error', 'Network request failed.', 'error');
                    } finally {
                        btnSaveMtu.disabled = false;
                    }
                });
            }
            
            if (!urbosaEnabled) return; // Stop here if disabled
        } catch (e) {
            console.error("Error loading settings:", e);
        }

        // Zoom and Pan Logic for SDN Topology SVG
        const svg = document.getElementById('sdn-topology-svg');
        const viewport = document.getElementById('topology-viewport-group');
        if (svg && viewport) {
            let zoomScale = 1;
            let zoomX = 0;
            let zoomY = 0;
            let isDragging = false;
            let dragStartX = 0;
            let dragStartY = 0;

            function updateViewportTransform() {
                viewport.setAttribute('transform', `translate(${zoomX}, ${zoomY}) scale(${zoomScale})`);
            }

            // Mouse wheel zoom
            svg.addEventListener('wheel', (e) => {
                e.preventDefault();
                const zoomIntensity = 0.08;
                
                const rect = svg.getBoundingClientRect();
                const mouseX = e.clientX - rect.left;
                const mouseY = e.clientY - rect.top;

                const targetX = (mouseX - zoomX) / zoomScale;
                const targetY = (mouseY - zoomY) / zoomScale;

                if (e.deltaY < 0) {
                    zoomScale += zoomScale * zoomIntensity;
                } else {
                    zoomScale -= zoomScale * zoomIntensity;
                }

                zoomScale = Math.min(Math.max(0.4, zoomScale), 3);

                zoomX = mouseX - targetX * zoomScale;
                zoomY = mouseY - targetY * zoomScale;

                updateViewportTransform();
            }, { passive: false });

            // Click-and-drag panning
            svg.addEventListener('mousedown', (e) => {
                if (e.target.closest('.svg-node') || e.target.closest('button') || e.target.closest('.zoom-controls')) return;
                isDragging = true;
                svg.style.cursor = 'grabbing';
                dragStartX = e.clientX - zoomX;
                dragStartY = e.clientY - zoomY;
            });

            window.addEventListener('mousemove', (e) => {
                if (!isDragging) return;
                zoomX = e.clientX - dragStartX;
                zoomY = e.clientY - dragStartY;
                updateViewportTransform();
            });

            window.addEventListener('mouseup', () => {
                if (isDragging) {
                    isDragging = false;
                    svg.style.cursor = 'grab';
                }
            });

            // Bind button controls
            document.getElementById('btn-zoom-in')?.addEventListener('click', (e) => {
                e.stopPropagation();
                const rect = svg.getBoundingClientRect();
                const centerX = rect.width / 2;
                const centerY = rect.height / 2;
                const targetX = (centerX - zoomX) / zoomScale;
                const targetY = (centerY - zoomY) / zoomScale;
                
                zoomScale = Math.min(zoomScale + 0.15, 3);
                zoomX = centerX - targetX * zoomScale;
                zoomY = centerY - targetY * zoomScale;
                updateViewportTransform();
            });

            document.getElementById('btn-zoom-out')?.addEventListener('click', (e) => {
                e.stopPropagation();
                const rect = svg.getBoundingClientRect();
                const centerX = rect.width / 2;
                const centerY = rect.height / 2;
                const targetX = (centerX - zoomX) / zoomScale;
                const targetY = (centerY - zoomY) / zoomScale;

                zoomScale = Math.max(zoomScale - 0.15, 0.4);
                zoomX = centerX - targetX * zoomScale;
                zoomY = centerY - targetY * zoomScale;
                updateViewportTransform();
            });

            document.getElementById('btn-zoom-reset')?.addEventListener('click', (e) => {
                e.stopPropagation();
                zoomScale = 1;
                zoomX = 0;
                zoomY = 0;
                updateViewportTransform();
            });
        }

        // Setup form submits
        setupUrbosaFormListeners();

        // Initial data load
        await refreshUrbosaData();
        
        // Render initial topology
        renderUrbosaTopology();

        // Start periodic refresh of Urbosa data and redraw topology if on dashboard
        setInterval(async () => {
            await refreshUrbosaData();
            const activeTab = document.querySelector('.sdn-tab-btn.active');
            if (activeTab && activeTab.getAttribute('data-sdn-tab') === 'dashboard') {
                renderUrbosaTopology();
            }
        }, 5000);
    }

    async function refreshUrbosaData() {
        try {
            const urls = [
                `${state.apiHost}/api/urbosa/t0`,
                `${state.apiHost}/api/urbosa/t1`,
                `${state.apiHost}/api/urbosa/segments`,
                `${state.apiHost}/api/urbosa/firewall`,
                `${state.apiHost}/api/vms`,
                `${state.apiHost}/api/urbosa/tunnels/status`
            ];
            
            const results = await Promise.all(urls.map(url => 
                fetch(url)
                    .then(async res => {
                        if (!res.ok) throw new Error(`HTTP ${res.status}`);
                        return await res.json();
                    })
                    .catch(err => {
                        console.error(`Error fetching ${url}:`, err);
                        return null;
                    })
            ));
            
            const [dataT0, dataT1, dataSeg, dataFw, dataVms, dataTunnels] = results;
            
            state.urbosaT0 = (dataT0 ? (dataT0.routers || []) : []).map(router => {
                if (router.nat_rules) {
                    try {
                        const parsed = typeof router.nat_rules === 'string' ? JSON.parse(router.nat_rules) : router.nat_rules;
                        router.bgp_enabled = parsed.bgp_enabled === true || parsed.bgp_enabled === 'true';
                        router.bgp_local_asn = parsed.bgp_local_asn;
                        router.bgp_neighbor_ip = parsed.bgp_neighbor_ip;
                        router.bgp_remote_asn = parsed.bgp_remote_asn;
                        router.nat_masq = parsed.source_nat === 'masquerade';
                    } catch (e) {
                        console.error("Error parsing nat_rules:", e);
                    }
                }
                return router;
            });
            state.urbosaT1 = dataT1 ? (dataT1.routers || []) : [];
            state.urbosaSegments = dataSeg ? (dataSeg.segments || []) : [];
            state.urbosaFirewall = dataFw ? (dataFw.rules || []) : [];
            state.vms = dataVms ? (dataVms.vms || []) : [];
            state.urbosaTunnels = dataTunnels ? (dataTunnels.tunnels || []) : [];
        } catch (err) {
            console.error("Error refreshing Urbosa data:", err);
        }

        // Render tables
        renderGatoT0Table();
        renderGatoT1Table();
        renderGatoSegmentsTable();
        renderGatoFirewallTable();
        renderTepTable();
        renderTunnelsTable();
        
        // Update dropdown menus
        updateGatoDropdowns();
    }

    function renderGatoT0Table() {
        const tbody = document.getElementById('t0-table-body');
        if (!tbody) return;
        tbody.innerHTML = '';
        if (state.urbosaT0.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7" class="table-loading">No Tier-0 Gateways defined.</td></tr>';
            return;
        }
        state.urbosaT0.forEach(router => {
            let natRules = {};
            if (router.nat_rules) {
                try {
                    natRules = typeof router.nat_rules === 'string' ? JSON.parse(router.nat_rules) : router.nat_rules;
                } catch (e) {
                    console.error("Failed to parse nat_rules:", e);
                }
            }
            const hasBgp = natRules.bgp_enabled === true || natRules.bgp_enabled === 'true';
            const bgpLocalAsn = natRules.bgp_local_asn || '';
            const bgpNeighborIp = natRules.bgp_neighbor_ip || '';
            const bgpRemoteAsn = natRules.bgp_remote_asn || '';
            const natMasq = natRules.source_nat === 'masquerade' || natRules.snat === true || natRules.snat === 'true';
            
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td><strong>${router.name}</strong></td>
                <td><code>${router.uplink_interface}</code></td>
                <td><code>${router.uplink_ip}</code></td>
                <td><code>${router.gateway_ip}</code></td>
                <td>${natMasq ? '<span class="status-indicator status-green">Enabled</span>' : '<span class="status-indicator status-grey">Disabled</span>'}</td>
                <td>${hasBgp ? `<span class="badge badge-vlan">BGP: ASN ${bgpLocalAsn || 'N/A'}</span>` : '<span class="status-indicator status-grey">Disabled</span>'}</td>
                <td style="text-align: right;">
                    <button class="table-action-btn btn-edit-t0" data-id="${router.router_id}" data-name="${router.name}" data-uplink-interface="${router.uplink_interface}" data-uplink-ip="${router.uplink_ip}" data-gateway-ip="${router.gateway_ip}" data-nat-masq="${natMasq}" data-bgp-enabled="${hasBgp}" data-bgp-local-asn="${bgpLocalAsn}" data-bgp-neighbor-ip="${bgpNeighborIp}" data-bgp-remote-asn="${bgpRemoteAsn}" style="margin-right: 5px; border-color: var(--color-primary); color: var(--color-primary);">Edit</button>
                    <button class="table-action-btn power-btn-stop btn-delete-t0" data-id="${router.router_id}">Delete</button>
                </td>
            `;
            tbody.appendChild(tr);
        });
        
        tbody.querySelectorAll('.btn-edit-t0').forEach(btn => {
            btn.addEventListener('click', () => {
                const id = btn.getAttribute('data-id');
                const name = btn.getAttribute('data-name');
                const uplinkInterface = btn.getAttribute('data-uplink-interface');
                const uplinkIp = btn.getAttribute('data-uplink-ip');
                const gatewayIp = btn.getAttribute('data-gateway-ip');
                const natMasq = btn.getAttribute('data-nat-masq') === 'true';
                const bgpEnabled = btn.getAttribute('data-bgp-enabled') === 'true';
                const bgpLocalAsn = btn.getAttribute('data-bgp-local-asn');
                const bgpNeighborIp = btn.getAttribute('data-bgp-neighbor-ip');
                const bgpRemoteAsn = btn.getAttribute('data-bgp-remote-asn');
                handleEditT0Modal(id, name, uplinkInterface, uplinkIp, gatewayIp, natMasq, bgpEnabled, bgpLocalAsn, bgpNeighborIp, bgpRemoteAsn);
            });
        });

        tbody.querySelectorAll('.btn-delete-t0').forEach(btn => {
            btn.addEventListener('click', async () => {
                const id = btn.getAttribute('data-id');
                if (confirm("Are you sure you want to delete this Tier-0 Gateway?")) {
                    await handleGatoDelete('/api/urbosa/t0/delete', { router_id: id });
                }
            });
        });
    }

    function renderGatoT1Table() {
        const tbody = document.getElementById('t1-table-body');
        if (!tbody) return;
        tbody.innerHTML = '';
        if (state.urbosaT1.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" class="table-loading">No Tier-1 Routers defined.</td></tr>';
            return;
        }
        state.urbosaT1.forEach(router => {
            const t0Parent = state.urbosaT0.find(r => r.router_id === router.t0_link_id);
            const parentName = t0Parent ? t0Parent.name : 'Unknown T0';
            const dhcpStatus = router.dhcp_enabled ? '<span class="status-indicator status-green">Enabled</span>' : '<span class="status-indicator status-grey">Disabled</span>';
            
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td><strong>${router.name}</strong></td>
                <td><code>${parentName}</code></td>
                <td>${dhcpStatus}</td>
                <td><span class="status-indicator status-green">Active (DLR)</span></td>
                <td style="text-align: right;">
                    <button class="table-action-btn btn-edit-t1" data-id="${router.router_id}" data-name="${router.name}" data-t0="${router.t0_link_id}" data-dhcp-enabled="${router.dhcp_enabled}" style="margin-right: 5px; border-color: var(--color-primary); color: var(--color-primary);">Edit</button>
                    <button class="table-action-btn power-btn-stop btn-delete-t1" data-id="${router.router_id}">Delete</button>
                </td>
            `;
            tbody.appendChild(tr);
        });
        
        tbody.querySelectorAll('.btn-edit-t1').forEach(btn => {
            btn.addEventListener('click', () => {
                const id = btn.getAttribute('data-id');
                const name = btn.getAttribute('data-name');
                const t0LinkId = btn.getAttribute('data-t0');
                const dhcpEnabled = btn.getAttribute('data-dhcp-enabled') === 'true' || btn.getAttribute('data-dhcp-enabled') === 'Enabled';
                handleEditT1Modal(id, name, t0LinkId, dhcpEnabled);
            });
        });

        tbody.querySelectorAll('.btn-delete-t1').forEach(btn => {
            btn.addEventListener('click', async () => {
                const id = btn.getAttribute('data-id');
                if (confirm("Are you sure you want to delete this Tier-1 Router?")) {
                    await handleGatoDelete('/api/urbosa/t1/delete', { router_id: id });
                }
            });
        });
    }

    function renderGatoSegmentsTable() {
        const tbody = document.getElementById('segments-table-body');
        if (!tbody) return;
        tbody.innerHTML = '';
        if (state.urbosaSegments.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7" class="table-loading">No VXLAN Overlay Segments provisioned.</td></tr>';
            return;
        }
        state.urbosaSegments.forEach(seg => {
            const t1Parent = state.urbosaT1.find(r => r.router_id === seg.t1_link_id);
            const parentName = t1Parent ? t1Parent.name : 'Unknown T1';
            
            const dhcpInfo = seg.dhcp_enabled 
                ? `<span class="badge-allow" style="font-size:10px;" title="Range: ${seg.dhcp_start} - ${seg.dhcp_end}">Range: ${seg.dhcp_start.split('.').slice(2).join('.')}-${seg.dhcp_end.split('.').slice(3).join('.')}</span>` 
                : '<span class="status-indicator status-grey">Disabled</span>';
            
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td><strong>${seg.name}</strong></td>
                <td><span class="badge badge-vlan">VXLAN: VNI ${seg.vni}</span></td>
                <td><code>${seg.subnet_cidr}</code></td>
                <td><code>${seg.gateway_ip}</code></td>
                <td><code>${parentName}</code></td>
                <td>${dhcpInfo}</td>
                <td style="text-align: right;">
                    <button class="table-action-btn btn-edit-segment" 
                            data-id="${seg.segment_id}" 
                            data-name="${seg.name}" 
                            data-vni="${seg.vni}"
                            data-cidr="${seg.subnet_cidr}"
                            data-gw="${seg.gateway_ip}"
                            data-t1="${seg.t1_link_id}" 
                            data-dhcp-enabled="${seg.dhcp_enabled}" 
                            data-dhcp-start="${seg.dhcp_start || ''}" 
                            data-dhcp-end="${seg.dhcp_end || ''}" 
                            style="margin-right: 5px; border-color: var(--color-primary); color: var(--color-primary);">Edit</button>
                    <button class="table-action-btn power-btn-stop btn-delete-segment" data-id="${seg.segment_id}">Delete</button>
                </td>
            `;
            tbody.appendChild(tr);
        });
        
        tbody.querySelectorAll('.btn-edit-segment').forEach(btn => {
            btn.addEventListener('click', () => {
                const id = btn.getAttribute('data-id');
                const name = btn.getAttribute('data-name');
                const vni = btn.getAttribute('data-vni');
                const cidr = btn.getAttribute('data-cidr');
                const gw = btn.getAttribute('data-gw');
                const t1 = btn.getAttribute('data-t1');
                const dhcpEnabled = btn.getAttribute('data-dhcp-enabled');
                const dhcpStart = btn.getAttribute('data-dhcp-start');
                const dhcpEnd = btn.getAttribute('data-dhcp-end');
                handleEditSegmentModal(id, name, vni, cidr, gw, t1, dhcpEnabled, dhcpStart, dhcpEnd);
            });
        });

        tbody.querySelectorAll('.btn-delete-segment').forEach(btn => {
            btn.addEventListener('click', async () => {
                const id = btn.getAttribute('data-id');
                if (confirm("Are you sure you want to delete this Overlay Segment?")) {
                    await handleGatoDelete('/api/urbosa/segments/delete', { segment_id: id });
                }
            });
        });
    }

    function handleEditSegmentModal(segmentId, segmentName, vni, subnetCidr, gatewayIp, currentT1Id, dhcpEnabled, dhcpStart, dhcpEnd) {
        const existing = document.getElementById('edit-segment-modal');
        if (existing) existing.remove();

        const modalDiv = document.createElement('div');
        modalDiv.id = 'edit-segment-modal';
        modalDiv.className = 'modal-backdrop';
        modalDiv.style.display = 'flex';
        modalDiv.style.alignItems = 'center';
        modalDiv.style.justifyContent = 'center';
        modalDiv.style.zIndex = '2000';

        let options = '';
        state.urbosaT1.forEach(router => {
            options += `<option value="${router.router_id}" ${router.router_id === currentT1Id ? 'selected' : ''}>${router.name}</option>`;
        });

        const isChecked = dhcpEnabled === true || dhcpEnabled === 'true' ? 'checked' : '';

        modalDiv.innerHTML = `
            <div class="modal-container" style="width: 1050px; max-width: 95vw; background: var(--bg-card); backdrop-filter: blur(20px); border: 1px solid var(--border-color); border-radius: 12px; padding: 20px; box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);">
                <div class="modal-header" style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; border-bottom: 1px solid var(--border-color); padding-bottom: 10px;">
                    <h3 style="font-family: 'Space Grotesk', sans-serif; font-size: 18px; font-weight: 600; color: #fff; margin: 0;">Edit Segment Configuration</h3>
                    <button id="btn-close-edit-segment" style="background: none; border: none; color: var(--text-primary); font-size: 24px; cursor: pointer; line-height: 1; padding: 0;">&times;</button>
                </div>
                <div class="modal-body">
                    <form id="edit-segment-form" style="display: flex; flex-direction: column; gap: 15px;">
                        <div class="form-grid" style="display: grid; grid-template-columns: 1.2fr 1fr 1fr; gap: 20px;">
                            <!-- Column 1: Subnet Info -->
                            <div style="display: flex; flex-direction: column; gap: 12px;">
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Segment Name</label>
                                    <input type="text" id="edit-segment-name" class="form-input" value="${segmentName}" style="width: 100%; box-sizing: border-box;">
                                </div>
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">VXLAN VNI</label>
                                    <input type="number" id="edit-segment-vni" class="form-input" value="${vni}" style="width: 100%; box-sizing: border-box;">
                                </div>
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Subnet CIDR</label>
                                    <input type="text" id="edit-segment-cidr" class="form-input" value="${subnetCidr}" style="width: 100%; box-sizing: border-box;">
                                </div>
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Gateway IP</label>
                                    <input type="text" id="edit-segment-gw" class="form-input" value="${gatewayIp}" style="width: 100%; box-sizing: border-box;">
                                </div>
                            </div>
                            <!-- Column 2: Router Link & DHCP switch -->
                            <div style="display: flex; flex-direction: column; gap: 12px; border-left: 1px solid var(--border-color); padding-left: 20px;">
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Backing Tier-1 Router <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <select id="edit-segment-t1" class="form-input" style="width: 100%; box-sizing: border-box;">
                                        ${options}
                                    </select>
                                </div>
                                <div class="form-group" style="margin-top: 10px;">
                                    <label style="display: flex; align-items: center; gap: 8px; font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); cursor: pointer;">
                                        <input type="checkbox" id="edit-segment-dhcp-enabled" ${isChecked} style="accent-color: var(--color-primary); width: 16px; height: 16px;">
                                        Enable DHCP IPAM Server
                                    </label>
                                </div>
                            </div>
                            <!-- Column 3: DHCP IPAM details -->
                            <div style="display: flex; flex-direction: column; gap: 12px; border-left: 1px solid var(--border-color); padding-left: 20px;">
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 11px; color: var(--text-muted); display: block; margin-bottom: 5px;">DHCP Range Start</label>
                                    <input type="text" id="edit-segment-dhcp-start" class="form-input" value="${dhcpStart}" style="width: 100%; box-sizing: border-box;">
                                </div>
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 11px; color: var(--text-muted); display: block; margin-bottom: 5px;">DHCP Range End</label>
                                    <input type="text" id="edit-segment-dhcp-end" class="form-input" value="${dhcpEnd}" style="width: 100%; box-sizing: border-box;">
                                </div>
                            </div>
                        </div>
                        <div style="display: flex; justify-content: flex-end; gap: 10px; margin-top: 15px; border-top: 1px solid var(--border-color); padding-top: 15px;">
                            <button type="button" id="btn-cancel-edit-segment" class="btn btn-secondary" style="padding: 8px 16px; font-family: 'Space Grotesk', sans-serif;">Cancel</button>
                            <button type="submit" class="btn btn-primary" style="padding: 8px 16px; font-family: 'Space Grotesk', sans-serif;">Save Changes</button>
                        </div>
                    </form>
                </div>
            </div>
        `;

        document.body.appendChild(modalDiv);

        const close = () => modalDiv.remove();
        document.getElementById('btn-close-edit-segment').addEventListener('click', close);
        document.getElementById('btn-cancel-edit-segment').addEventListener('click', close);

        document.getElementById('edit-segment-form').addEventListener('submit', async (e) => {
            e.preventDefault();
            const newName = document.getElementById('edit-segment-name').value.trim();
            const newVni = parseInt(document.getElementById('edit-segment-vni').value);
            const newCidr = document.getElementById('edit-segment-cidr').value.trim();
            const newGw = document.getElementById('edit-segment-gw').value.trim();
            const newT1Id = document.getElementById('edit-segment-t1').value;
            const newDhcpEnabled = document.getElementById('edit-segment-dhcp-enabled').checked;
            const newDhcpStart = document.getElementById('edit-segment-dhcp-start').value.trim();
            const newDhcpEnd = document.getElementById('edit-segment-dhcp-end').value.trim();

            try {
                const res = await fetch('/api/urbosa/segments/update', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        segment_id: segmentId,
                        name: newName,
                        vni: newVni,
                        subnet_cidr: newCidr,
                        gateway_ip: newGw,
                        t1_link_id: newT1Id,
                        dhcp_enabled: newDhcpEnabled,
                        dhcp_start: newDhcpStart,
                        dhcp_end: newDhcpEnd
                    })
                });
                const data = await res.json();
                if (res.ok) {
                    showToast('Success', data.message || 'Segment updated successfully.', 'success');
                    close();
                    if (data.task_id && typeof window.updateTasksList === 'function') {
                        window.updateTasksList();
                    }
                    setTimeout(() => window.location.reload(), 1500);
                } else {
                    showToast('Error', data.error || 'Failed to update segment.', 'error');
                }
            } catch (err) {
                showToast('Error', 'Network request failed.', 'error');
            }
        });
    }

    function renderGatoFirewallTable() {
        const tbody = document.getElementById('firewall-table-body');
        if (!tbody) return;
        tbody.innerHTML = '';
        if (state.urbosaFirewall.length === 0) {
            tbody.innerHTML = '<tr><td colspan="8" class="table-loading">No micro-segmentation rules deployed.</td></tr>';
            return;
        }
        
        const sortedRules = [...state.urbosaFirewall].sort((a, b) => (a.priority || 0) - (b.priority || 0));
        
        sortedRules.forEach(rule => {
            const actionBadge = rule.action === 'ALLOW' 
                ? '<span class="badge-allow">ALLOW</span>' 
                : '<span class="badge-deny">DENY</span>';
                
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td><code>${rule.priority}</code></td>
                <td><strong>${rule.description}</strong></td>
                <td><code>${rule.source_ip}</code></td>
                <td><code>${rule.dest_ip}</code></td>
                <td><span class="badge badge-vlan" style="font-size:10px;">${rule.protocol}</span></td>
                <td><code>${rule.port === 0 ? 'ANY' : rule.port}</code></td>
                <td>${actionBadge}</td>
                <td style="text-align: right;">
                    <button class="table-action-btn btn-edit-firewall" data-id="${rule.rule_id}" data-priority="${rule.priority}" data-description="${rule.description}" data-source="${rule.source_ip}" data-dest="${rule.dest_ip}" data-protocol="${rule.protocol}" data-port="${rule.port}" data-action="${rule.action}" style="margin-right: 5px; border-color: var(--color-primary); color: var(--color-primary);">Edit</button>
                    <button class="table-action-btn power-btn-stop btn-delete-rule" data-id="${rule.rule_id}">Delete</button>
                </td>
            `;
            tbody.appendChild(tr);
        });

        tbody.querySelectorAll('.btn-edit-firewall').forEach(btn => {
            btn.addEventListener('click', () => {
                const id = btn.getAttribute('data-id');
                const priority = parseInt(btn.getAttribute('data-priority'));
                const description = btn.getAttribute('data-description');
                const source = btn.getAttribute('data-source');
                const dest = btn.getAttribute('data-dest');
                const protocol = btn.getAttribute('data-protocol');
                const port = parseInt(btn.getAttribute('data-port'));
                const action = btn.getAttribute('data-action');
                handleEditFirewallModal(id, priority, description, source, dest, protocol, port, action);
            });
        });
        
        tbody.querySelectorAll('.btn-delete-rule').forEach(btn => {
            btn.addEventListener('click', async () => {
                const id = btn.getAttribute('data-id');
                if (confirm("Are you sure you want to delete this stateful firewall rule?")) {
                    await handleGatoDelete('/api/urbosa/firewall/delete', { rule_id: id });
                }
            });
        });
    }

    function renderTepTable() {
        const tbody = document.getElementById('tep-table-body');
        if (!tbody) return;
        tbody.innerHTML = '';
        const nodesMap = {};
        const activeNodes = (state.nodes && state.nodes.length > 0) ? state.nodes : [
            { name: 'hci-node01', ip: '10.10.102.220', role: 'Leader' },
            { name: 'hci-node02', ip: '10.10.102.222', role: 'Follower' },
            { name: 'hci-node03', ip: '10.10.102.223', role: 'Follower' }
        ];
        activeNodes.forEach(node => {
            nodesMap[node.ip] = {
                name: node.name,
                ip: node.ip,
                status: node.status || 'ONLINE',
                uplink: 'ens192',
                mtu: '1500',
                rx_sum: 0,
                tx_sum: 0
            };
        });
        if (state.urbosaTunnels && state.urbosaTunnels.length > 0) {
            state.urbosaTunnels.forEach(t => {
                if (nodesMap[t.node_ip]) {
                    nodesMap[t.node_ip].rx_sum += parseFloat(t.rx_kbps || 0);
                    nodesMap[t.node_ip].tx_sum += parseFloat(t.tx_kbps || 0);
                }
            });
        }
        Object.values(nodesMap).forEach(node => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td><strong>${node.name}</strong></td>
                <td><code>${node.ip}</code></td>
                <td><span class="status-indicator status-green">Online</span></td>
                <td><code>${node.uplink}</code></td>
                <td><code>${document.getElementById('urbosa-mtu-input')?.value || '1500'}</code></td>
                <td><span style="font-family: monospace;">${node.rx_sum.toFixed(2)} KB/s</span></td>
                <td><span style="font-family: monospace;">${node.tx_sum.toFixed(2)} KB/s</span></td>
            `;
            tbody.appendChild(tr);
        });
    }

    function renderTunnelsTable() {
        const tbody = document.getElementById('tunnels-table-body');
        if (!tbody) return;
        tbody.innerHTML = '';
        if (!state.urbosaTunnels || state.urbosaTunnels.length === 0) {
            tbody.innerHTML = '<tr><td colspan="8" class="table-loading">No active VXLAN overlay tunnels.</td></tr>';
            return;
        }
        state.urbosaTunnels.forEach(tunnel => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td><span class="badge badge-vlan">${tunnel.vni}</span></td>
                <td><strong>${tunnel.segment_name || 'Overlay Segment'}</strong></td>
                <td><code>${tunnel.node_ip}</code> (${tunnel.node_name})</td>
                <td><code>${tunnel.interface_name}</code></td>
                <td><span class="status-indicator status-green">Up</span></td>
                <td><span style="font-family: monospace;">${parseFloat(tunnel.rx_kbps || 0).toFixed(2)} KB/s</span></td>
                <td><span style="font-family: monospace;">${parseFloat(tunnel.tx_kbps || 0).toFixed(2)} KB/s</span></td>
                <td>
                    <button class="btn btn-primary btn-sm btn-tunnel-details" 
                            data-ip="${tunnel.node_ip}" 
                            data-iface="${tunnel.interface_name}" 
                            data-seg="${tunnel.segment_name || 'Overlay Segment'}"
                            data-vni="${tunnel.vni}"
                            style="padding: 2px 6px; font-size: 11px; height: 22px;">
                        Details
                    </button>
                </td>
            `;
            tbody.appendChild(tr);
        });
        tbody.querySelectorAll('.btn-tunnel-details').forEach(btn => {
            btn.addEventListener('click', () => {
                const ip = btn.getAttribute('data-ip');
                const iface = btn.getAttribute('data-iface');
                const segment = btn.getAttribute('data-seg');
                const vni = btn.getAttribute('data-vni');
                handleTunnelDetailsModal(ip, iface, segment, vni);
            });
        });
    }

    function handleTunnelDetailsModal(nodeIp, interfaceName, segmentName, vni) {
        const existing = document.getElementById('tunnel-details-modal');
        if (existing) existing.remove();
        const modalDiv = document.createElement('div');
        modalDiv.id = 'tunnel-details-modal';
        modalDiv.className = 'modal-backdrop';
        modalDiv.style.display = 'flex';
        modalDiv.style.alignItems = 'center';
        modalDiv.style.justifyContent = 'center';
        modalDiv.style.zIndex = '2000';
        modalDiv.innerHTML = `
            <div class="modal-container" style="width: 650px; background: var(--bg-card); backdrop-filter: blur(20px); border: 1px solid var(--border-color); border-radius: 12px; padding: 20px; box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37); color: #fff;">
                <div class="modal-header" style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; border-bottom: 1px solid var(--border-color); padding-bottom: 10px;">
                    <h3 style="font-family: 'Space Grotesk', sans-serif; font-size: 18px; font-weight: 600; color: #fff; margin: 0;">
                        Tunnel Details: ${interfaceName} (Segment: ${segmentName})
                    </h3>
                    <button id="btn-close-tunnel-details" style="background: none; border: none; color: var(--text-primary); font-size: 24px; cursor: pointer; line-height: 1; padding: 0;">&times;</button>
                </div>
                <div class="modal-body" style="display: flex; flex-direction: column; gap: 15px;">
                    <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 10px; background: rgba(255,255,255,0.03); padding: 10px; border-radius: 6px;">
                        <div>
                            <span style="font-size: 11px; color: var(--text-secondary);">Host Node IP</span>
                            <div style="font-size: 14px; font-weight: 600;">${nodeIp}</div>
                        </div>
                        <div>
                            <span style="font-size: 11px; color: var(--text-secondary);">Interface VNI</span>
                            <div style="font-size: 14px; font-weight: 600;">${vni}</div>
                        </div>
                        <div>
                            <span style="font-size: 11px; color: var(--text-secondary);">Status</span>
                            <div style="font-size: 14px; font-weight: 600; color: #10b981;">● Connected</div>
                        </div>
                    </div>
                    <div>
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                            <span style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600;">Real-time Throughput (10s intervals)</span>
                            <div style="font-size: 11px; display: flex; gap: 10px;">
                                <span style="color: #38bdf8;">● Rx Throughput</span>
                                <span style="color: #a855f7;">● Tx Throughput</span>
                            </div>
                        </div>
                        <div style="background: rgba(0,0,0,0.2); border: 1px solid var(--border-color); border-radius: 6px; padding: 10px; position: relative;">
                            <canvas id="tunnel-details-chart" style="width: 100%; height: 200px; display: block;"></canvas>
                        </div>
                    </div>
                    <div style="display: flex; justify-content: flex-end; margin-top: 10px;">
                        <button id="btn-close-details-bottom" class="btn btn-secondary">Close</button>
                    </div>
                </div>
            </div>
        `;
        document.body.appendChild(modalDiv);
        const close = () => {
            if (chartInterval) clearInterval(chartInterval);
            modalDiv.remove();
        };
        document.getElementById('btn-close-tunnel-details').onclick = close;
        document.getElementById('btn-close-details-bottom').onclick = close;
        let chartInterval = null;
        async function fetchAndDrawMetrics() {
            try {
                const res = await fetch(`/api/urbosa/tunnels/metrics?node_ip=${nodeIp}&interface_name=${interfaceName}&limit=60`);
                if (res.ok) {
                    const data = await res.json();
                    const canvas = document.getElementById('tunnel-details-chart');
                    drawTunnelMetricChart(canvas, data.metrics || []);
                }
            } catch (err) {
                console.error("Error drawing tunnel metrics:", err);
            }
        }
        fetchAndDrawMetrics();
        chartInterval = setInterval(fetchAndDrawMetrics, 10000);
    }

    function drawTunnelMetricChart(canvas, dataPoints) {
        if (!canvas) return;
        const ctx = canvas.getContext('2d');
        const width = canvas.clientWidth;
        const height = canvas.clientHeight;
        if (canvas.width !== width || canvas.height !== height) {
            canvas.width = width;
            canvas.height = height;
        }
        ctx.clearRect(0, 0, width, height);
        if (!dataPoints || dataPoints.length === 0) {
            ctx.fillStyle = '#fff';
            ctx.font = '12px Space Grotesk';
            ctx.fillText('No telemetry data available', width / 2 - 70, height / 2);
            return;
        }
        let maxVal = 10;
        dataPoints.forEach(d => {
            if (d.rx_kbps > maxVal) maxVal = d.rx_kbps;
            if (d.tx_kbps > maxVal) maxVal = d.tx_kbps;
        });
        const pad = 15;
        const graphHeight = height - pad * 2;
        const graphWidth = width - pad * 2;
        ctx.strokeStyle = 'rgba(255, 255, 255, 0.04)';
        ctx.lineWidth = 1;
        ctx.setLineDash([3, 3]);
        ctx.beginPath();
        ctx.moveTo(pad, height / 2);
        ctx.lineTo(width - pad, height / 2);
        ctx.stroke();
        ctx.setLineDash([]);
        const rxPoints = [];
        const txPoints = [];
        dataPoints.forEach((d, idx) => {
            const x = pad + (idx / (dataPoints.length - 1)) * graphWidth;
            const yRx = height - pad - (d.rx_kbps / maxVal) * graphHeight;
            const yTx = height - pad - (d.tx_kbps / maxVal) * graphHeight;
            rxPoints.push({ x, y: yRx });
            txPoints.push({ x, y: yTx });
        });
        function drawPath(points, strokeColor, fillColor) {
            if (points.length < 2) return;
            ctx.beginPath();
            ctx.moveTo(points[0].x, height - pad);
            ctx.lineTo(points[0].x, points[0].y);
            for (let i = 0; i < points.length - 1; i++) {
                const p0 = points[i];
                const p1 = points[i+1];
                const cpX1 = p0.x + (p1.x - p0.x) / 2;
                const cpY1 = p0.y;
                const cpX2 = p0.x + (p1.x - p0.x) / 2;
                const cpY2 = p1.y;
                ctx.bezierCurveTo(cpX1, cpY1, cpX2, cpY2, p1.x, p1.y);
            }
            ctx.lineTo(points[points.length - 1].x, height - pad);
            ctx.closePath();
            const grad = ctx.createLinearGradient(0, 0, 0, height);
            grad.addColorStop(0, fillColor);
            grad.addColorStop(1, 'rgba(0, 0, 0, 0)');
            ctx.fillStyle = grad;
            ctx.fill();
            ctx.beginPath();
            ctx.moveTo(points[0].x, points[0].y);
            for (let i = 0; i < points.length - 1; i++) {
                const p0 = points[i];
                const p1 = points[i+1];
                const cpX1 = p0.x + (p1.x - p0.x) / 2;
                const cpY1 = p0.y;
                const cpX2 = p0.x + (p1.x - p0.x) / 2;
                const cpY2 = p1.y;
                ctx.bezierCurveTo(cpX1, cpY1, cpX2, cpY2, p1.x, p1.y);
            }
            ctx.strokeStyle = strokeColor;
            ctx.lineWidth = 2;
            ctx.stroke();
        }
        drawPath(rxPoints, '#38bdf8', 'rgba(56, 189, 248, 0.15)');
        drawPath(txPoints, '#a855f7', 'rgba(168, 85, 247, 0.15)');
        ctx.fillStyle = '#94a3b8';
        ctx.font = '10px Space Grotesk';
        ctx.fillText(`${maxVal.toFixed(1)} KB/s`, pad, pad - 2);
        ctx.fillText('0 KB/s', pad, height - pad + 12);
    }

    async function handleGatoDelete(url, payload) {
        try {
            const res = await fetch(`${state.apiHost}${url}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await res.json();
            if (res.ok) {
                showToast('Success', data.message || 'Resource deleted successfully.', 'success');
                if (data.task_id && typeof window.updateTasksList === 'function') {
                    window.updateTasksList();
                }
                setTimeout(() => window.location.reload(), 1500);
            } else {
                showToast('Error', data.error || 'Failed to delete resource.', 'error');
            }
        } catch (e) {
            console.error("urbosa deletion error:", e);
            showToast('Error', 'Network request failed.', 'error');
        }
    }

    function updateGatoDropdowns() {
        // Obsolete as options are dynamically built during modal creation
    }

    function handleCreateT0Modal() {
        const existing = document.getElementById('create-t0-modal');
        if (existing) existing.remove();

        const modalDiv = document.createElement('div');
        modalDiv.id = 'create-t0-modal';
        modalDiv.className = 'modal-backdrop';
        modalDiv.style.display = 'flex';
        modalDiv.style.alignItems = 'center';
        modalDiv.style.justifyContent = 'center';
        modalDiv.style.zIndex = '2000';

        modalDiv.innerHTML = `
            <div class="modal-container" style="width: 1050px; max-width: 95vw; background: var(--bg-card); backdrop-filter: blur(20px); border: 1px solid var(--border-color); border-radius: 12px; padding: 20px; box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37); overflow-y: auto; max-height: 95vh;">
                <div class="modal-header" style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; border-bottom: 1px solid var(--border-color); padding-bottom: 10px;">
                    <h3 style="font-family: 'Space Grotesk', sans-serif; font-size: 18px; font-weight: 600; color: #fff; margin: 0;">Create Tier-0 Logical Router</h3>
                    <button id="btn-close-create-t0" style="background: none; border: none; color: var(--text-primary); font-size: 24px; cursor: pointer; line-height: 1; padding: 0;">&times;</button>
                </div>
                <div class="modal-body">
                    <form id="create-t0-form" style="display: flex; flex-direction: column; gap: 15px;">
                        <div class="form-grid" style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 20px;">
                            <!-- Column 1: Identification & Interface -->
                            <div style="display: flex; flex-direction: column; gap: 12px;">
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Router Name <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <input type="text" id="t0-name" required placeholder="e.g. T0-Gateway" class="form-input" style="width: 100%; box-sizing: border-box;">
                                </div>
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Uplink Interface <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <select id="t0-uplink-interface" class="form-input" style="width: 100%; box-sizing: border-box;">
                                        <option value="ens192" selected>ens192 (ESXi Default)</option>
                                        <option value="ens3">ens3 (KVM/Proxmox Default)</option>
                                        <option value="ens33">ens33 (VMware Workstation Default)</option>
                                        <option value="eth0">eth0 (Standard Linux)</option>
                                        <option value="eno1">eno1 (Bare Metal Default)</option>
                                    </select>
                                    <span class="input-hint" style="font-size: 9px; color: var(--text-muted); display: block; margin-top: 2px;">Physical NIC on host nodes mapped to upstream switch.</span>
                                </div>
                            </div>
                            <!-- Column 2: IP Configuration & NAT -->
                            <div style="display: flex; flex-direction: column; gap: 12px; border-left: 1px solid var(--border-color); padding-left: 20px;">
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Uplink External IP / CIDR <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <input type="text" id="t0-uplink-ip" required placeholder="e.g. 10.10.102.250/24" class="form-input" style="width: 100%; box-sizing: border-box;">
                                    <span class="input-hint" style="font-size: 9px; color: var(--text-muted); display: block; margin-top: 2px;">Static IPv4 address and netmask for the uplink.</span>
                                </div>
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Upstream Gateway IP <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <input type="text" id="t0-gateway-ip" required placeholder="e.g. 10.10.102.1" class="form-input" style="width: 100%; box-sizing: border-box;">
                                    <span class="input-hint" style="font-size: 9px; color: var(--text-muted); display: block; margin-top: 2px;">Default gateway address of physical switch/router.</span>
                                </div>
                                <div class="form-group" style="margin-top: 5px;">
                                    <label style="display: flex; align-items: center; gap: 8px; font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); cursor: pointer;">
                                        <input type="checkbox" id="t0-nat-masq" checked style="accent-color: var(--color-primary); width: 16px; height: 16px;">
                                        Enable Outbound Source NAT (Masquerade)
                                    </label>
                                </div>
                            </div>
                            <!-- Column 3: BGP Configuration -->
                            <div style="display: flex; flex-direction: column; gap: 12px; border-left: 1px solid var(--border-color); padding-left: 20px;">
                                <div class="form-group">
                                    <label style="display: flex; align-items: center; gap: 8px; font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); cursor: pointer;">
                                        <input type="checkbox" id="t0-bgp-enabled" style="accent-color: var(--color-primary); width: 16px; height: 16px;">
                                        Enable Dynamic Routing (BGP Peering)
                                    </label>
                                </div>
                                <div id="t0-bgp-section" style="display: none; flex-direction: column; gap: 12px; padding-top: 5px;">
                                    <div class="form-group">
                                        <label style="font-family: 'Space Grotesk', sans-serif; font-size: 11px; color: var(--text-muted); display: block; margin-bottom: 5px;">Local ASN</label>
                                        <input type="number" id="t0-bgp-local-asn" placeholder="e.g. 65001" class="form-input" style="width: 100%; box-sizing: border-box;">
                                    </div>
                                    <div class="form-group">
                                        <label style="font-family: 'Space Grotesk', sans-serif; font-size: 11px; color: var(--text-muted); display: block; margin-bottom: 5px;">Neighbor IP</label>
                                        <input type="text" id="t0-bgp-neighbor-ip" placeholder="e.g. 10.10.102.254" class="form-input" style="width: 100%; box-sizing: border-box;">
                                    </div>
                                    <div class="form-group">
                                        <label style="font-family: 'Space Grotesk', sans-serif; font-size: 11px; color: var(--text-muted); display: block; margin-bottom: 5px;">Remote ASN</label>
                                        <input type="number" id="t0-bgp-remote-asn" placeholder="e.g. 65000" class="form-input" style="width: 100%; box-sizing: border-box;">
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div style="display: flex; justify-content: flex-end; gap: 10px; margin-top: 15px; border-top: 1px solid var(--border-color); padding-top: 15px;">
                            <button type="button" id="btn-cancel-create-t0" class="btn btn-secondary" style="padding: 8px 16px; font-family: 'Space Grotesk', sans-serif;">Cancel</button>
                            <button type="submit" id="btn-submit-t0" class="btn btn-primary" style="padding: 8px 16px; font-family: 'Space Grotesk', sans-serif;">Deploy Gateway</button>
                        </div>
                    </form>
                </div>
            </div>
        `;

        document.body.appendChild(modalDiv);
        populateInterfaceDropdown('t0-uplink-interface', null, 't0-uplink-ip', 't0-gateway-ip');

        const close = () => modalDiv.remove();
        document.getElementById('btn-close-create-t0').addEventListener('click', close);
        document.getElementById('btn-cancel-create-t0').addEventListener('click', close);

        const bgpToggle = document.getElementById('t0-bgp-enabled');
        const bgpSection = document.getElementById('t0-bgp-section');
        if (bgpToggle && bgpSection) {
            bgpToggle.addEventListener('change', () => {
                bgpSection.style.display = bgpToggle.checked ? 'flex' : 'none';
            });
        }

        document.getElementById('create-t0-form').addEventListener('submit', async (e) => {
            e.preventDefault();
            const btn = document.getElementById('btn-submit-t0');
            if (btn) btn.disabled = true;

            const payload = {
                name: document.getElementById('t0-name').value.trim(),
                uplink_interface: document.getElementById('t0-uplink-interface').value.trim(),
                uplink_ip: document.getElementById('t0-uplink-ip').value.trim(),
                gateway_ip: document.getElementById('t0-gateway-ip').value.trim(),
                bgp_enabled: document.getElementById('t0-bgp-enabled').checked,
                nat_rules: {
                    source_nat: document.getElementById('t0-nat-masq').checked ? "masquerade" : "disabled",
                    bgp_enabled: document.getElementById('t0-bgp-enabled').checked,
                    bgp_local_asn: parseInt(document.getElementById('t0-bgp-local-asn').value) || 0,
                    bgp_neighbor_ip: document.getElementById('t0-bgp-neighbor-ip').value.trim(),
                    bgp_remote_asn: parseInt(document.getElementById('t0-bgp-remote-asn').value) || 0
                }
            };

            try {
                const res = await fetch(`${state.apiHost}/api/urbosa/t0/create`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
                const data = await res.json();
                if (res.ok) {
                    showToast('Success', data.message || 'T0 Router deployed.', 'success');
                    close();
                    if (data.task_id && typeof window.updateTasksList === 'function') {
                        window.updateTasksList();
                    }
                    setTimeout(() => window.location.reload(), 1500);
                } else {
                    showToast('Error', data.error || 'Failed to deploy T0 Router.', 'error');
                }
            } catch (err) {
                showToast('Error', 'Network error deploying T0 router', 'error');
            } finally {
                if (btn) btn.disabled = false;
            }
        });
    }

    function handleEditT0Modal(id, currentName, uplinkInterface, uplinkIp, gatewayIp, natMasq, bgpEnabled, bgpLocalAsn, bgpNeighborIp, bgpRemoteAsn) {
        const existing = document.getElementById('edit-t0-modal');
        if (existing) existing.remove();

        const modalDiv = document.createElement('div');
        modalDiv.id = 'edit-t0-modal';
        modalDiv.className = 'modal-backdrop';
        modalDiv.style.display = 'flex';
        modalDiv.style.alignItems = 'center';
        modalDiv.style.justifyContent = 'center';
        modalDiv.style.zIndex = '2000';

        const isBgpChecked = bgpEnabled ? 'checked' : '';
        const isNatChecked = natMasq ? 'checked' : '';
        const bgpStyle = bgpEnabled ? 'display: flex;' : 'display: none;';

        modalDiv.innerHTML = `
            <div class="modal-container" style="width: 1050px; max-width: 95vw; background: var(--bg-card); backdrop-filter: blur(20px); border: 1px solid var(--border-color); border-radius: 12px; padding: 20px; box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37); overflow-y: auto; max-height: 95vh;">
                <div class="modal-header" style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; border-bottom: 1px solid var(--border-color); padding-bottom: 10px;">
                    <h3 style="font-family: 'Space Grotesk', sans-serif; font-size: 18px; font-weight: 600; color: #fff; margin: 0;">Edit Tier-0 Logical Router</h3>
                    <button id="btn-close-edit-t0" style="background: none; border: none; color: var(--text-primary); font-size: 24px; cursor: pointer; line-height: 1; padding: 0;">&times;</button>
                </div>
                <div class="modal-body">
                    <form id="edit-t0-form" style="display: flex; flex-direction: column; gap: 15px;">
                        <div class="form-grid" style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 20px;">
                            <!-- Column 1: Identification & Interface -->
                            <div style="display: flex; flex-direction: column; gap: 12px;">
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Router Name</label>
                                    <input type="text" readonly class="form-input" value="${currentName}" style="width: 100%; box-sizing: border-box; background: rgba(255, 255, 255, 0.05); border: 1px solid var(--border-color); color: #aaa; padding: 8px; border-radius: 4px; cursor: not-allowed;">
                                </div>
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Uplink Interface <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <select id="edit-t0-uplink-interface" class="form-input" style="width: 100%; box-sizing: border-box;">
                                        <option value="ens192" ${uplinkInterface === 'ens192' ? 'selected' : ''}>ens192 (ESXi Default)</option>
                                        <option value="ens3" ${uplinkInterface === 'ens3' ? 'selected' : ''}>ens3 (KVM/Proxmox Default)</option>
                                        <option value="ens33" ${uplinkInterface === 'ens33' ? 'selected' : ''}>ens33 (VMware Workstation Default)</option>
                                        <option value="eth0" ${uplinkInterface === 'eth0' ? 'selected' : ''}>eth0 (Standard Linux)</option>
                                        <option value="eno1" ${uplinkInterface === 'eno1' ? 'selected' : ''}>eno1 (Bare Metal Default)</option>
                                        ${['ens192', 'ens3', 'ens33', 'eth0', 'eno1'].includes(uplinkInterface) ? '' : `<option value="${uplinkInterface}" selected>${uplinkInterface} (Current)</option>`}
                                    </select>
                                </div>
                            </div>
                            <!-- Column 2: IP Configuration & NAT -->
                            <div style="display: flex; flex-direction: column; gap: 12px; border-left: 1px solid var(--border-color); padding-left: 20px;">
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Uplink External IP / CIDR <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <input type="text" id="edit-t0-uplink-ip" required value="${uplinkIp}" class="form-input" style="width: 100%; box-sizing: border-box;">
                                </div>
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Upstream Gateway IP <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <input type="text" id="edit-t0-gateway-ip" required value="${gatewayIp}" class="form-input" style="width: 100%; box-sizing: border-box;">
                                </div>
                                <div class="form-group" style="margin-top: 5px;">
                                    <label style="display: flex; align-items: center; gap: 8px; font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); cursor: pointer;">
                                        <input type="checkbox" id="edit-t0-nat-masq" ${isNatChecked} style="accent-color: var(--color-primary); width: 16px; height: 16px;">
                                        Enable Outbound Source NAT (Masquerade)
                                    </label>
                                </div>
                            </div>
                            <!-- Column 3: BGP Configuration -->
                            <div style="display: flex; flex-direction: column; gap: 12px; border-left: 1px solid var(--border-color); padding-left: 20px;">
                                <div class="form-group">
                                    <label style="display: flex; align-items: center; gap: 8px; font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); cursor: pointer;">
                                        <input type="checkbox" id="edit-t0-bgp-enabled" ${isBgpChecked} style="accent-color: var(--color-primary); width: 16px; height: 16px;">
                                        Enable Dynamic Routing (BGP Peering)
                                    </label>
                                </div>
                                <div id="edit-t0-bgp-section" style="${bgpStyle} flex-direction: column; gap: 12px; padding-top: 5px;">
                                    <div class="form-group">
                                        <label style="font-family: 'Space Grotesk', sans-serif; font-size: 11px; color: var(--text-muted); display: block; margin-bottom: 5px;">Local ASN</label>
                                        <input type="number" id="edit-t0-bgp-local-asn" value="${bgpLocalAsn}" placeholder="e.g. 65001" class="form-input" style="width: 100%; box-sizing: border-box;">
                                    </div>
                                    <div class="form-group">
                                        <label style="font-family: 'Space Grotesk', sans-serif; font-size: 11px; color: var(--text-muted); display: block; margin-bottom: 5px;">Neighbor IP</label>
                                        <input type="text" id="edit-t0-bgp-neighbor-ip" value="${bgpNeighborIp}" placeholder="e.g. 10.10.102.254" class="form-input" style="width: 100%; box-sizing: border-box;">
                                    </div>
                                    <div class="form-group">
                                        <label style="font-family: 'Space Grotesk', sans-serif; font-size: 11px; color: var(--text-muted); display: block; margin-bottom: 5px;">Remote ASN</label>
                                        <input type="number" id="edit-t0-bgp-remote-asn" value="${bgpRemoteAsn}" placeholder="e.g. 65000" class="form-input" style="width: 100%; box-sizing: border-box;">
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div style="display: flex; justify-content: flex-end; gap: 10px; margin-top: 15px; border-top: 1px solid var(--border-color); padding-top: 15px;">
                            <button type="button" id="btn-cancel-edit-t0" class="btn btn-secondary" style="padding: 8px 16px; font-family: 'Space Grotesk', sans-serif;">Cancel</button>
                            <button type="submit" id="btn-submit-edit-t0" class="btn btn-primary" style="padding: 8px 16px; font-family: 'Space Grotesk', sans-serif;">Save Changes</button>
                        </div>
                    </form>
                </div>
        `;

        document.body.appendChild(modalDiv);
        populateInterfaceDropdown('edit-t0-uplink-interface', uplinkInterface, 'edit-t0-uplink-ip', 'edit-t0-gateway-ip');

        const close = () => modalDiv.remove();
        document.getElementById('btn-close-edit-t0').addEventListener('click', close);
        document.getElementById('btn-cancel-edit-t0').addEventListener('click', close);

        const bgpToggle = document.getElementById('edit-t0-bgp-enabled');
        const bgpSection = document.getElementById('edit-t0-bgp-section');
        if (bgpToggle && bgpSection) {
            bgpToggle.addEventListener('change', () => {
                bgpSection.style.display = bgpToggle.checked ? 'flex' : 'none';
            });
        }

        document.getElementById('edit-t0-form').addEventListener('submit', async (e) => {
            e.preventDefault();
            const btn = document.getElementById('btn-submit-edit-t0');
            if (btn) btn.disabled = true;

            const payload = {
                router_id: id,
                name: currentName,
                uplink_interface: document.getElementById('edit-t0-uplink-interface').value.trim(),
                uplink_ip: document.getElementById('edit-t0-uplink-ip').value.trim(),
                gateway_ip: document.getElementById('edit-t0-gateway-ip').value.trim(),
                nat_rules: {
                    source_nat: document.getElementById('edit-t0-nat-masq').checked ? "masquerade" : "disabled",
                    bgp_enabled: document.getElementById('edit-t0-bgp-enabled').checked,
                    bgp_local_asn: parseInt(document.getElementById('edit-t0-bgp-local-asn').value) || 0,
                    bgp_neighbor_ip: document.getElementById('edit-t0-bgp-neighbor-ip').value.trim(),
                    bgp_remote_asn: parseInt(document.getElementById('edit-t0-bgp-remote-asn').value) || 0
                }
            };

            try {
                const res = await fetch(`${state.apiHost}/api/urbosa/t0/update`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
                const data = await res.json();
                if (res.ok) {
                    showToast('Success', data.message || 'T0 Router updated.', 'success');
                    close();
                    if (data.task_id && typeof window.updateTasksList === 'function') {
                        window.updateTasksList();
                    }
                    setTimeout(() => window.location.reload(), 1500);
                } else {
                    showToast('Error', data.error || 'Failed to update T0 Router.', 'error');
                }
            } catch (err) {
                showToast('Error', 'Network error updating T0 router', 'error');
            } finally {
                if (btn) btn.disabled = false;
            }
        });
    }

    function handleCreateT1Modal() {
        const existing = document.getElementById('create-t1-modal');
        if (existing) existing.remove();

        const modalDiv = document.createElement('div');
        modalDiv.id = 'create-t1-modal';
        modalDiv.className = 'modal-backdrop';
        modalDiv.style.display = 'flex';
        modalDiv.style.alignItems = 'center';
        modalDiv.style.justifyContent = 'center';
        modalDiv.style.zIndex = '2000';

        let t0Options = '';
        state.urbosaT0.forEach(router => {
            t0Options += `<option value="${router.router_id}">${router.name} (${router.uplink_ip})</option>`;
        });

        modalDiv.innerHTML = `
            <div class="modal-container" style="width: 1050px; max-width: 95vw; background: var(--bg-card); backdrop-filter: blur(20px); border: 1px solid var(--border-color); border-radius: 12px; padding: 20px; box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);">
                <div class="modal-header" style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; border-bottom: 1px solid var(--border-color); padding-bottom: 10px;">
                    <h3 style="font-family: 'Space Grotesk', sans-serif; font-size: 18px; font-weight: 600; color: #fff; margin: 0;">Create Tier-1 Logical Router</h3>
                    <button id="btn-close-create-t1" style="background: none; border: none; color: var(--text-primary); font-size: 24px; cursor: pointer; line-height: 1; padding: 0;">&times;</button>
                </div>
                <div class="modal-body">
                    <form id="create-t1-form" style="display: flex; flex-direction: column; gap: 15px;">
                        <div class="form-grid" style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 20px;">
                            <!-- Column 1: Identification -->
                            <div style="display: flex; flex-direction: column; gap: 12px;">
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Router Name <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <input type="text" id="t1-name" required placeholder="e.g. T1-Tenant-Router-A" class="form-input" style="width: 100%; box-sizing: border-box;">
                                </div>
                            </div>
                            <!-- Column 2: Gateway Link -->
                            <div style="display: flex; flex-direction: column; gap: 12px; border-left: 1px solid var(--border-color); padding-left: 20px;">
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Linked Tier-0 Gateway <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <select id="t1-t0-link" required class="form-input" style="width: 100%; box-sizing: border-box;">
                                        <option value="" disabled selected>Select T0 parent gateway...</option>
                                        ${t0Options}
                                    </select>
                                    <span class="input-hint" style="font-size: 9px; color: var(--text-muted); display: block; margin-top: 2px;">Parent gateway for default northbound routing.</span>
                                </div>
                            </div>
                            <!-- Column 3: Services -->
                            <div style="display: flex; flex-direction: column; gap: 12px; border-left: 1px solid var(--border-color); padding-left: 20px;">
                                <div class="form-group" style="margin-top: 10px;">
                                    <label style="display: flex; align-items: center; gap: 8px; font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); cursor: pointer;">
                                        <input type="checkbox" id="t1-dhcp-enabled" checked style="accent-color: var(--color-primary); width: 16px; height: 16px;">
                                        Enable DHCP & IPAM Server settings
                                    </label>
                                    <span class="input-hint" style="font-size: 9px; color: var(--text-muted); display: block; margin-top: 2px; margin-left: 24px;">Distributes IP leases to attached overlay virtual machines.</span>
                                </div>
                            </div>
                        </div>
                        <div style="display: flex; justify-content: flex-end; gap: 10px; margin-top: 15px; border-top: 1px solid var(--border-color); padding-top: 15px;">
                            <button type="button" id="btn-cancel-create-t1" class="btn btn-secondary" style="padding: 8px 16px; font-family: 'Space Grotesk', sans-serif;">Cancel</button>
                            <button type="submit" id="btn-submit-t1" class="btn btn-primary" style="padding: 8px 16px; font-family: 'Space Grotesk', sans-serif;">Deploy Router</button>
                        </div>
                    </form>
                </div>
            </div>
        `;

        document.body.appendChild(modalDiv);

        const close = () => modalDiv.remove();
        document.getElementById('btn-close-create-t1').addEventListener('click', close);
        document.getElementById('btn-cancel-create-t1').addEventListener('click', close);

        document.getElementById('create-t1-form').addEventListener('submit', async (e) => {
            e.preventDefault();
            const btn = document.getElementById('btn-submit-t1');
            if (btn) btn.disabled = true;

            const payload = {
                name: document.getElementById('t1-name').value.trim(),
                t0_link_id: document.getElementById('t1-t0-link').value,
                dhcp_enabled: document.getElementById('t1-dhcp-enabled').checked
            };

            try {
                const res = await fetch(`${state.apiHost}/api/urbosa/t1/create`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
                const data = await res.json();
                if (res.ok) {
                    showToast('Success', data.message || 'T1 Router deployed.', 'success');
                    close();
                    if (data.task_id && typeof window.updateTasksList === 'function') {
                        window.updateTasksList();
                    }
                    setTimeout(() => window.location.reload(), 1500);
                } else {
                    showToast('Error', data.error || 'Failed to deploy T1 Router.', 'error');
                }
            } catch (err) {
                showToast('Error', 'Network error deploying T1 router', 'error');
            } finally {
                if (btn) btn.disabled = false;
            }
        });
    }

    function handleEditT1Modal(id, currentName, currentT0LinkId, dhcpEnabled) {
        const existing = document.getElementById('edit-t1-modal');
        if (existing) existing.remove();

        const modalDiv = document.createElement('div');
        modalDiv.id = 'edit-t1-modal';
        modalDiv.className = 'modal-backdrop';
        modalDiv.style.display = 'flex';
        modalDiv.style.alignItems = 'center';
        modalDiv.style.justifyContent = 'center';
        modalDiv.style.zIndex = '2000';

        let t0Options = '';
        state.urbosaT0.forEach(router => {
            t0Options += `<option value="${router.router_id}" ${router.router_id === currentT0LinkId ? 'selected' : ''}>${router.name} (${router.uplink_ip})</option>`;
        });

        const isChecked = dhcpEnabled ? 'checked' : '';

        modalDiv.innerHTML = `
            <div class="modal-container" style="width: 1050px; max-width: 95vw; background: var(--bg-card); backdrop-filter: blur(20px); border: 1px solid var(--border-color); border-radius: 12px; padding: 20px; box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);">
                <div class="modal-header" style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; border-bottom: 1px solid var(--border-color); padding-bottom: 10px;">
                    <h3 style="font-family: 'Space Grotesk', sans-serif; font-size: 18px; font-weight: 600; color: #fff; margin: 0;">Edit Tier-1 Logical Router</h3>
                    <button id="btn-close-edit-t1" style="background: none; border: none; color: var(--text-primary); font-size: 24px; cursor: pointer; line-height: 1; padding: 0;">&times;</button>
                </div>
                <div class="modal-body">
                    <form id="edit-t1-form" style="display: flex; flex-direction: column; gap: 15px;">
                        <div class="form-grid" style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 20px;">
                            <!-- Column 1 -->
                            <div style="display: flex; flex-direction: column; gap: 12px;">
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Router Name</label>
                                    <input type="text" readonly class="form-input" value="${currentName}" style="width: 100%; box-sizing: border-box; background: rgba(255, 255, 255, 0.05); border: 1px solid var(--border-color); color: #aaa; padding: 8px; border-radius: 4px; cursor: not-allowed;">
                                </div>
                            </div>
                            <!-- Column 2 -->
                            <div style="display: flex; flex-direction: column; gap: 12px; border-left: 1px solid var(--border-color); padding-left: 20px;">
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Linked Tier-0 Gateway <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <select id="edit-t1-t0-link" required class="form-input" style="width: 100%; box-sizing: border-box;">
                                        ${t0Options}
                                    </select>
                                </div>
                            </div>
                            <!-- Column 3 -->
                            <div style="display: flex; flex-direction: column; gap: 12px; border-left: 1px solid var(--border-color); padding-left: 20px;">
                                <div class="form-group" style="margin-top: 10px;">
                                    <label style="display: flex; align-items: center; gap: 8px; font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); cursor: pointer;">
                                        <input type="checkbox" id="edit-t1-dhcp-enabled" ${isChecked} style="accent-color: var(--color-primary); width: 16px; height: 16px;">
                                        Enable DHCP & IPAM Server settings
                                    </label>
                                </div>
                            </div>
                        </div>
                        <div style="display: flex; justify-content: flex-end; gap: 10px; margin-top: 15px; border-top: 1px solid var(--border-color); padding-top: 15px;">
                            <button type="button" id="btn-cancel-edit-t1" class="btn btn-secondary" style="padding: 8px 16px; font-family: 'Space Grotesk', sans-serif;">Cancel</button>
                            <button type="submit" id="btn-submit-edit-t1" class="btn btn-primary" style="padding: 8px 16px; font-family: 'Space Grotesk', sans-serif;">Save Changes</button>
                        </div>
                    </form>
                </div>
            </div>
        `;

        document.body.appendChild(modalDiv);

        const close = () => modalDiv.remove();
        document.getElementById('btn-close-edit-t1').addEventListener('click', close);
        document.getElementById('btn-cancel-edit-t1').addEventListener('click', close);

        document.getElementById('edit-t1-form').addEventListener('submit', async (e) => {
            e.preventDefault();
            const btn = document.getElementById('btn-submit-edit-t1');
            if (btn) btn.disabled = true;

            const payload = {
                router_id: id,
                name: currentName,
                t0_link_id: document.getElementById('edit-t1-t0-link').value,
                dhcp_enabled: document.getElementById('edit-t1-dhcp-enabled').checked
            };

            try {
                const res = await fetch(`${state.apiHost}/api/urbosa/t1/update`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
                const data = await res.json();
                if (res.ok) {
                    showToast('Success', data.message || 'T1 Router updated.', 'success');
                    close();
                    if (data.task_id && typeof window.updateTasksList === 'function') {
                        window.updateTasksList();
                    }
                    setTimeout(() => window.location.reload(), 1500);
                } else {
                    showToast('Error', data.error || 'Failed to update T1 Router.', 'error');
                }
            } catch (err) {
                showToast('Error', 'Network error updating T1 router', 'error');
            } finally {
                if (btn) btn.disabled = false;
            }
        });
    }

    function handleCreateSegmentModal() {
        const existing = document.getElementById('create-segment-modal');
        if (existing) existing.remove();

        const modalDiv = document.createElement('div');
        modalDiv.id = 'create-segment-modal';
        modalDiv.className = 'modal-backdrop';
        modalDiv.style.display = 'flex';
        modalDiv.style.alignItems = 'center';
        modalDiv.style.justifyContent = 'center';
        modalDiv.style.zIndex = '2000';

        let t1Options = '';
        state.urbosaT1.forEach(router => {
            t1Options += `<option value="${router.router_id}">${router.name}</option>`;
        });

        // Determine next unused VNI
        let nextVni = 10000;
        if (state.urbosaSegments) {
            const existingVnis = state.urbosaSegments.map(s => parseInt(s.vni)).filter(v => !isNaN(v));
            while (existingVnis.includes(nextVni)) {
                nextVni++;
            }
        }

        modalDiv.innerHTML = `
            <div class="modal-container" style="width: 1050px; max-width: 95vw; background: var(--bg-card); backdrop-filter: blur(20px); border: 1px solid var(--border-color); border-radius: 12px; padding: 20px; box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);">
                <div class="modal-header" style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; border-bottom: 1px solid var(--border-color); padding-bottom: 10px;">
                    <h3 style="font-family: 'Space Grotesk', sans-serif; font-size: 18px; font-weight: 600; color: #fff; margin: 0;">Create VXLAN Overlay Segment</h3>
                    <button id="btn-close-create-segment" style="background: none; border: none; color: var(--text-primary); font-size: 24px; cursor: pointer; line-height: 1; padding: 0;">&times;</button>
                </div>
                <div class="modal-body">
                    <form id="create-segment-form-sdn" style="display: flex; flex-direction: column; gap: 15px;">
                        <div class="form-grid" style="display: grid; grid-template-columns: 1.2fr 1fr 1fr; gap: 20px;">
                            <!-- Column 1: Subnet Info -->
                            <div style="display: flex; flex-direction: column; gap: 12px;">
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Segment Name <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <input type="text" id="seg-name" required placeholder="e.g. Production-Overlay" class="form-input" style="width: 100%; box-sizing: border-box;">
                                </div>
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">VXLAN VNI (10000 - 16777215) <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <input type="number" id="seg-vni" min="10000" max="16777215" value="${nextVni}" required class="form-input" style="width: 100%; box-sizing: border-box;">
                                </div>
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Subnet Range (CIDR) <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <input type="text" id="seg-subnet" required placeholder="e.g. 10.0.10.0/24" class="form-input" style="width: 100%; box-sizing: border-box;">
                                </div>
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Subnet Default Gateway IP <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <input type="text" id="seg-gateway" required placeholder="e.g. 10.0.10.1" class="form-input" style="width: 100%; box-sizing: border-box;">
                                </div>
                            </div>
                            <!-- Column 2: Router & DHCP IPAM Switch -->
                            <div style="display: flex; flex-direction: column; gap: 12px; border-left: 1px solid var(--border-color); padding-left: 20px;">
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Attached Tier-1 Router <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <select id="seg-t1-link" required class="form-input" style="width: 100%; box-sizing: border-box;">
                                        <option value="" disabled selected>Select T1 router...</option>
                                        ${t1Options}
                                    </select>
                                </div>
                                <div class="form-group" style="margin-top: 10px;">
                                    <label style="display: flex; align-items: center; gap: 8px; font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); cursor: pointer;">
                                        <input type="checkbox" id="seg-dhcp-enabled" checked style="accent-color: var(--color-primary); width: 16px; height: 16px;">
                                        Enable DHCP IPAM Server
                                    </label>
                                </div>
                            </div>
                            <!-- Column 3: DHCP IPAM Range Details -->
                            <div style="display: flex; flex-direction: column; gap: 12px; border-left: 1px solid var(--border-color); padding-left: 20px;">
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 11px; color: var(--text-muted); display: block; margin-bottom: 5px;">DHCP Range Start</label>
                                    <input type="text" id="seg-dhcp-start" placeholder="e.g. 10.0.10.100" class="form-input" style="width: 100%; box-sizing: border-box;">
                                </div>
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 11px; color: var(--text-muted); display: block; margin-bottom: 5px;">DHCP Range End</label>
                                    <input type="text" id="seg-dhcp-end" placeholder="e.g. 10.0.10.250" class="form-input" style="width: 100%; box-sizing: border-box;">
                                </div>
                            </div>
                        </div>
                        <div style="display: flex; justify-content: flex-end; gap: 10px; margin-top: 15px; border-top: 1px solid var(--border-color); padding-top: 15px;">
                            <button type="button" id="btn-cancel-create-segment" class="btn btn-secondary" style="padding: 8px 16px; font-family: 'Space Grotesk', sans-serif;">Cancel</button>
                            <button type="submit" id="btn-submit-segment-sdn" class="btn btn-primary" style="padding: 8px 16px; font-family: 'Space Grotesk', sans-serif;">Provision Segment</button>
                        </div>
                    </form>
                </div>
            </div>
        `;

        document.body.appendChild(modalDiv);

        const close = () => modalDiv.remove();
        document.getElementById('btn-close-create-segment').addEventListener('click', close);
        document.getElementById('btn-cancel-create-segment').addEventListener('click', close);

        // Auto-suggest Gateway IP and DHCP range based on subnet input
        const segSubnetInput = document.getElementById('seg-subnet');
        if (segSubnetInput) {
            segSubnetInput.addEventListener('input', () => {
                const val = segSubnetInput.value.trim();
                const match = val.match(/^(\d{1,3}\.\d{1,3}\.\d{1,3}\.)0\/24$/);
                if (match) {
                    const prefix = match[1];
                    const gwInput = document.getElementById('seg-gateway');
                    if (gwInput && !gwInput.value) gwInput.value = prefix + '1';
                    const startInput = document.getElementById('seg-dhcp-start');
                    if (startInput && !startInput.value) startInput.value = prefix + '100';
                    const endInput = document.getElementById('seg-dhcp-end');
                    if (endInput && !endInput.value) endInput.value = prefix + '250';
                }
            });
        }

        document.getElementById('create-segment-form-sdn').addEventListener('submit', async (e) => {
            e.preventDefault();
            const btn = document.getElementById('btn-submit-segment-sdn');
            if (btn) btn.disabled = true;

            const payload = {
                name: document.getElementById('seg-name').value.trim(),
                vni: parseInt(document.getElementById('seg-vni').value),
                t1_link_id: document.getElementById('seg-t1-link').value,
                subnet_cidr: document.getElementById('seg-subnet').value.trim(),
                gateway_ip: document.getElementById('seg-gateway').value.trim() || (document.getElementById('seg-subnet').value.split('.')[0] + '.' + document.getElementById('seg-subnet').value.split('.')[1] + '.' + document.getElementById('seg-subnet').value.split('.')[2] + '.1'),
                dhcp_enabled: document.getElementById('seg-dhcp-enabled').checked,
                dhcp_start: document.getElementById('seg-dhcp-start').value.trim(),
                dhcp_end: document.getElementById('seg-dhcp-end').value.trim()
            };

            try {
                const res = await fetch(`${state.apiHost}/api/urbosa/segments/create`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
                const data = await res.json();
                if (res.ok) {
                    showToast('Success', data.message || 'Segment provisioned.', 'success');
                    close();
                    if (data.task_id && typeof window.updateTasksList === 'function') {
                        window.updateTasksList();
                    }
                    setTimeout(() => window.location.reload(), 1500);
                } else {
                    showToast('Error', data.error || 'Failed to provision Segment.', 'error');
                }
            } catch (err) {
                showToast('Error', 'Network error provisioning Segment', 'error');
            } finally {
                if (btn) btn.disabled = false;
            }
        });
    }

    function handleCreateFirewallModal() {
        const existing = document.getElementById('create-firewall-modal');
        if (existing) existing.remove();

        const modalDiv = document.createElement('div');
        modalDiv.id = 'create-firewall-modal';
        modalDiv.className = 'modal-backdrop';
        modalDiv.style.display = 'flex';
        modalDiv.style.alignItems = 'center';
        modalDiv.style.justifyContent = 'center';
        modalDiv.style.zIndex = '2000';

        modalDiv.innerHTML = `
            <div class="modal-container" style="width: 780px; max-width: 90vw; background: var(--bg-card); backdrop-filter: blur(20px); border: 1px solid var(--border-color); border-radius: 12px; padding: 20px; box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);">
                <div class="modal-header" style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; border-bottom: 1px solid var(--border-color); padding-bottom: 10px;">
                    <h3 style="font-family: 'Space Grotesk', sans-serif; font-size: 18px; font-weight: 600; color: #fff; margin: 0;">Add Distributed Firewall (DFW) Rule</h3>
                    <button id="btn-close-create-firewall" style="background: none; border: none; color: var(--text-primary); font-size: 24px; cursor: pointer; line-height: 1; padding: 0;">&times;</button>
                </div>
                <div class="modal-body">
                    <form id="create-firewall-form" style="display: flex; flex-direction: column; gap: 15px;">
                        <div class="form-grid" style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px;">
                            <!-- Column 1 -->
                            <div style="display: flex; flex-direction: column; gap: 12px;">
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Rule Priority (1 - 1000) <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <input type="number" id="fw-priority" min="1" max="1000" value="100" required class="form-input" style="width: 100%; box-sizing: border-box;">
                                </div>
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Rule Description <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <input type="text" id="fw-description" required placeholder="e.g. Block SSH to DB segment" class="form-input" style="width: 100%; box-sizing: border-box;">
                                </div>
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Source IP / CIDR <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <input type="text" id="fw-source" required value="ANY" class="form-input" style="width: 100%; box-sizing: border-box;">
                                </div>
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Destination IP / CIDR <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <input type="text" id="fw-dest" required value="ANY" class="form-input" style="width: 100%; box-sizing: border-box;">
                                </div>
                            </div>
                            <!-- Column 2 -->
                            <div style="display: flex; flex-direction: column; gap: 12px;">
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">IP Protocol <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <select id="fw-protocol" required class="form-input" style="width: 100%; box-sizing: border-box;">
                                        <option value="ANY">ANY (All Protocols)</option>
                                        <option value="TCP">TCP</option>
                                        <option value="UDP">UDP</option>
                                        <option value="ICMP">ICMP</option>
                                    </select>
                                </div>
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Target Port <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <input type="number" id="fw-port" min="0" max="65535" value="0" required class="form-input" style="width: 100%; box-sizing: border-box;">
                                </div>
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Enforcement Action <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <select id="fw-action" required class="form-input" style="width: 100%; box-sizing: border-box;">
                                        <option value="ALLOW">ALLOW</option>
                                        <option value="DENY">DENY</option>
                                    </select>
                                </div>
                            </div>
                        </div>
                        <div style="display: flex; justify-content: flex-end; gap: 10px; margin-top: 15px; border-top: 1px solid var(--border-color); padding-top: 15px;">
                            <button type="button" id="btn-cancel-create-firewall" class="btn btn-secondary" style="padding: 8px 16px; font-family: 'Space Grotesk', sans-serif;">Cancel</button>
                            <button type="submit" id="btn-submit-firewall" class="btn btn-primary" style="padding: 8px 16px; font-family: 'Space Grotesk', sans-serif;">Deploy Rule</button>
                        </div>
                    </form>
                </div>
            </div>
        `;

        document.body.appendChild(modalDiv);

        const close = () => modalDiv.remove();
        document.getElementById('btn-close-create-firewall').addEventListener('click', close);
        document.getElementById('btn-cancel-create-firewall').addEventListener('click', close);

        document.getElementById('create-firewall-form').addEventListener('submit', async (e) => {
            e.preventDefault();
            const btn = document.getElementById('btn-submit-firewall');
            if (btn) btn.disabled = true;

            const payload = {
                priority: parseInt(document.getElementById('fw-priority').value),
                description: document.getElementById('fw-description').value.trim(),
                source_ip: document.getElementById('fw-source').value.trim(),
                dest_ip: document.getElementById('fw-dest').value.trim(),
                protocol: document.getElementById('fw-protocol').value,
                port: parseInt(document.getElementById('fw-port').value),
                action: document.getElementById('fw-action').value
            };

            try {
                const res = await fetch(`${state.apiHost}/api/urbosa/firewall/create`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
                const data = await res.json();
                if (res.ok) {
                    showToast('Success', data.message || 'Firewall rule created.', 'success');
                    close();
                    if (data.task_id && typeof window.updateTasksList === 'function') {
                        window.updateTasksList();
                    }
                    setTimeout(() => window.location.reload(), 1500);
                } else {
                    showToast('Error', data.error || 'Failed to create firewall rule.', 'error');
                }
            } catch (err) {
                showToast('Error', 'Network error creating firewall rule', 'error');
            } finally {
                if (btn) btn.disabled = false;
            }
        });
    }

    function handleEditFirewallModal(id, priority, description, source, dest, protocol, port, action) {
        const existing = document.getElementById('edit-firewall-modal');
        if (existing) existing.remove();

        const modalDiv = document.createElement('div');
        modalDiv.id = 'edit-firewall-modal';
        modalDiv.className = 'modal-backdrop';
        modalDiv.style.display = 'flex';
        modalDiv.style.alignItems = 'center';
        modalDiv.style.justifyContent = 'center';
        modalDiv.style.zIndex = '2000';

        modalDiv.innerHTML = `
            <div class="modal-container" style="width: 780px; max-width: 90vw; background: var(--bg-card); backdrop-filter: blur(20px); border: 1px solid var(--border-color); border-radius: 12px; padding: 20px; box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);">
                <div class="modal-header" style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; border-bottom: 1px solid var(--border-color); padding-bottom: 10px;">
                    <h3 style="font-family: 'Space Grotesk', sans-serif; font-size: 18px; font-weight: 600; color: #fff; margin: 0;">Edit Distributed Firewall Rule</h3>
                    <button id="btn-close-edit-firewall" style="background: none; border: none; color: var(--text-primary); font-size: 24px; cursor: pointer; line-height: 1; padding: 0;">&times;</button>
                </div>
                <div class="modal-body">
                    <form id="edit-firewall-form" style="display: flex; flex-direction: column; gap: 15px;">
                        <div class="form-grid" style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px;">
                            <!-- Column 1 -->
                            <div style="display: flex; flex-direction: column; gap: 12px;">
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Rule Priority (1 - 1000) <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <input type="number" id="edit-fw-priority" min="1" max="1000" value="${priority}" required class="form-input" style="width: 100%; box-sizing: border-box;">
                                </div>
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Rule Description <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <input type="text" id="edit-fw-description" required value="${description}" class="form-input" style="width: 100%; box-sizing: border-box;">
                                </div>
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Source IP / CIDR <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <input type="text" id="edit-fw-source" required value="${source}" class="form-input" style="width: 100%; box-sizing: border-box;">
                                </div>
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Destination IP / CIDR <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <input type="text" id="edit-fw-dest" required value="${dest}" class="form-input" style="width: 100%; box-sizing: border-box;">
                                </div>
                            </div>
                            <!-- Column 2 -->
                            <div style="display: flex; flex-direction: column; gap: 12px;">
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">IP Protocol <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <select id="edit-fw-protocol" required class="form-input" style="width: 100%; box-sizing: border-box;">
                                        <option value="ANY" ${protocol === 'ANY' ? 'selected' : ''}>ANY (All Protocols)</option>
                                        <option value="TCP" ${protocol === 'TCP' ? 'selected' : ''}>TCP</option>
                                        <option value="UDP" ${protocol === 'UDP' ? 'selected' : ''}>UDP</option>
                                        <option value="ICMP" ${protocol === 'ICMP' ? 'selected' : ''}>ICMP</option>
                                    </select>
                                </div>
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Target Port <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <input type="number" id="edit-fw-port" min="0" max="65535" value="${port}" required class="form-input" style="width: 100%; box-sizing: border-box;">
                                </div>
                                <div class="form-group">
                                    <label style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: var(--text-primary); display: block; margin-bottom: 5px;">Enforcement Action <span class="required" style="color: var(--color-primary);">*</span></label>
                                    <select id="edit-fw-action" required class="form-input" style="width: 100%; box-sizing: border-box;">
                                        <option value="ALLOW" ${action === 'ALLOW' ? 'selected' : ''}>ALLOW</option>
                                        <option value="DENY" ${action === 'DENY' ? 'selected' : ''}>DENY</option>
                                    </select>
                                </div>
                            </div>
                        </div>
                        <div style="display: flex; justify-content: flex-end; gap: 10px; margin-top: 15px; border-top: 1px solid var(--border-color); padding-top: 15px;">
                            <button type="button" id="btn-cancel-edit-firewall" class="btn btn-secondary" style="padding: 8px 16px; font-family: 'Space Grotesk', sans-serif;">Cancel</button>
                            <button type="submit" id="btn-submit-edit-firewall" class="btn btn-primary" style="padding: 8px 16px; font-family: 'Space Grotesk', sans-serif;">Save Changes</button>
                        </div>
                    </form>
                </div>
            </div>
        `;

        document.body.appendChild(modalDiv);

        const close = () => modalDiv.remove();
        document.getElementById('btn-close-edit-firewall').addEventListener('click', close);
        document.getElementById('btn-cancel-edit-firewall').addEventListener('click', close);

        document.getElementById('edit-firewall-form').addEventListener('submit', async (e) => {
            e.preventDefault();
            const btn = document.getElementById('btn-submit-edit-firewall');
            if (btn) btn.disabled = true;

            const payload = {
                rule_id: id,
                priority: parseInt(document.getElementById('edit-fw-priority').value),
                description: document.getElementById('edit-fw-description').value.trim(),
                source_ip: document.getElementById('edit-fw-source').value.trim(),
                dest_ip: document.getElementById('edit-fw-dest').value.trim(),
                protocol: document.getElementById('edit-fw-protocol').value,
                port: parseInt(document.getElementById('edit-fw-port').value),
                action: document.getElementById('edit-fw-action').value
            };

            try {
                const res = await fetch(`${state.apiHost}/api/urbosa/firewall/update`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
                const data = await res.json();
                if (res.ok) {
                    showToast('Success', data.message || 'Firewall rule updated.', 'success');
                    close();
                    if (data.task_id && typeof window.updateTasksList === 'function') {
                        window.updateTasksList();
                    }
                    setTimeout(() => window.location.reload(), 1500);
                } else {
                    showToast('Error', data.error || 'Failed to update firewall rule.', 'error');
                }
            } catch (err) {
                showToast('Error', 'Network error updating firewall rule', 'error');
            } finally {
                if (btn) btn.disabled = false;
            }
        });
    }

    function setupUrbosaFormListeners() {
        const btnOpenCreateT0 = document.getElementById('btn-open-create-t0');
        if (btnOpenCreateT0) {
            btnOpenCreateT0.addEventListener('click', handleCreateT0Modal);
        }

        const btnOpenCreateT1 = document.getElementById('btn-open-create-t1');
        if (btnOpenCreateT1) {
            btnOpenCreateT1.addEventListener('click', handleCreateT1Modal);
        }

        const btnOpenCreateSeg = document.getElementById('btn-open-create-segment');
        if (btnOpenCreateSeg) {
            btnOpenCreateSeg.addEventListener('click', handleCreateSegmentModal);
        }

        const btnOpenCreateFw = document.getElementById('btn-open-create-firewall');
        if (btnOpenCreateFw) {
            btnOpenCreateFw.addEventListener('click', handleCreateFirewallModal);
        }
    }

    function renderUrbosaTopology() {
        try {
            const svg = document.getElementById('sdn-topology-svg');
            if (!svg) return;
            
            const nodesGroup = document.getElementById('topology-nodes-group');
            const linksGroup = document.getElementById('topology-links-group');
            if (!nodesGroup || !linksGroup) return;
            
            nodesGroup.innerHTML = '';
            linksGroup.innerHTML = '';
            
            const Y_UPLINK = 55;
            const Y_T0 = 175;
            const Y_T1 = 295;
            const Y_SEGMENT = 415;
            const Y_VM = 515;
            
            const nodes = [];
            const links = [];
            
            nodes.push({
                id: 'uplink',
                label: 'Physical Uplink (ens192)',
                type: 'uplink',
                x: 400,
                y: Y_UPLINK,
                details: {
                    name: 'Physical Uplink (ens192)',
                    interface: 'ens192',
                    mtu: '1500',
                    speed: '10 Gbps',
                    status: 'Connected',
                    subnet: '10.10.102.0/24'
                }
            });
            
            if (state.urbosaT0 && state.urbosaT0.length > 0) {
                state.urbosaT0.forEach((r, idx) => {
                    const count = state.urbosaT0.length;
                    const x = count === 1 ? 400 : 150 + (idx / (count - 1)) * 500;
                    const connectedT1s = state.urbosaT1 ? state.urbosaT1.filter(t1 => t1.t0_link_id === r.router_id) : [];
                    const transitIps = connectedT1s.map(t1 => `${t1.t0_transit_ip || 'N/A'} (to ${t1.name})`);
                    nodes.push({
                        id: r.router_id,
                        label: r.name,
                        type: 't0',
                        x: x,
                        y: Y_T0,
                        data: r,
                        details: {
                            name: r.name,
                            id: r.router_id,
                            interface: r.uplink_interface,
                            ip: r.uplink_ip,
                            gateway: r.gateway_ip,
                            bgp: (r.bgp_enabled === true || r.bgp_enabled === 'true') ? 'Enabled' : 'Disabled',
                            bgp_local_asn: r.bgp_local_asn || 'N/A',
                            bgp_neighbor: r.bgp_neighbor_ip || 'N/A',
                            bgp_remote_asn: r.bgp_remote_asn || 'N/A',
                            nat: 'Source NAT (masquerade)',
                            transit_ips: transitIps
                        }
                    });
                    
                    links.push({
                        from: 'uplink',
                        to: r.router_id,
                        type: 'uplink-t0',
                        active: true
                    });
                });
            }
            
            if (state.urbosaT1 && state.urbosaT1.length > 0) {
                state.urbosaT1.forEach((r, idx) => {
                    const count = state.urbosaT1.length;
                    const x = count === 1 ? 400 : 150 + (idx / (count - 1)) * 500;
                    nodes.push({
                        id: r.router_id,
                        label: r.name,
                        type: 't1',
                        x: x,
                        y: Y_T1,
                        data: r,
                        details: {
                            name: r.name,
                            id: r.router_id,
                            t0_parent: r.t0_link_id,
                            dhcp: r.dhcp_enabled ? 'Enabled' : 'Disabled',
                            routing_mode: 'Distributed Logical Router (DLR)',
                            transit_ip: r.transit_ip || 'N/A',
                            t0_transit_ip: r.t0_transit_ip || 'N/A'
                        }
                    });
                    
                    if (r.t0_link_id) {
                        links.push({
                            from: r.t0_link_id,
                            to: r.router_id,
                            type: 't0-t1',
                            active: true
                        });
                    }
                });
            }
            
            if (state.urbosaSegments && state.urbosaSegments.length > 0) {
                state.urbosaSegments.forEach((s, idx) => {
                    const count = state.urbosaSegments.length;
                    const x = count === 1 ? 400 : 150 + (idx / (count - 1)) * 500;
                    nodes.push({
                        id: s.segment_id,
                        label: `${s.name} (${s.subnet_cidr})`,
                        type: 'segment',
                        x: x,
                        y: Y_SEGMENT,
                        data: s,
                        details: {
                            name: s.name,
                            vni: s.vni,
                            subnet: s.subnet_cidr,
                            gateway: s.gateway_ip,
                            t1_parent: s.t1_link_id,
                            overlay_type: 'VXLAN (Port 4789)'
                        }
                    });
                    
                    if (s.t1_link_id) {
                        links.push({
                            from: s.t1_link_id,
                            to: s.segment_id,
                            type: 't1-segment',
                            active: true
                        });
                    }
                });
            }
            
            const activeVms = state.vms || [];
            const connectedVmNodes = [];
            
            activeVms.forEach(vm => {
                let nids = [];
                try {
                    if (vm.network_id) {
                        let parsedNids = [];
                        if (typeof vm.network_id === 'string' && vm.network_id.startsWith('[')) {
                            parsedNids = JSON.parse(vm.network_id);
                        } else if (Array.isArray(vm.network_id)) {
                            parsedNids = vm.network_id;
                        } else {
                            parsedNids = [vm.network_id];
                        }
                        nids = parsedNids.map(id => typeof id === 'string' ? id.split(':')[0] : id);
                    }
                } catch(e) {}
                
                nids.forEach(nid => {
                    const seg = state.urbosaSegments && state.urbosaSegments.find(s => s.segment_id === nid);
                    if (seg) {
                        connectedVmNodes.push({
                            vm: vm,
                            segment_id: seg.segment_id
                        });
                    }
                });
            });
            
            if (connectedVmNodes.length > 0) {
                connectedVmNodes.forEach((cvm, idx) => {
                    const count = connectedVmNodes.length;
                    const x = count === 1 ? 400 : 100 + (idx / (count - 1)) * 600;
                    const vmNodeId = `vm-${cvm.vm.name}-${cvm.segment_id}`;
                    
                    nodes.push({
                        id: vmNodeId,
                        label: cvm.vm.name,
                        type: 'vm',
                        segment_id: cvm.segment_id,
                        x: x,
                        y: Y_VM,
                        data: cvm.vm,
                        details: {
                            name: cvm.vm.name,
                            status: cvm.vm.status,
                            vcpus: cvm.vm.vcpus ? `${cvm.vm.vcpus} vCPUs` : 'N/A',
                            memory: cvm.vm.memory ? `${cvm.vm.memory} MB` : 'N/A',
                            disk: cvm.vm.disk ? `${cvm.vm.disk} GB` : 'N/A',
                            node: cvm.vm.node || 'N/A',
                            ip: cvm.vm.ip_address || 'DHCP Resolving...'
                        }
                    });
                    
                    links.push({
                        from: cvm.segment_id,
                        to: vmNodeId,
                        type: 'segment-vm',
                        active: cvm.vm.status === 'running'
                    });
                });
            }
            
            const t0Map = {};
            const t1Map = {};
            const segmentMap = {};
            
            const t0Nodes = nodes.filter(n => n.type === 't0');
            t0Nodes.forEach((node, idx) => {
                const count = t0Nodes.length;
                node.x = count === 1 ? 400 : 150 + (idx / (count - 1)) * 500;
                t0Map[node.id] = node;
            });
            
            const t1Nodes = nodes.filter(n => n.type === 't1');
            const t1ByT0 = {};
            const unlinkedT1s = [];
            
            t1Nodes.forEach(node => {
                const t0ParentId = node.details.t0_parent;
                if (t0ParentId && t0Map[t0ParentId]) {
                    if (!t1ByT0[t0ParentId]) t1ByT0[t0ParentId] = [];
                    t1ByT0[t0ParentId].push(node);
                } else {
                    unlinkedT1s.push(node);
                }
            });
            
            Object.keys(t1ByT0).forEach(t0Id => {
                const t0Node = t0Map[t0Id];
                const childT1s = t1ByT0[t0Id];
                const count = childT1s.length;
                childT1s.forEach((node, idx) => {
                    const spacing = 160;
                    const offset = (idx - (count - 1) / 2) * spacing;
                    node.x = t0Node.x + offset;
                });
            });
            
            unlinkedT1s.forEach((node, idx) => {
                const count = unlinkedT1s.length;
                node.x = count === 1 ? 400 : 150 + (idx / (count - 1)) * 500;
            });
            
            t1Nodes.forEach(node => {
                t1Map[node.id] = node;
            });
            
            const segmentNodes = nodes.filter(n => n.type === 'segment');
            const segByT1 = {};
            const unlinkedSegs = [];
            
            segmentNodes.forEach(node => {
                const t1ParentId = node.details.t1_parent;
                if (t1ParentId && t1Map[t1ParentId]) {
                    if (!segByT1[t1ParentId]) segByT1[t1ParentId] = [];
                    segByT1[t1ParentId].push(node);
                } else {
                    unlinkedSegs.push(node);
                }
            });
            
            Object.keys(segByT1).forEach(t1Id => {
                const t1Node = t1Map[t1Id];
                const childSegs = segByT1[t1Id];
                const count = childSegs.length;
                childSegs.forEach((node, idx) => {
                    const spacing = 120;
                    const offset = (idx - (count - 1) / 2) * spacing;
                    node.x = t1Node.x + offset;
                });
            });
            
            unlinkedSegs.forEach((node, idx) => {
                const count = unlinkedSegs.length;
                node.x = count === 1 ? 400 : 150 + (idx / (count - 1)) * 500;
            });
            
            segmentNodes.forEach(node => {
                segmentMap[node.id] = node;
            });
            
            const vmNodes = nodes.filter(n => n.type === 'vm');
            const vmsBySeg = {};
            const unlinkedVms = [];
            
            vmNodes.forEach(node => {
                const segId = node.segment_id;
                if (segId && segmentMap[segId]) {
                    if (!vmsBySeg[segId]) vmsBySeg[segId] = [];
                    vmsBySeg[segId].push(node);
                } else {
                    unlinkedVms.push(node);
                }
            });
            
            Object.keys(vmsBySeg).forEach(segId => {
                const segNode = segmentMap[segId];
                const childVms = vmsBySeg[segId];
                const count = childVms.length;
                childVms.forEach((node, idx) => {
                    const spacing = 80;
                    const offset = (idx - (count - 1) / 2) * spacing;
                    node.x = segNode.x + offset;
                });
            });
            
            unlinkedVms.forEach((node, idx) => {
                const count = unlinkedVms.length;
                node.x = count === 1 ? 400 : 100 + (idx / (count - 1)) * 600;
            });
            
            const findNode = (id) => nodes.find(n => n.id === id);
            
            links.forEach(link => {
                const sourceNode = findNode(link.from);
                const targetNode = findNode(link.to);
                if (!sourceNode || !targetNode) return;
                
                let strokeColor = 'var(--sdn-link-inactive, rgba(255,255,255,0.15))';
                let flowColor = 'var(--sdn-link-flow, rgba(255,255,255,0.3))';
                
                if (link.type === 'uplink-t0') {
                    strokeColor = 'rgba(180, 80, 255, 0.25)';
                    flowColor = '#b450ff';
                } else if (link.type === 't0-t1') {
                    strokeColor = 'rgba(56, 189, 248, 0.25)';
                    flowColor = '#38bdf8';
                } else if (link.type === 't1-segment') {
                    strokeColor = 'rgba(46, 204, 113, 0.25)';
                    flowColor = '#2ecc71';
                } else if (link.type === 'segment-vm') {
                    if (link.active) {
                        strokeColor = 'rgba(224, 86, 36, 0.3)';
                        flowColor = '#e05624';
                    } else {
                        strokeColor = 'var(--sdn-link-inactive-vm, rgba(255, 255, 255, 0.05))';
                        flowColor = 'transparent';
                    }
                }
                
                const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
                const dx = targetNode.x - sourceNode.x;
                const dy = targetNode.y - sourceNode.y;
                const cy1 = sourceNode.y + dy * 0.4;
                const cy2 = sourceNode.y + dy * 0.6;
                const pathData = `M ${sourceNode.x} ${sourceNode.y} C ${sourceNode.x} ${cy1}, ${targetNode.x} ${cy2}, ${targetNode.x} ${targetNode.y}`;
                path.setAttribute('d', pathData);
                path.setAttribute('fill', 'none');
                path.setAttribute('stroke', strokeColor);
                path.setAttribute('stroke-width', '2');
                
                if (link.active) {
                    linksGroup.appendChild(path);
                    
                    const flowPath = document.createElementNS('http://www.w3.org/2000/svg', 'path');
                    flowPath.setAttribute('d', pathData);
                    flowPath.setAttribute('fill', 'none');
                    flowPath.setAttribute('stroke', flowColor);
                    flowPath.setAttribute('stroke-width', '2');
                    flowPath.setAttribute('class', 'svg-link');
                    linksGroup.appendChild(flowPath);
                } else {
                    path.setAttribute('class', 'svg-link-inactive');
                    path.setAttribute('stroke-dasharray', '4 4');
                    linksGroup.appendChild(path);
                }
            });
            
            nodes.forEach(node => {
                const group = document.createElementNS('http://www.w3.org/2000/svg', 'g');
                group.setAttribute('class', 'svg-node');
                group.setAttribute('transform', `translate(${node.x}, ${node.y})`);
                
                let color = '#ccc';
                let glow = 'none';
                let iconText = '';
                
                if (node.type === 'uplink') {
                    color = '#94a3b8';
                    iconText = '🌐';
                } else if (node.type === 't0') {
                    color = '#b450ff';
                    glow = 'url(#glow-t0)';
                    iconText = '🛡️';
                } else if (node.type === 't1') {
                    color = '#38bdf8';
                    glow = 'url(#glow-t1)';
                    iconText = '⚡';
                } else if (node.type === 'segment') {
                    color = '#2ecc71';
                    glow = 'url(#glow-segment)';
                    iconText = '☁️';
                } else if (node.type === 'vm') {
                    color = '#e05624';
                    iconText = '💻';
                }
                
                if (glow !== 'none') {
                    const glowCircle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
                    glowCircle.setAttribute('r', '40');
                    glowCircle.setAttribute('fill', glow);
                    group.appendChild(glowCircle);
                }
                
                const outerCircle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
                outerCircle.setAttribute('r', '18');
                outerCircle.setAttribute('fill', 'var(--bg-card-solid, #090b14)');
                outerCircle.setAttribute('stroke', color);
                outerCircle.setAttribute('stroke-width', '2');
                group.appendChild(outerCircle);
                
                if (node.type === 't0' || node.type === 't1') {
                    const iconPath = document.createElementNS('http://www.w3.org/2000/svg', 'path');
                    iconPath.setAttribute('d', 'M19 13h-4v-2h4V5c0-1.1-.9-2-2-2H3c-1.1 0-2 .9-2 2v6h4v2H1v6c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2v-6zM3 5h14v4H3V5zm2 12c-.55 0-1-.45-1-1s.45-1 1-1 1 .45 1 1-.45 1-1 1zm4 0c-.55 0-1-.45-1-1s.45-1 1-1 1 .45 1 1-.45 1-1 1zm4 0c-.55 0-1-.45-1-1s.45-1 1-1 1 .45 1 1-.45 1-1 1zm4 0c-.55 0-1-.45-1-1s.45-1 1-1 1 .45 1 1-.45 1-1 1z');
                    iconPath.setAttribute('fill', color);
                    iconPath.setAttribute('transform', 'translate(-10, -10) scale(1.0)');
                    group.appendChild(iconPath);
                } else {
                    const textIcon = document.createElementNS('http://www.w3.org/2000/svg', 'text');
                    textIcon.setAttribute('y', '5');
                    textIcon.setAttribute('text-anchor', 'middle');
                    textIcon.setAttribute('font-size', '14');
                    textIcon.textContent = iconText;
                    group.appendChild(textIcon);
                }
                
                const labelText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
                labelText.setAttribute('y', '32');
                labelText.setAttribute('text-anchor', 'middle');
                labelText.setAttribute('fill', 'var(--text-primary)');
                labelText.setAttribute('font-family', "'Space Grotesk', sans-serif");
                labelText.setAttribute('font-size', '10');
                labelText.setAttribute('font-weight', '500');
                
                const displayLabel = String(node.label || '');
                labelText.textContent = displayLabel.length > 25 ? displayLabel.substring(0, 22) + '...' : displayLabel;
                group.appendChild(labelText);
                
                group.addEventListener('click', (e) => {
                    e.stopPropagation();
                    showNodeDetails(node);
                });
                
                nodesGroup.appendChild(group);
            });
        } catch (err) {
            console.error("Error rendering topology:", err);
        }
    }

    function showNodeDetails(node) {
        const content = document.getElementById('sdn-inspector-content');
        if (!content) return;
        
        let html = `
            <div style="background: var(--bg-input); border: 1px solid var(--border-color); border-radius: 8px; padding: 15px; display: flex; flex-direction: column; gap: 10px;">
                <div style="display: flex; align-items: center; gap: 10px; border-bottom: 1px solid var(--border-color); padding-bottom: 10px;">
                    <span style="font-size: 20px; display: flex; align-items: center; justify-content: center; width: 24px; height: 24px;">
                        ${node.type === 'uplink' ? '🌐' : 
                          (node.type === 't0' || node.type === 't1') ? 
                          `<svg viewBox="0 0 24 24" style="width: 24px; height: 24px; fill: ${node.type === 't0' ? '#b450ff' : '#38bdf8'};"><path d="M19 13h-4v-2h4V5c0-1.1-.9-2-2-2H3c-1.1 0-2 .9-2 2v6h4v2H1v6c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2v-6zM3 5h14v4H3V5zm2 12c-.55 0-1-.45-1-1s.45-1 1-1 1 .45 1 1-.45 1-1 1zm4 0c-.55 0-1-.45-1-1s.45-1 1-1 1 .45 1 1-.45 1-1 1zm4 0c-.55 0-1-.45-1-1s.45-1 1-1 1 .45 1 1-.45 1-1 1zm4 0c-.55 0-1-.45-1-1s.45-1 1-1 1 .45 1 1-.45 1-1 1z"/></svg>` : 
                          node.type === 'segment' ? '☁️' : '💻'}
                    </span>
                    <div>
                        <h4 style="margin: 0; font-family: 'Space Grotesk', sans-serif; font-size: 14px; font-weight: 600; color: var(--text-primary);">${node.details.name}</h4>
                        <span class="status-indicator ${node.type === 'vm' ? (node.details.status === 'running' ? 'status-green' : 'status-red') : 'status-green'}" style="font-size: 9px; padding: 1px 4px; margin-top: 3px; display: inline-block;">
                            ${node.type === 'vm' ? (node.details.status === 'running' ? 'Running' : 'Stopped') : 'Active'}
                        </span>
                    </div>
                </div>
                
                <div style="display: flex; flex-direction: column; gap: 4px;">
        `;
        
        if (node.type === 'uplink') {
            html += `
                <div class="inspector-prop"><span>Interface</span><span>${node.details.interface}</span></div>
                <div class="inspector-prop"><span>Link Speed</span><span>${node.details.speed}</span></div>
                <div class="inspector-prop"><span>MTU Size</span><span>${node.details.mtu} Bytes</span></div>
                <div class="inspector-prop"><span>Subnet</span><span>${node.details.subnet}</span></div>
            `;
        } else if (node.type === 't0') {
            const hasBgp = node.details.bgp === 'Enabled';
            html += `
                <div class="inspector-prop"><span>Router ID</span><span style="font-size: 9px; max-width: 120px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">${node.details.id}</span></div>
                <div class="inspector-prop"><span>Uplink NIC</span><span>${node.details.interface}</span></div>
                <div class="inspector-prop"><span>Uplink CIDR</span><span>${node.details.ip}</span></div>
                <div class="inspector-prop"><span>Upstream Gateway</span><span>${node.details.gateway}</span></div>
                <div class="inspector-prop"><span>Source NAT</span><span>Enabled</span></div>
                <div class="inspector-prop"><span>BGP Peering</span><span>${node.details.bgp}</span></div>
            `;
            if (hasBgp) {
                html += `
                    <div class="inspector-prop"><span>Local ASN</span><span>${node.details.bgp_local_asn}</span></div>
                    <div class="inspector-prop"><span>Neighbor IP</span><span>${node.details.bgp_neighbor}</span></div>
                    <div class="inspector-prop"><span>Remote ASN</span><span>${node.details.bgp_remote_asn}</span></div>
                `;
            }
            if (node.details.transit_ips && node.details.transit_ips.length > 0) {
                node.details.transit_ips.forEach(ip => {
                    html += `
                        <div class="inspector-prop"><span>Transit IP</span><span>${ip}</span></div>
                    `;
                });
            }
        } else if (node.type === 't1') {
            html += `
                <div class="inspector-prop"><span>Router ID</span><span style="font-size: 9px; max-width: 120px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">${node.details.id}</span></div>
                <div class="inspector-prop"><span>Routing Mode</span><span>DLR (Distributed)</span></div>
                <div class="inspector-prop"><span>DHCP Server</span><span>${node.details.dhcp}</span></div>
                <div class="inspector-prop"><span>Transit IP</span><span>${node.details.transit_ip}</span></div>
                <div class="inspector-prop"><span>Transit Subnet</span><span>100.64.0.0/16 scope</span></div>
            `;
        } else if (node.type === 'segment') {
            html += `
                <div class="inspector-prop"><span>VXLAN VNI</span><span>${node.details.vni}</span></div>
                <div class="inspector-prop"><span>IP Subnet</span><span>${node.details.subnet}</span></div>
                <div class="inspector-prop"><span>Default Gateway</span><span>${node.details.gateway}</span></div>
                <div class="inspector-prop"><span>Overlay Type</span><span>${node.details.overlay_type}</span></div>
                <div class="inspector-prop"><span>VTEP Interface</span><span>vxlan${node.details.vni}</span></div>
            `;
        } else if (node.type === 'vm') {
            html += `
                <div class="inspector-prop"><span>Host Node</span><span>${node.details.node}</span></div>
                <div class="inspector-prop"><span>IP Address</span><span>${node.details.ip}</span></div>
                <div class="inspector-prop"><span>vCPUs</span><span>${node.details.vcpus}</span></div>
                <div class="inspector-prop"><span>Memory</span><span>${node.details.memory}</span></div>
                <div class="inspector-prop"><span>Disk Size</span><span>${node.details.disk}</span></div>
            `;
        }
        
        html += `
                </div>
            </div>
        `;
        
        content.innerHTML = html;
    }

    if (document.getElementById('sdn-topology-svg')) {
        initUrbosaSdnPage();
    }
    // ----------------------------------------------------
    // Hylia Life Cycle Management (LCM) Logic
    // ----------------------------------------------------
    let lcmPollInterval = null;
    let lcmScrollLock = true;

    function initLcmPage() {
        const fileInput = document.getElementById('lcm-file-input');
        const dropzone = document.getElementById('lcm-upload-dropzone');
        const uploadCard = document.getElementById('lcm-upload-card');
        const previewCard = document.getElementById('lcm-preview-card');
        const progressCard = document.getElementById('lcm-progress-card');
        const consoleCard = document.getElementById('lcm-console-card');
        const consoleLog = document.getElementById('lcm-console-log');
        
        const uploadProgressContainer = document.getElementById('upload-progress-container');
        const uploadProgressBar = document.getElementById('upload-progress-bar');
        const uploadStatusText = document.getElementById('upload-status-text');
        const uploadPercentText = document.getElementById('upload-percent-text');
        
        const btnStartUpgrade = document.getElementById('btn-start-upgrade');
        const btnCancelUpgrade = document.getElementById('btn-cancel-upgrade');
        const btnScrollLock = document.getElementById('btn-scroll-lock');

        if (!fileInput || !dropzone) return;

        // Set up scroll lock button click
        if (btnScrollLock) {
            btnScrollLock.addEventListener('click', () => {
                lcmScrollLock = !lcmScrollLock;
                btnScrollLock.style.opacity = lcmScrollLock ? '1' : '0.6';
                showToast('Scroll Lock', lcmScrollLock ? 'Auto-scroll enabled' : 'Auto-scroll disabled', 'info');
            });
        }

        // Dropzone interactions
        dropzone.addEventListener('click', () => fileInput.click());
        
        dropzone.addEventListener('dragover', (e) => {
            e.preventDefault();
            dropzone.style.borderColor = 'var(--color-primary)';
            dropzone.style.background = 'rgba(59, 130, 246, 0.04)';
        });

        dropzone.addEventListener('dragleave', () => {
            dropzone.style.borderColor = 'rgba(255, 255, 255, 0.15)';
            dropzone.style.background = 'rgba(255, 255, 255, 0.01)';
        });

        dropzone.addEventListener('drop', (e) => {
            e.preventDefault();
            dropzone.style.borderColor = 'rgba(255, 255, 255, 0.15)';
            dropzone.style.background = 'rgba(255, 255, 255, 0.01)';
            if (e.dataTransfer.files.length > 0) {
                handleLcmPackageFile(e.dataTransfer.files[0]);
            }
        });

        fileInput.addEventListener('change', () => {
            if (fileInput.files.length > 0) {
                handleLcmPackageFile(fileInput.files[0]);
            }
        });

        // Cancel upgrade button
        if (btnCancelUpgrade) {
            btnCancelUpgrade.addEventListener('click', () => {
                if (confirm("Are you sure you want to discard this upgrade package preview?")) {
                    previewCard.style.display = 'none';
                    consoleCard.style.display = 'none';
                    uploadCard.style.display = 'flex';
                    fileInput.value = '';
                }
            });
        }

        // Start upgrade button
        if (btnStartUpgrade) {
            btnStartUpgrade.addEventListener('click', async () => {
                btnStartUpgrade.disabled = true;
                btnStartUpgrade.textContent = 'Starting...';
                
                try {
                    const res = await fetch(`${state.apiHost}/api/lcm/upgrade/start`, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'Authorization': 'Bearer ' + getStoredToken()
                        }
                    });
                    
                    const data = await res.json();
                    if (res.ok) {
                        showToast('Upgrade Started', data.message || 'Rolling upgrade loop initiated.', 'success');
                        previewCard.style.display = 'none';
                        progressCard.style.display = 'flex';
                        consoleCard.style.display = 'flex';
                        
                        // Clear old console and start polling status
                        if (consoleLog) consoleLog.textContent = '';
                        startLcmStatusPolling();
                    } else {
                        showToast('Error', data.error || 'Failed to start upgrade.', 'error');
                        btnStartUpgrade.disabled = false;
                        btnStartUpgrade.textContent = 'Start Rolling Upgrade';
                    }
                } catch (err) {
                    console.error("Error starting upgrade:", err);
                    showToast('Connection Error', 'Could not connect to update orchestrator.', 'error');
                    btnStartUpgrade.disabled = false;
                    btnStartUpgrade.textContent = 'Start Rolling Upgrade';
                }
            });
        }

        // File upload execution
        function handleLcmPackageFile(file) {
            if (!file.name.endsWith('.zip')) {
                showToast('Invalid File Type', 'Please upload a valid .zip upgrade archive.', 'error');
                return;
            }

            if (uploadProgressContainer) {
                uploadStatusText.textContent = 'Uploading upgrade package...';
                uploadPercentText.textContent = '0%';
                uploadProgressBar.style.width = '0%';
                uploadProgressContainer.style.display = 'block';
            }

            const xhr = new XMLHttpRequest();
            xhr.open('POST', `${state.apiHost}/api/lcm/upload`, true);
            
            const token = getStoredToken();
            if (token) {
                xhr.setRequestHeader('Authorization', `Bearer ${token}`);
            }
            
            xhr.setRequestHeader('X-File-Name', file.name);
            xhr.setRequestHeader('Content-Type', 'application/octet-stream');
            
            xhr.upload.onprogress = (e) => {
                if (e.lengthComputable) {
                    const percent = Math.round((e.loaded / e.total) * 100);
                    if (uploadPercentText) uploadPercentText.textContent = `${percent}%`;
                    if (uploadProgressBar) uploadProgressBar.style.width = `${percent}%`;
                }
            };
            
            xhr.onload = () => {
                if (xhr.status === 200) {
                    try {
                        const data = JSON.parse(xhr.responseText);
                        showToast('Package Uploaded', 'Upgrade package verified and parsed successfully.', 'success');
                        
                        // Render components list
                        const tbody = document.getElementById('lcm-components-preview-tbody');
                        if (tbody) {
                            tbody.innerHTML = '';
                            data.components.forEach(comp => {
                                const tr = document.createElement('tr');
                                tr.innerHTML = `
                                    <td style="font-weight:600; color:#fff;">${comp.name}</td>
                                    <td><span style="font-family:monospace; background:rgba(255,255,255,0.05); padding:2px 6px; border-radius:4px;">${comp.current_build}</span></td>
                                    <td><span style="font-family:monospace; color:var(--color-primary); background:rgba(59,130,246,0.1); padding:2px 6px; border-radius:4px;">${comp.new_build}</span></td>
                                `;
                                tbody.appendChild(tr);
                            });
                        }
                        
                        // Render changelog
                        const changelogContainer = document.getElementById('lcm-changelog-container');
                        if (changelogContainer) {
                            changelogContainer.textContent = data.changelog || 'No changelog provided in this package.';
                        }
                        
                        // Set build badge
                        const buildBadge = document.getElementById('lcm-target-build-badge');
                        if (buildBadge) {
                            buildBadge.textContent = `Build ${data.build_number}`;
                        }
                        
                        // Switch Cards
                        uploadCard.style.display = 'none';
                        previewCard.style.display = 'flex';
                        consoleCard.style.display = 'flex';
                        if (consoleLog) {
                            consoleLog.textContent = `[Hylia Console] Ready to upgrade to build ${data.build_number}\n[Hylia Console] Package hash check passed.`;
                        }
                    } catch (e) {
                        showToast('Verification Failed', 'Invalid response from server during package verification.', 'error');
                    }
                } else {
                    let errMsg = 'Package verification failed.';
                    try {
                        const data = JSON.parse(xhr.responseText);
                        errMsg = data.error || errMsg;
                    } catch (e) {}
                    showToast('Upload Failed', errMsg, 'error');
                }
                if (uploadProgressContainer) uploadProgressContainer.style.display = 'none';
            };
            
            xhr.onerror = () => {
                showToast('Upload Failed', 'Network error during upload.', 'error');
                if (uploadProgressContainer) uploadProgressContainer.style.display = 'none';
            };
            
            xhr.send(file);
        }

        // Check if there is an active job running on load
        pollLcmStatus(true);
    }

    function getNodeNameByIp(ip) {
        if (ip === "127.0.0.1") {
            const localNode = state.nodes.find(n => n.role === 'Leader');
            if (localNode) return localNode.name;
        }
        if (state.nodes) {
            const node = state.nodes.find(n => n.ip === ip);
            if (node) return node.name;
        }
        return ip;
    }

    async function pollLcmStatus(firstCheck = false) {
        try {
            const res = await fetch(`${state.apiHost}/api/lcm/upgrade/status`, {
                headers: { 'Authorization': 'Bearer ' + getStoredToken() }
            });
            if (res.ok) {
                const data = await res.json();
                
                const uploadCard = document.getElementById('lcm-upload-card');
                const previewCard = document.getElementById('lcm-preview-card');
                const progressCard = document.getElementById('lcm-progress-card');
                const consoleCard = document.getElementById('lcm-console-card');
                const consoleLog = document.getElementById('lcm-console-log');
                
                const progressBar = document.getElementById('lcm-upgrade-progress-bar');
                const percentText = document.getElementById('lcm-upgrade-percent-text');
                const statusBadge = document.getElementById('lcm-overall-status-badge');
                const stepDescText = document.getElementById('lcm-step-description-text');
                const stepperContainer = document.getElementById('lcm-hosts-stepper-container');

                if (data.status === 'IDLE') {
                    if (firstCheck) {
                        if (uploadCard) uploadCard.style.display = 'flex';
                        if (previewCard) previewCard.style.display = 'none';
                        if (progressCard) progressCard.style.display = 'none';
                        if (consoleCard) consoleCard.style.display = 'none';
                    }
                } else {
                    // VALIDATING, STARTING, UPGRADING, COMPLETED, FAILED
                    if (uploadCard) uploadCard.style.display = 'none';
                    if (previewCard) previewCard.style.display = 'none';
                    if (progressCard) progressCard.style.display = 'flex';
                    if (consoleCard) consoleCard.style.display = 'flex';
                    
                    // Update main progress bar
                    if (progressBar) {
                        progressBar.style.width = `${data.progress}%`;
                        if (data.status === 'COMPLETED') {
                            progressBar.style.backgroundColor = '#34d399';
                        } else if (data.status === 'FAILED') {
                            progressBar.style.backgroundColor = '#ef4444';
                        } else {
                            progressBar.style.backgroundColor = 'var(--color-primary)';
                        }
                    }
                    if (percentText) percentText.textContent = `${data.progress}%`;
                    
                    if (statusBadge) {
                        statusBadge.textContent = data.status;
                        if (data.status === 'COMPLETED') {
                            statusBadge.style.color = '#34d399';
                            statusBadge.style.background = 'rgba(52, 211, 153, 0.1)';
                            statusBadge.style.borderColor = 'rgba(52, 211, 153, 0.2)';
                        } else if (data.status === 'FAILED') {
                            statusBadge.style.color = '#ef4444';
                            statusBadge.style.background = 'rgba(239, 68, 68, 0.1)';
                            statusBadge.style.borderColor = 'rgba(239, 68, 68, 0.2)';
                        } else {
                            statusBadge.style.color = 'var(--color-primary)';
                            statusBadge.style.background = 'rgba(59, 130, 246, 0.1)';
                            statusBadge.style.borderColor = 'rgba(59, 130, 246, 0.2)';
                        }
                    }

                    // Update description text
                    if (stepDescText) {
                        if (data.status === 'STARTING') {
                            stepDescText.textContent = 'Orchestrating upgrade and establishing ZK locks...';
                        } else if (data.status === 'COMPLETED') {
                            stepDescText.textContent = 'Cluster successfully upgraded to build ' + data.build_number;
                        } else if (data.status === 'FAILED') {
                            stepDescText.textContent = 'Upgrade failed. Please consult the console logs below.';
                        } else if (data.current_node) {
                            const nodeName = getNodeNameByIp(data.current_node);
                            stepDescText.textContent = `Upgrading host ${nodeName} (${data.current_node})...`;
                        } else {
                            stepDescText.textContent = 'Rolling upgrade loop executing...';
                        }
                    }

                    // Render node steps
                    if (stepperContainer && data.target_nodes) {
                        stepperContainer.innerHTML = '';
                        const currentIdx = data.target_nodes.indexOf(data.current_node);
                        
                        data.target_nodes.forEach((ip, idx) => {
                            const nodeName = getNodeNameByIp(ip);
                            let stepStatusClass = 'waiting';
                            let stepBorderColor = 'rgba(255, 255, 255, 0.05)';
                            let stepBg = 'rgba(255, 255, 255, 0.01)';
                            let pillStyle = 'color: var(--text-muted); background: rgba(255, 255, 255, 0.05); border: 1px solid rgba(255, 255, 255, 0.08);';
                            let pillText = 'QUEUED';
                            let subLabel = 'Pending';
                            let subPercent = '0%';
                            let subColor = 'rgba(255, 255, 255, 0.1)';

                            if (data.status === 'COMPLETED') {
                                stepStatusClass = 'completed';
                                stepBorderColor = 'rgba(52, 211, 153, 0.2)';
                                stepBg = 'rgba(52, 211, 153, 0.03)';
                                pillStyle = 'color: #34d399; background: rgba(52, 211, 153, 0.1); border: 1px solid rgba(52, 211, 153, 0.15);';
                                pillText = 'COMPLETED';
                                subLabel = 'Finished';
                                subPercent = '100%';
                                subColor = '#34d399';
                            } else if (data.status === 'FAILED' && idx === currentIdx) {
                                stepStatusClass = 'failed';
                                stepBorderColor = 'rgba(239, 68, 68, 0.3)';
                                stepBg = 'rgba(239, 68, 68, 0.05)';
                                pillStyle = 'color: #ef4444; background: rgba(239, 68, 68, 0.1); border: 1px solid rgba(239, 68, 68, 0.2);';
                                pillText = 'FAILED';
                                subLabel = 'Error';
                                subPercent = '100%';
                                subColor = '#ef4444';
                            } else if (idx < currentIdx) {
                                stepStatusClass = 'completed';
                                stepBorderColor = 'rgba(52, 211, 153, 0.2)';
                                stepBg = 'rgba(52, 211, 153, 0.03)';
                                pillStyle = 'color: #34d399; background: rgba(52, 211, 153, 0.1); border: 1px solid rgba(52, 211, 153, 0.15);';
                                pillText = 'COMPLETED';
                                subLabel = 'Finished';
                                subPercent = '100%';
                                subColor = '#34d399';
                            } else if (idx === currentIdx) {
                                stepStatusClass = 'active';
                                stepBorderColor = 'rgba(59, 130, 246, 0.4)';
                                stepBg = 'rgba(59, 130, 246, 0.05)';
                                pillStyle = 'color: var(--color-primary); background: rgba(59, 130, 246, 0.1); border: 1px solid rgba(59, 130, 246, 0.25);';
                                pillText = 'UPGRADING';
                                
                                // Inspect logs for subprogress
                                subLabel = 'Evacuating VMs';
                                subPercent = '20%';
                                subColor = 'var(--color-primary)';
                                
                                const hostLogs = data.logs.filter(l => l.includes(ip) || l.includes(nodeName));
                                if (hostLogs.length > 0) {
                                    const lastLog = hostLogs[hostLogs.length - 1].toLowerCase();
                                    if (lastLog.includes('leave') || lastLog.includes('normal') || lastLog.includes('upgraded successfully')) {
                                        subLabel = 'Rejoining cluster';
                                        subPercent = '95%';
                                    } else if (lastLog.includes('stabiliz') || lastLog.includes('online') || lastLog.includes('back online')) {
                                        subLabel = 'Stabilizing services';
                                        subPercent = '80%';
                                    } else if (lastLog.includes('reboot') || lastLog.includes('offline')) {
                                        subLabel = 'Rebooting host';
                                        subPercent = '60%';
                                    } else if (lastLog.includes('deploy') || lastLog.includes('cop') || lastLog.includes('transfer')) {
                                        subLabel = 'Deploying files';
                                        subPercent = '40%';
                                    }
                                }
                            }

                            const stepEl = document.createElement('div');
                            stepEl.className = `glass-card node-step-card ${stepStatusClass}`;
                            stepEl.style.padding = '15px';
                            stepEl.style.display = 'flex';
                            stepEl.style.flexDirection = 'column';
                            stepEl.style.gap = '8px';
                            stepEl.style.border = `1px solid ${stepBorderColor}`;
                            stepEl.style.background = stepBg;
                            stepEl.style.textAlign = 'left';
                            
                            stepEl.innerHTML = `
                                <div style="display: flex; justify-content: space-between; align-items: center; gap: 8px;">
                                    <span style="font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; color: #fff;">${nodeName}</span>
                                    <span style="font-size: 9px; font-weight: 700; padding: 2px 6px; border-radius: 10px; ${pillStyle}">${pillText}</span>
                                </div>
                                <div style="font-size: 11px; color: var(--text-muted);">IP: ${ip}</div>
                                <div style="display: flex; flex-direction: column; gap: 4px; margin-top: 5px;">
                                    <div style="display: flex; justify-content: space-between; font-size: 10px; color: var(--text-muted); font-family:'Space Grotesk', sans-serif;">
                                        <span>${subLabel}</span>
                                        <span>${subPercent}</span>
                                    </div>
                                    <div class="progress-bar-container" style="height: 4px;">
                                        <div class="progress-bar" style="width: ${subPercent}; background-color: ${subColor};"></div>
                                    </div>
                                </div>
                            `;
                            stepperContainer.appendChild(stepEl);
                        });
                    }

                    // Update Logs
                    if (consoleLog && data.logs) {
                        const originalText = consoleLog.textContent;
                        const newText = data.logs.join('\n');
                        if (originalText !== newText) {
                            consoleLog.textContent = newText;
                            if (lcmScrollLock) {
                                consoleLog.scrollTop = consoleLog.scrollHeight;
                            }
                        }
                    }

                    // Start status loop if not already running
                    if (data.status === 'UPGRADING' || data.status === 'STARTING') {
                        startLcmStatusPolling();
                    } else {
                        // Completed or failed
                        stopLcmStatusPolling();
                    }
                }
            }
        } catch (err) {
            console.error("Error polling LCM status:", err);
        }
    }

    function startLcmStatusPolling() {
        if (!lcmPollInterval) {
            lcmPollInterval = setInterval(() => pollLcmStatus(false), 2000);
        }
    }

    function stopLcmStatusPolling() {
        if (lcmPollInterval) {
            clearInterval(lcmPollInterval);
            lcmPollInterval = null;
        }
    }

    // Call init if we are on LCM page
    if (document.getElementById('lcm-file-input')) {
        initLcmPage();
    }

    // Start by checking authentication
    checkAuth();
});
