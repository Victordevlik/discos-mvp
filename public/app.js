// Añadimos venueId para operar en modo SaaS multi-venue
let S = { sessionId: '', venueId: '', user: null, staff: null, role: '', sse: null, staffSSE: null, currentInvite: null, meeting: null, consumptionReq: null, nav: { history: [], current: '' }, notifications: { invites: 0 }, timers: { userPoll: 0, staffPoll: 0, userReconnect: 0, staffReconnect: 0, catalogSave: 0, modalHide: 0 }, staffTab: '', cart: [], messageTTL: 4000, modalShownAt: 0, isMeetingReceiver: false, meetingPlan: '', sched: {}, loading: {}, catalogGroups: {}, catalogCat: '', catalogSubcat: '', waiterReason: '', invitesQueue: [], inInviteFlow: false, missed: [] }

function q(id) { return document.getElementById(id) }
function show(id) {
  for (const el of document.querySelectorAll('.screen')) el.classList.remove('active')
  q(id).classList.add('active')
  if (S.nav && S.nav.current && S.nav.current !== id) S.nav.history.push(S.nav.current)
  S.nav.current = id
  const tb = q('topbar')
  if (tb) tb.classList.add('show')
  setActiveNavByScreen(id)
  if (String(id).startsWith('screen-staff')) {
    const map = {
      'screen-staff': 'tab-staff-panel',
      'screen-staff-orders': 'tab-staff-orders',
      'screen-staff-mesas': 'tab-staff-mesas',
      'screen-staff-users': 'tab-staff-users',
      'screen-staff-waiter': 'tab-staff-waiter',
      'screen-staff-reportes': 'tab-staff-reportes',
      'screen-staff-promos': 'tab-staff-promos',
      'screen-staff-catalog': 'tab-staff-catalog',
    }
    for (const el of document.querySelectorAll('#staff-tabs .tab-item')) el.classList.remove('active')
    const tabId = map[id]
    if (tabId) { const el = q(tabId); if (el) el.classList.add('active') }
  }
  const isStaffView = String(id).startsWith('screen-staff')
  const nav = document.getElementById('navbar')
  const fab = document.getElementById('fab-call')
  const fabLabel = document.getElementById('fab-call-label')
  if (nav) nav.style.display = isStaffView ? 'none' : ''
  if (fab) fab.style.display = isStaffView ? 'none' : ''
  if (fabLabel) fabLabel.style.display = isStaffView ? 'none' : ''
  if (isStaffView) { renderVenueTitle() }
}
function goBack() {
  if (!S.nav || !S.nav.history.length) return
  const prev = S.nav.history.pop()
  for (const el of document.querySelectorAll('.screen')) el.classList.remove('active')
  q(prev).classList.add('active')
  S.nav.current = prev
  const tb = q('topbar')
  const roots = ['screen-welcome', 'screen-user-home', 'screen-staff']
  if (tb) tb.classList.toggle('show', !!(S.nav.history.length && !roots.includes(S.nav.current)))
  setActiveNavByScreen(prev)
}
function goHome() {
  if (S.role === 'user') { show('screen-user-home'); return }
  if (S.role === 'staff') { show('screen-staff'); return }
  show('screen-welcome')
}
function setActiveNav(tab) {
  for (const el of document.querySelectorAll('.nav-item')) el.classList.remove('active')
  const map = { carta: 'nav-carta', disponibles: 'nav-disponibles', mesas: 'nav-mesas', orders: 'nav-orders', perfil: 'nav-perfil' }
  const id = map[tab]
  if (id) { const el = q(id); if (el) el.classList.add('active') }
}
function setActiveNavByScreen(screenId) {
  const reverse = { 'screen-consumption': 'carta', 'screen-disponibles-select': 'disponibles', 'screen-disponibles': 'disponibles', 'screen-mesas': 'mesas', 'screen-orders-user': 'orders', 'screen-edit-profile': 'perfil' }
  const tab = reverse[screenId]
  if (tab) setActiveNav(tab)
}
function setBadgeNav(tab, count) {
  const map = { disponibles: 'badge-disponibles' }
  const id = map[tab]
  const el = id ? q(id) : null
  if (!el) return
  if (count > 0) { el.classList.add('show'); el.textContent = count > 9 ? '9+' : String(count) }
  else { el.classList.remove('show'); el.textContent = '' }
}

function normalizeTableId(v) {
  const m = String(v || '').match(/\d+/)
  return m ? m[0] : ''
}

function setTheme(t) {
  try { localStorage.setItem('discos_theme', t) } catch {}
  if (document && document.body) document.body.dataset.theme = t
}
function toggleTheme() {
  const cur = document && document.body ? document.body.dataset.theme : ''
  setTheme(cur === 'light' ? 'dark' : 'light')
}

function showStaffTab(tab) {
  const contentMap = {
    panel: 'staff-panel-content',
    orders: 'staff-orders-content',
    mesas: 'staff-mesas-content',
    users: 'staff-users-content',
    waiter: 'staff-waiter-content',
    reportes: 'staff-reportes-content',
    promos: 'staff-promos-content',
    catalog: 'staff-catalog-content',
    analytics: 'staff-analytics-content',
  }
  for (const el of document.querySelectorAll('#staff-content .section')) el.classList.remove('active')
  const id = contentMap[tab]
  if (id) { const el = q(id); if (el) el.classList.add('active') }
  for (const el of document.querySelectorAll('#staff-tabs .tab-item')) el.classList.remove('active')
  const tabMap = {
    panel: 'tab-staff-panel', orders: 'tab-staff-orders', mesas: 'tab-staff-mesas', users: 'tab-staff-users',
    waiter: 'tab-staff-waiter', reportes: 'tab-staff-reportes', promos: 'tab-staff-promos', catalog: 'tab-staff-catalog'
  }
  const tId = tabMap[tab]; if (tId) { const el = q(tId); if (el) el.classList.add('active') }
  if (tab === 'orders') loadOrders(q('staff-orders-filter')?.value || '')
  else if (tab === 'users') loadUsers()
  else if (tab === 'waiter') loadWaiterCalls()
  else if (tab === 'reportes') loadReports()
  else if (tab === 'promos') loadStaffPromos()
  else if (tab === 'catalog') loadStaffCatalogEditor()
  else if (tab === 'panel') loadSessionInfo()
  else if (tab === 'analytics') loadAnalytics()
  S.staffTab = tab
}

async function loadSessionInfo() {
  try {
    let pin = ''
    try { const r = await api('/api/session/active'); pin = r.pin || '' } catch {}
    let baseCandidate = ''
    try {
      const pb = await api(`/api/session/public-base?sessionId=${encodeURIComponent(S.sessionId)}`)
      baseCandidate = (pb.publicBaseUrl || '').trim()
      const inp = q('public-base'); if (inp && baseCandidate) inp.value = baseCandidate
    } catch {}
    let base = baseCandidate || location.origin
    // QR y link de sesión incluyen venueId + sessionId
    const url = `${base}/?venueId=${encodeURIComponent(S.venueId || 'default')}&sessionId=${encodeURIComponent(S.sessionId)}&aj=1`
    const pd = q('pin-display'); if (pd) pd.textContent = pin
    const qrImg = q('qr-session'); if (qrImg) qrImg.src = `https://api.qrserver.com/v1/create-qr-code/?size=200x200&data=${encodeURIComponent(url)}`
    const share = q('share-url'); if (share) { share.href = url; share.textContent = url }
    renderVenueTitle()
  } catch {}
}

async function renderVenueTitle() {
  try {
    const sess = await api(`/api/session/active${S.venueId ? ('?venueId=' + encodeURIComponent(S.venueId)) : ''}`)
    const el = q('staff-title')
    if (el) el.textContent = `Panel de órdenes ${sess.venueName || S.venueId || ''}`
  } catch {}
}
async function api(path, opts = {}) {
  const res = await fetch(path, { method: 'GET', ...opts, headers: { 'Content-Type': 'application/json' } })
  let data = null
  try { data = await res.json() } catch {}
  if (!res.ok) {
    const msg = data && data.error ? data.error : 'api'
    throw new Error(msg)
  }
  return data
}
async function apiAdmin(path, secret, opts = {}) {
  const headers = { 'Content-Type': 'application/json', 'X-Admin-Secret': String(secret || '') }
  const res = await fetch(path, { method: 'GET', ...opts, headers })
  let data = null
  try { data = await res.json() } catch {}
  if (!res.ok) {
    const msg = data && data.error ? data.error : 'api_admin'
    throw new Error(msg)
  }
  return data
}
async function apiStaff(path, secret, opts = {}) {
  const headers = { 'Content-Type': 'application/json', 'X-Staff-Secret': String(secret || '') }
  const res = await fetch(path, { method: 'GET', ...opts, headers })
  let data = null
  try { data = await res.json() } catch {}
  if (!res.ok) {
    const msg = data && data.error ? [
      data.error,
      data.provider ? String(data.provider) : '',
      data.status != null ? String(data.status) : '',
      data.message ? String(data.message) : ''
    ].filter(Boolean).join(':') : 'api_staff'
    throw new Error(msg)
  }
  return data
}
async function sendVenuePinAdminWelcome() {
  try {
    const secret = q('admin-secret-welcome')?.value.trim() || '2207'
    let venueId = 'default'
    try {
      const u = new URL(location.href)
      const v = u.searchParams.get('venueId') || ''
      if (v) venueId = v
    } catch {}
    await apiStaff('/api/admin/venues/pin/send', secret, { method: 'POST', body: JSON.stringify({ venueId }) })
    showError('PIN enviado al email')
    setTimeout(() => showError(''), 1000)
  } catch (e) { showError(String(e.message)) }
}

function showModal(title, msg, type = 'info') {
  const m = q('modal')
  const t = q('modal-text')
  const tt = q('modal-title')
  if (!m || !t || !tt) return
  try { const btn = q('modal-action'); if (btn) btn.remove() } catch {}
  try { const inp = q('modal-input'); if (inp) inp.remove() } catch {}
  m.classList.remove('type-error', 'type-success', 'type-info')
  m.classList.add('type-' + type)
  tt.textContent = title || ''
  tt.style.display = title ? '' : 'none'
  t.textContent = msg || ''
  m.classList.add('show')
  S.modalShownAt = Date.now()
  if (S.timers.modalHide) { try { clearTimeout(S.timers.modalHide) } catch {}; S.timers.modalHide = 0 }
}
function showError(msg) {
  if (!msg) {
    const ttl = Number(S.messageTTL || 0)
    const since = Date.now() - Number(S.modalShownAt || 0)
    if (ttl > 0 && since < ttl) {
      const rem = ttl - since
      if (S.timers.modalHide) { try { clearTimeout(S.timers.modalHide) } catch {} }
      S.timers.modalHide = setTimeout(() => { const m = q('modal'); if (m) m.classList.remove('show'); S.timers.modalHide = 0 }, rem)
      return
    }
    const m = q('modal'); if (m) m.classList.remove('show'); return
  }
  showModal('', msg || '', 'error')
}
function showInfo(msg) { showModal('Info', msg || '', 'info') }
function showSuccess(msg) { showModal('Listo', msg || '', 'success') }
function showImageModal(url) {
  showModal('', '', 'info')
  const t = q('modal-text')
  if (!t) return
  try { t.innerHTML = '' } catch {}
  const img = document.createElement('img')
  img.src = url || ''
  img.style.maxWidth = '92vw'
  img.style.maxHeight = '80vh'
  img.style.borderRadius = '12px'
  img.style.border = '1px solid #333'
  t.append(img)
}
function showModalAction(title, msg, btnText, handler, type = 'info') {
  showModal(title, msg, type)
  const row = document.querySelector('#modal .row')
  if (!row) return
  let btn = q('modal-action')
  if (btn) { try { btn.remove() } catch {} }
  btn = document.createElement('button')
  btn.id = 'modal-action'
  btn.textContent = btnText || 'Aceptar'
  btn.onclick = () => { try { const m = q('modal'); if (m) m.classList.remove('show') } catch {}; if (typeof handler === 'function') handler() }
  const closeBtn = q('modal-close')
  if (closeBtn && closeBtn.parentElement === row) row.insertBefore(btn, closeBtn)
  else row.append(btn)
}
async function confirmAction(paraphrase) {
  return await new Promise(resolve => {
    const m = q('modal')
    const closeBtn = q('modal-close')
    const onCancel = () => { try { if (m) m.classList.remove('show') } catch {}; cleanup(); resolve(false) }
    const onEsc = (e) => { if (e.key === 'Escape') { onCancel() } }
    const onOverlay = (e) => { if (e.target && e.target.id === 'modal') { onCancel() } }
    const cleanup = () => {
      try { document.removeEventListener('keydown', onEsc) } catch {}
      try { if (m) m.removeEventListener('click', onOverlay) } catch {}
      try { if (closeBtn) closeBtn.onclick = () => { const mm = q('modal'); if (mm) mm.classList.remove('show') } } catch {}
    }
    showModalAction('Confirmar', paraphrase || '', 'Aceptar', () => { cleanup(); resolve(true) }, 'info')
    try { document.addEventListener('keydown', onEsc) } catch {}
    try { if (m) m.addEventListener('click', onOverlay) } catch {}
    try { if (closeBtn) closeBtn.onclick = onCancel } catch {}
  })
}
async function promptInput(title, placeholder = '') {
  return await new Promise(resolve => {
    showModal(title || '', '', 'info')
    const t = q('modal-text')
    const row = document.querySelector('#modal .row')
    const closeBtn = q('modal-close')
    if (!t || !row || !closeBtn) { resolve(''); return }
    const inp = document.createElement('input')
    inp.id = 'modal-input'
    inp.type = 'text'
    inp.placeholder = placeholder || ''
    inp.style.width = '100%'
    t.innerHTML = ''
    t.appendChild(inp)
    let btn = q('modal-action')
    if (btn) { try { btn.remove() } catch {} }
    btn = document.createElement('button')
    btn.id = 'modal-action'
    btn.textContent = 'Aceptar'
    btn.onclick = () => { try { const m = q('modal'); if (m) m.classList.remove('show') } catch {}; resolve(String(inp.value || '').trim()) }
    if (closeBtn && closeBtn.parentElement === row) row.insertBefore(btn, closeBtn)
    else row.append(btn)
    inp.focus()
    const onEnter = (e) => { if (e.key === 'Enter') { btn.click() } }
    inp.addEventListener('keydown', onEnter)
    closeBtn.onclick = () => { try { const m = q('modal'); if (m) m.classList.remove('show') } catch {}; resolve('') }
  })
}
function formatPriceShort(p) {
  const n = Number(p || 0)
  if (n >= 1000) {
    const k = n / 1000
    const txt = k % 1 === 0 ? String(k) : k.toFixed(1)
    return txt + 'k'
  }
  return String(n)
}
function scheduleUserSSEReconnect() {
  if (S.timers.userReconnect) return
  S.timers.userReconnect = setTimeout(() => { S.timers.userReconnect = 0; startEvents() }, 3000)
}
function scheduleStaffSSEReconnect() {
  if (!S.sessionId) return
  if (S.timers.staffReconnect) return
  S.timers.staffReconnect = setTimeout(() => { S.timers.staffReconnect = 0; startStaffEvents() }, 3000)
}
function scheduleCatalogSave() {
  if (S.timers.catalogSave) { try { clearTimeout(S.timers.catalogSave) } catch {} }
  S.timers.catalogSave = setTimeout(async () => { S.timers.catalogSave = 0; try { await saveStaffCatalog() } catch {} }, 800)
}
function scheduleLater(key, fn, wait = 500) {
  S.sched = S.sched || {}
  if (S.sched[key]) { try { clearTimeout(S.sched[key]) } catch {} }
  S.sched[key] = setTimeout(async () => {
    S.sched[key] = 0
    const lk = `_${key}_loading`
    S.loading = S.loading || {}
    if (S.loading[lk]) { scheduleLater(key, fn, wait); return }
    S.loading[lk] = true
    try { await fn() } catch {}
    S.loading[lk] = false
  }, wait)
}
function scheduleStaffOrdersUpdate() {
  scheduleLater('staff_orders', async () => {
    S.ui = S.ui || {}
    if (S.ui.freezeStaffOrders) { scheduleStaffOrdersUpdate(); return }
    await loadOrders()
    await loadAnalytics()
  }, 500)
}
function scheduleStaffUsersUpdate() { scheduleLater('staff_users', async () => { await loadUsers() }, 500) }
function scheduleStaffWaiterUpdate() { scheduleLater('staff_waiter', async () => { await loadWaiterCalls(); await loadAnalytics() }, 500) }
function scheduleStaffReportsUpdate() { scheduleLater('staff_reports', async () => { await loadReports(); await loadAnalytics() }, 500) }
function scheduleStaffAnalyticsUpdate() { scheduleLater('staff_analytics', async () => { await loadAnalytics() }, 500) }
function scheduleUserOrdersUpdate() { scheduleLater('user_orders', async () => { await loadUserOrders() }, 400) }
function scheduleRenderUserHeader() { scheduleLater('user_header', async () => { renderUserHeader() }, 300) }
function scheduleRefreshAvailableList() { scheduleLater('user_avail', async () => { await refreshAvailableList() }, 500) }
function startUserPolls() {
  if (S.timers.userPoll) { try { clearInterval(S.timers.userPoll) } catch {} }
  S.timers.userPoll = setInterval(() => {
    if (S.user && S.user.available && S.nav.current === 'screen-disponibles') scheduleRefreshAvailableList()
    if (S.nav.current === 'screen-orders-user') { scheduleUserOrdersUpdate(); scheduleLater('user_invites', async () => { await loadUserInvitesHistory() }, 600) }
    if (S.nav.current === 'screen-user-home') scheduleRenderUserHeader()
  }, 8000)
}
function startStaffPolls() {
  if (S.timers.staffPoll) { try { clearInterval(S.timers.staffPoll) } catch {} }
  S.timers.staffPoll = setInterval(() => {
    if (S.nav.current === 'screen-staff') { scheduleStaffUsersUpdate(); scheduleStaffOrdersUpdate(); scheduleStaffWaiterUpdate(); scheduleStaffReportsUpdate(); scheduleStaffAnalyticsUpdate() }
  }, 8000)
}

async function join(role, codeOverride = '', pinOverride = '') {
  try {
    showError('')
    const code = codeOverride || (q('join-code') ? q('join-code').value.trim() : '')
    const pin = pinOverride || (q('join-pin') ? q('join-pin').value.trim() : '')
    if (!code) { showError('Ingresa el código de sesión'); return }
    if (role === 'staff' && !pin) { showError('Ingresa el PIN de sesión'); return }
    let alias = ''
    if (role === 'user') {
      alias = (q('alias') ? q('alias').value.trim() : '')
      if (!alias) {
        alias = await promptInput('Ingresa tu alias', 'Tu alias')
      }
      if (!alias) { showError('Ingresa tu alias'); return }
    }
    S.sessionId = code
    let r = null
    try {
      r = await api('/api/join', { method: 'POST', body: JSON.stringify({ sessionId: code, role, pin, alias }) })
    } catch (e) {
      if (role === 'user' && String(e.message) === 'no_session') {
        let active = null
        try { active = await api(`/api/session/active${S.venueId ? ('?venueId=' + encodeURIComponent(S.venueId)) : ''}`) } catch {}
        if (active && active.sessionId) {
          S.sessionId = active.sessionId
          r = await api('/api/join', { method: 'POST', body: JSON.stringify({ sessionId: active.sessionId, role, pin: '', alias }) })
        } else {
          showError('Sin sesión activa para este local'); return
        }
      } else {
        throw e
      }
    }
    S.user = r.user
    S.role = role
    try { saveLocalUser() } catch {}
    if (role === 'user') {
      const aliasInput = q('alias'); if (aliasInput) aliasInput.value = S.user.alias || alias
      show('screen-profile')
    } else {
      show('screen-staff')
      showStaffTab('session')
      await loadSessionInfo()
      startStaffEvents()
      loadOrders()
      loadUsers()
      loadReports()
      loadStaffCatalogEditor()
    }
    startEvents()
  } catch (e) {
    showError(String(e.message))
  }
}

async function saveProfile() {
  const alias = q('alias').value.trim()
  const tableId = (q('profile-table') ? q('profile-table').value.trim() : '')
  const file = q('selfie').files[0]
  let selfie = ''
  if (file) {
    selfie = await processSelfie(file).catch(() => '')
    if (!selfie) { showError('Selfie inválida o muy grande'); setTimeout(() => showError(''), 1400); return }
  }
  if (!alias) { showError('Ingresa tu alias'); setTimeout(() => showError(''), 1200); return }
  if (!tableId) { showError('Ingresa tu mesa'); setTimeout(() => showError(''), 1200); return }
  if (!file) { showError('Debes subir tu selfie'); setTimeout(() => showError(''), 1400); return }
  await api('/api/user/profile', { method: 'POST', body: JSON.stringify({ userId: S.user.id, alias, selfie }) })
  await api('/api/user/change-table', { method: 'POST', body: JSON.stringify({ userId: S.user.id, newTable: tableId }) })
  S.user.alias = alias
  S.user.selfie = selfie
  S.user.tableId = tableId
  try { saveLocalUser() } catch {}
  q('selfie-note').textContent = 'Selfie cargada'
  const ua = q('user-alias'), us = q('user-selfie'); if (ua) ua.textContent = S.user.alias || S.user.id; if (us) us.src = S.user.selfie || ''
  const ut = q('user-table'); if (ut) ut.textContent = S.user.tableId || '-'
  show('screen-user-home')
}

function dataUrlBytes(d) {
  const m = String(d || '').match(/^data:.*;base64,(.+)$/)
  return m ? Math.floor(m[1].length * 3 / 4) : 0
}
function loadImageFromFile(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader()
    reader.onload = () => {
      const img = new Image()
      img.onload = () => resolve(img)
      img.onerror = reject
      img.src = reader.result
    }
    reader.onerror = reject
    reader.readAsDataURL(file)
  })
}
async function processSelfie(file) {
  const img = await loadImageFromFile(file)
  const max = 640
  const ratio = Math.min(1, max / Math.max(img.width || max, img.height || max))
  const w = Math.max(1, Math.floor((img.width || max) * ratio))
  const h = Math.max(1, Math.floor((img.height || max) * ratio))
  const canvas = document.createElement('canvas')
  canvas.width = w
  canvas.height = h
  const ctx = canvas.getContext('2d')
  ctx.drawImage(img, 0, 0, w, h)
  let q = 0.8
  let out = canvas.toDataURL('image/jpeg', q)
  let tries = 0
  while (dataUrlBytes(out) > 500 * 1024 && tries < 5) {
    q = Math.max(0.4, q - 0.1)
    out = canvas.toDataURL('image/jpeg', q)
    tries++
  }
  if (dataUrlBytes(out) > 500 * 1024) {
    q = 0.8
    out = canvas.toDataURL('image/webp', q)
    tries = 0
    while (dataUrlBytes(out) > 500 * 1024 && tries < 5) {
      q = Math.max(0.4, q - 0.1)
      out = canvas.toDataURL('image/webp', q)
      tries++
    }
  }
  return dataUrlBytes(out) <= 500 * 1024 ? out : ''
}

async function setAvailable() {
  const next = q('switch-available') ? q('switch-available').checked : false
  const receiveMode = q('receive-mode') ? q('receive-mode').value : 'all'
  const zone = q('zone') ? q('zone').value.trim() : ''
  const prev = !!(S.user && S.user.available)
  const modeTxt = receiveMode === 'all' ? 'de todos' : (receiveMode === 'mesas' ? 'de tu zona' : 'solo de invitados')
  const zoneTxt = zone ? `en zona ${zone}` : 'en todas las zonas'
  const phr = next ? `Vas a activar modo disponible para bailar y recibir ${modeTxt} ${zoneTxt}. ¿Confirmas?`
                    : `Vas a desactivar el modo disponible para bailar. ¿Confirmas?`
  const ok = await confirmAction(phr)
  if (!ok) { if (q('switch-available')) q('switch-available').checked = prev; return }
  await api('/api/user/available', { method: 'POST', body: JSON.stringify({ userId: S.user.id, available: next, receiveMode, zone }) })
  if (!S.user) S.user = {}
  S.user.available = next
  S.user.receiveMode = receiveMode
  S.user.zone = zone
  if (next) { setActiveNav('disponibles'); await viewAvailable(); show('screen-user-home') }
}

async function viewAvailable() {
  if (!S.user || !S.user.available) {
    showError('Oye: debes poner modo activo antes de ver las personas disponibles')
    setTimeout(() => showError(''), 1500)
    return
  }
  const r = await api(`/api/users/available?sessionId=${encodeURIComponent(S.sessionId)}&onlyAvailable=true&excludeUserId=${encodeURIComponent(S.user.id)}`)
  const container = q('available-list')
  container.innerHTML = ''
  for (const u of r.users) {
    if (u.id === S.user.id) continue
    if (u.danceState && u.danceState !== 'idle') continue
    if (u.id === S.user.id) continue
    const div = document.createElement('div')
    div.className = 'item'
    const img = document.createElement('img')
    img.width = 48; img.height = 48
    img.src = u.selfie || ''
    const alias = document.createElement('div')
    alias.className = 'alias'
    alias.textContent = u.alias || u.id
    const tbl = document.createElement('div')
    tbl.className = 'zone'
    tbl.textContent = u.tableId ? `Mesa: ${u.tableId}` : ''
    const zone = document.createElement('div')
    zone.className = 'zone'
    zone.textContent = u.zone ? `Zona: ${u.zone}` : ''
    const tagsEl = document.createElement('div')
    tagsEl.className = 'tags'
    if (Array.isArray(u.tags)) {
      for (const t of u.tags) {
        const chip = document.createElement('span')
        chip.className = 'chip'
        chip.textContent = t
        tagsEl.append(chip)
      }
    }
    const row = document.createElement('div')
    row.className = 'row'
    const bDance = document.createElement('button')
    const busy = (u.danceState && u.danceState !== 'idle')
    bDance.textContent = busy ? 'Ocupado' : 'Bailar'
    bDance.disabled = !!busy
    bDance.onclick = () => sendInviteQuick(u)
    const statusChip = document.createElement('span')
    statusChip.className = 'chip ' + (u.danceState === 'dancing' ? 'success' : (u.danceState === 'waiting' ? 'pending' : ''))
    statusChip.textContent = u.danceState === 'dancing' ? `Bailando con ${u.partnerAlias || ''}` :
                             u.danceState === 'waiting' ? `Esperando con ${u.partnerAlias || ''}` : ''
    const bConsumo = document.createElement('button'); bConsumo.textContent = 'Invitar'; bConsumo.onclick = () => { setReceiver(u); q('consumption-target').value = u.id; openConsumption() }
    row.append(bDance, bConsumo, statusChip)
    div.append(img, alias, tbl, zone, tagsEl, row)
    container.append(div)
  }
  show('screen-disponibles')
}
async function refreshAvailableList() {
  if (!S.user || !S.user.available) return
  const r = await api(`/api/users/available?sessionId=${encodeURIComponent(S.sessionId)}&onlyAvailable=true&excludeUserId=${encodeURIComponent(S.user.id)}`)
  const container = q('available-list')
  if (!container) return
  container.innerHTML = ''
  for (const u of r.users) {
    if (u.id === S.user.id) continue
    if (u.danceState && u.danceState !== 'idle') continue
    if (u.id === S.user.id) continue
    const div = document.createElement('div')
    div.className = 'item'
    const img = document.createElement('img')
    img.width = 48; img.height = 48
    img.src = u.selfie || ''
    const alias = document.createElement('div')
    alias.className = 'alias'
    alias.textContent = u.alias || u.id
    const tbl = document.createElement('div')
    tbl.className = 'zone'
    tbl.textContent = u.tableId ? `Mesa: ${u.tableId}` : ''
    const zone = document.createElement('div')
    zone.className = 'zone'
    zone.textContent = u.zone ? `Zona: ${u.zone}` : ''
    const tagsEl = document.createElement('div')
    tagsEl.className = 'tags'
    if (Array.isArray(u.tags)) {
      for (const t of u.tags) {
        const chip = document.createElement('span')
        chip.className = 'chip'
        chip.textContent = t
        tagsEl.append(chip)
      }
    }
    const row = document.createElement('div')
    row.className = 'row'
    const bDance = document.createElement('button')
    const busy = (u.danceState && u.danceState !== 'idle')
    bDance.textContent = busy ? 'Ocupado' : 'Bailar'
    bDance.disabled = !!busy
    bDance.onclick = () => sendInviteQuick(u)
    const statusChip = document.createElement('span')
    statusChip.className = 'chip ' + (u.danceState === 'dancing' ? 'success' : (u.danceState === 'waiting' ? 'pending' : ''))
    statusChip.textContent = u.danceState === 'dancing' ? `Bailando con ${u.partnerAlias || ''}` :
                             u.danceState === 'waiting' ? `Esperando con ${u.partnerAlias || ''}` : ''
    const bConsumo = document.createElement('button'); bConsumo.textContent = 'Invitar'; bConsumo.onclick = () => { setReceiver(u); q('consumption-target').value = u.id; openConsumption() }
    row.append(bDance, bConsumo, statusChip)
    div.append(img, alias, tbl, zone, tagsEl, row)
    container.append(div)
  }
}
async function viewAvailableByTable() {
  if (!S.user || !S.user.available) {
    showError('Oye: debes poner modo activo antes de ver las personas disponibles')
    setTimeout(() => showError(''), 1500)
    return
  }
  if (!S.user.tableId) { showError('Debes seleccionar tu mesa'); setTimeout(() => showError(''), 1200); openSelectTable(); return }
  const r = await api(`/api/users/available?sessionId=${encodeURIComponent(S.sessionId)}&onlyAvailable=true&excludeUserId=${encodeURIComponent(S.user.id)}`)
  const list = (r.users || []).filter(u => u.tableId === S.user.tableId && (u.danceState === 'idle' || !u.danceState))
  const container = q('available-list')
  container.innerHTML = ''
  for (const u of list) {
    const div = document.createElement('div')
    div.className = 'item'
    const img = document.createElement('img')
    img.width = 48; img.height = 48
    img.style.borderRadius = '50%'
    img.src = u.selfie || ''
    const alias = document.createElement('div')
    alias.className = 'alias'
    alias.textContent = u.alias || u.id
    const tagsEl = document.createElement('div')
    tagsEl.className = 'tags'
    if (Array.isArray(u.tags)) {
      for (const t of u.tags) {
        const chip = document.createElement('span')
        chip.className = 'chip'
        chip.textContent = t
        tagsEl.append(chip)
      }
    }
    const zone = document.createElement('div')
    zone.className = 'zone'
    zone.textContent = u.zone ? `Zona: ${u.zone}` : ''
    const row = document.createElement('div')
    row.className = 'row'
    const bDance = document.createElement('button'); bDance.textContent = 'Bailar'; bDance.onclick = () => sendInviteQuick(u)
    const bConsumo = document.createElement('button'); bConsumo.textContent = 'Invitar'; bConsumo.onclick = () => { setReceiver(u); q('consumption-target').value = u.id; openConsumption() }
    row.append(bDance, bConsumo)
    div.append(img, alias, tagsEl, zone, row)
    container.append(div)
  }
  show('screen-disponibles')
}
function showAvailableChoice() {
  show('screen-disponibles-select')
}

let inviteMsgType = 'bailamos'
function openInvite(u) {
  S.currentInvite = u
  q('invite-person').textContent = `A ${u.alias || u.id}`
  inviteMsgType = 'bailamos'
  show('screen-user-invite')
}

function chooseMsg(e) {
  inviteMsgType = e.target.dataset.msg
}

async function sendInvite() {
  if (!S.currentInvite) return
  const name = S.currentInvite.alias || S.currentInvite.id
  const ok = await confirmAction(`Vas a invitar a ${name} con el mensaje "${inviteMsgType}". ¿Confirmas?`)
  if (!ok) return
  await api('/api/invite/dance', { method: 'POST', body: JSON.stringify({ fromId: S.user.id, toId: S.currentInvite.id, messageType: inviteMsgType }) })
  show('screen-user-home')
}
function setReceiver(u) {
  const el = q('avail-receiver-id')
  if (el) el.textContent = u.id
}
async function sendInviteQuick(u) {
  setReceiver(u)
  S.currentInvite = u
  inviteMsgType = 'bailamos'
  const name = u.alias || u.id
  const ok = await confirmAction(`Vas a invitar a ${name} con el mensaje "bailamos". ¿Confirmas?`)
  if (!ok) return
  await api('/api/invite/dance', { method: 'POST', body: JSON.stringify({ fromId: S.user.id, toId: u.id, messageType: 'bailamos' }) })
  show('screen-disponibles')
}

function startEvents() {
  if (!S.user) return
  if (S.sse) S.sse.close()
  S.sse = new EventSource(`/api/events/user?userId=${encodeURIComponent(S.user.id)}`)
  S.sse.onopen = () => { if (S.timers.userReconnect) { try { clearTimeout(S.timers.userReconnect) } catch {}; S.timers.userReconnect = 0 } }
  S.sse.onerror = () => { scheduleUserSSEReconnect() }
  S.sse.addEventListener('dance_invite', e => {
    const data = JSON.parse(e.data)
    S.invitesQueue.push({ type: 'dance', id: data.invite.id, invite: data.invite })
    S.notifications.invites = (S.notifications.invites || 0) + 1
    setBadgeNav('disponibles', S.notifications.invites)
    if (document.hidden) {
      const msg = `Invitación de baile de ${data.invite.from.alias}`
      S.missed.push(msg)
    }
    if (!S.inInviteFlow) { showNextInvite() }
  })
  S.sse.addEventListener('match', e => {
    const data = JSON.parse(e.data)
    showError(`Match con ${data.with.alias}`)
    setTimeout(() => showError(''), 1500)
  })
  S.sse.addEventListener('dance_status', e => {
    const data = JSON.parse(e.data)
    S.user = S.user || {}
    S.user.danceState = data.state || 'idle'
    S.user.partnerAlias = data.partner ? (data.partner.alias || data.partner.id || '') : S.user.partnerAlias
    if (data.meeting && data.meeting.id) { S.meeting = data.meeting }
    scheduleRenderUserHeader()
  })
  S.sse.addEventListener('invite_result', e => {
    const data = JSON.parse(e.data)
    stopInviteCountdown()
    const invId = String(data.inviteId || '')
    if (invId) {
      const before = S.invitesQueue.length
      S.invitesQueue = S.invitesQueue.filter(x => !(x.type === 'dance' && String(x.id || '') === invId))
      const removed = before - S.invitesQueue.length
      if (removed > 0) {
        S.notifications.invites = Math.max(0, (S.notifications.invites || 0) - removed)
        setBadgeNav('disponibles', S.notifications.invites)
      }
    }
    if (data.status === 'aceptado') {
      S.meeting = data.meeting
      S.isMeetingReceiver = !!(S.currentInvite && data.inviteId && S.currentInvite.id === data.inviteId)
      S.currentInvite = null
      if (data.note) { const noteEl = q('meeting-note'); if (noteEl) noteEl.textContent = `Respuesta: ${data.note}`; showError(`Respuesta: ${data.note}`); setTimeout(() => showError(''), 1500) }
      renderMeeting()
    } else if (data.status === 'pasado') {
      if (data.note) {
        const msg = `Respuesta: ${data.note}`
        if (document.hidden) { S.missed.push(msg) } else { showError(msg); setTimeout(() => showError(''), 1500) }
      }
      show('screen-user-home')
      S.inInviteFlow = false
      showNextInvite()
    } else if (data.status === 'expirado') {
      const msg = 'Invitación expirada (30s)'
      if (document.hidden) { S.missed.push(msg) } else { showError(msg); setTimeout(() => showError(''), 1500) }
      show('screen-user-home')
      S.inInviteFlow = false
      showNextInvite()
    }
  })
  S.sse.addEventListener('meeting_plan', e => {
    const data = JSON.parse(e.data)
    if (data.meetingId && S.meeting && data.meetingId === S.meeting.id) {
      S.meetingPlan = data.plan || ''
      const el = q('meeting-plan-display')
      if (el) {
        const planTxt = S.meetingPlan === 'come' ? 'Ven por mí'
                      : S.meetingPlan === 'go' ? 'Ya voy por ti'
                      : S.meetingPlan === 'pista' ? 'Nos vemos en la pista — no me quites la mirada que me pierdo'
                      : ''
        el.textContent = planTxt ? `Plan: ${planTxt}` : ''
      }
      show('screen-meeting')
    }
  })
  S.sse.addEventListener('consumption_invite', e => {
    const data = JSON.parse(e.data)
    S.invitesQueue.push({ type: 'consumption', data })
    S.notifications.invites = (S.notifications.invites || 0) + 1
    setBadgeNav('disponibles', S.notifications.invites)
    if (document.hidden) {
      const msg = `Invitación de consumo de ${data.from.alias}: ${data.product}`
      S.missed.push(msg)
    }
    if (!S.inInviteFlow) { showNextInvite() }
  })
  S.sse.addEventListener('consumption_invite_bulk', e => {
    const data = JSON.parse(e.data)
    S.invitesQueue.push({ type: 'consumption', data })
    S.notifications.invites = (S.notifications.invites || 0) + 1
    setBadgeNav('disponibles', S.notifications.invites)
    if (document.hidden) {
      const listTxt = (Array.isArray(data.items) ? data.items.map(it => `${it.quantity} x ${it.product}`).join(', ') : '')
      const msg = `Invitación de consumo de ${data.from.alias}: ${listTxt}`
      S.missed.push(msg)
    }
    if (!S.inInviteFlow) { showNextInvite() }
  })
  S.sse.addEventListener('consumption_accepted', e => {
    const data = JSON.parse(e.data)
    const msg = `${data.from.alias} aceptó tu invitación: ${data.quantity} x ${data.product}`
    if (document.hidden) { S.missed.push(msg) } else { showSuccess(msg) }
  })
  S.sse.addEventListener('consumption_passed', e => {
    const data = JSON.parse(e.data)
    const listTxt = (Array.isArray(data.items) ? data.items.map(it => `${it.quantity} x ${it.product}`).join(', ') : data.product)
    const msg = `${data.to.alias} pasó tu invitación: ${listTxt}`
    if (document.hidden) { S.missed.push(msg) } else { showError(msg); setTimeout(() => showError(''), 1500) }
  })
  S.sse.addEventListener('order_update', e => {
    const data = JSON.parse(e.data)
    if (document.hidden) { S.missed.push('Se actualizó tu orden') } else { scheduleUserOrdersUpdate() }
  })
  S.sse.addEventListener('waiter_update', e => {
    const data = JSON.parse(e.data)
    const msg = `Estado de tu llamado: ${data.call.status}`
    if (document.hidden) { S.missed.push(msg) } else { showError(msg); setTimeout(() => showError(''), 1200) }
  })
  S.sse.addEventListener('meeting_expired', e => {
    S.meeting = null
    show('screen-user-home')
    S.inInviteFlow = false
    showNextInvite()
  })
  S.sse.addEventListener('thanks', e => {
    const data = JSON.parse(e.data)
    showSuccess(`Mensaje de ${data.from.alias}: ${data.message}`)
  })
  startUserPolls()
  loadPendingInvites()
}

function renderMeeting() {
  const m = S.meeting
  const left = Math.max(0, Math.floor((m.expiresAt - Date.now()) / 1000))
  q('meeting-info').textContent = `Punto: ${m.point} • Tiempo: ${left}s`
  const noteEl = q('meeting-note')
  const showChoices = !!S.isMeetingReceiver
  const bCome = q('btn-meet-come'), bGo = q('btn-meet-go'), bPista = q('btn-meet-pista')
  const bConfirm = q('btn-meeting-confirm')
  if (bCome) bCome.style.display = showChoices ? '' : 'none'
  if (bGo) bGo.style.display = showChoices ? '' : 'none'
  if (bPista) bPista.style.display = showChoices ? '' : 'none'
  if (bConfirm) bConfirm.style.display = showChoices ? '' : 'none'
  const mpd = q('meeting-plan-display')
  if (mpd) {
    const planTxt = S.meetingPlan === 'come' ? 'Ven por mí'
                  : S.meetingPlan === 'go' ? 'Ya voy por ti'
                  : S.meetingPlan === 'pista' ? 'Nos vemos en la pista — no me quites la mirada que me pierdo'
                  : ''
    mpd.textContent = planTxt ? `Plan: ${planTxt}` : ''
  }
  show('screen-meeting')
}

async function respondInvite(accept) {
  if (S.consumptionReq) {
    const hasItems = Array.isArray(S.consumptionReq.items) && S.consumptionReq.items.length > 0
    const listTxt = hasItems ? S.consumptionReq.items.map(it => `${it.quantity} x ${it.product}`).join(', ') : S.consumptionReq.product
    const phr = accept ? `Vas a aceptar invitación de consumo de ${S.consumptionReq.from.alias} (${listTxt}). ¿Confirmas?`
                       : `Vas a ignorar invitación de consumo. ¿Confirmas?`
    const ok = await confirmAction(phr)
    if (!ok) return
    if (accept) {
      if (hasItems) {
        await api('/api/consumption/respond/bulk', { method: 'POST', body: JSON.stringify({ fromId: S.consumptionReq.from.id, toId: S.user.id, items: S.consumptionReq.items, action: 'accept', requestId: S.consumptionReq.requestId || '' }) })
        openThanks(S.consumptionReq.from.id, 'consumption')
      } else {
        await api('/api/consumption/respond', { method: 'POST', body: JSON.stringify({ fromId: S.consumptionReq.from.id, toId: S.user.id, product: S.consumptionReq.product, action: 'accept', requestId: S.consumptionReq.requestId || '' }) })
        openThanks(S.consumptionReq.from.id, 'consumption')
      }
    }
    S.consumptionReq = null
    show('screen-user-home')
    S.inInviteFlow = false
    showNextInvite()
    return
  }
  if (!S.currentInvite) return
  const id = S.currentInvite.id
  const action = accept ? 'accept' : 'pass'
  const phr2 = accept ? `Vas a aceptar invitación de baile. ¿Confirmas?` : `Vas a pasar la invitación de baile. ¿Confirmas?`
  const ok2 = await confirmAction(phr2)
  if (!ok2) return
  stopInviteCountdown()
  const note = (q('invite-response') ? q('invite-response').value.trim().slice(0, 120) : '')
  await api('/api/invite/respond', { method: 'POST', body: JSON.stringify({ inviteId: id, action, note }) })
  S.notifications.invites = Math.max(0, (S.notifications.invites || 0) - 1)
  setBadgeNav('disponibles', S.notifications.invites)
  if (!accept) show('screen-user-home')
  if (!accept) { S.inInviteFlow = false; showNextInvite() }
}

function openThanks(toId, context) {
  showModal('Agradecer', 'Elige un mensaje para agradecer', 'success')
  const row = document.querySelector('#modal .row')
  if (!row) return
  const options = ['¡Gracias!', 'Mil gracias, cuando quieras', 'Gracias por la invitación']
  for (const txt of options) {
    const b = document.createElement('button')
    b.textContent = txt
    b.onclick = async () => {
      try {
        const m = q('modal'); if (m) m.classList.remove('show')
      } catch {}
      await api('/api/thanks/send', { method: 'POST', body: JSON.stringify({ fromId: S.user.id, toId, context, message: txt }) })
      showSuccess('Se envió tu agradecimiento')
    }
    row.append(b)
  }
}

async function cancelMeeting() {
  if (!S.meeting) return
  const ok = await confirmAction('Vas a cancelar el encuentro. ¿Confirmas?')
  if (!ok) return
  await api('/api/meeting/cancel', { method: 'POST', body: JSON.stringify({ meetingId: S.meeting.id }) })
  S.meeting = null
  show('screen-user-home')
  S.inInviteFlow = false
  showNextInvite()
}

async function confirmMeeting() {
  if (!S.meeting) return
  const plan = S.meetingPlan || ''
  const planTxt = plan === 'come' ? 'Ven por mí' : plan === 'go' ? 'Ya voy por ti' : plan === 'pista' ? 'Nos vemos en la pista — no me quites la mirada que me pierdo' : ''
  const phr = planTxt ? `Vas a confirmar el encuentro: ${planTxt}. ¿Confirmas?` : 'Vas a confirmar el encuentro. ¿Confirmas?'
  const ok = await confirmAction(phr)
  if (!ok) return
  showConfetti()
  await api('/api/meeting/confirm', { method: 'POST', body: JSON.stringify({ meetingId: S.meeting.id, plan }) })
  show('screen-user-home')
}
function setMeetingPlan(plan) {
  S.meetingPlan = plan
  const txt = plan === 'come' ? 'Elegiste: Ven por mí'
            : plan === 'go' ? 'Elegiste: Ya voy por ti'
            : plan === 'pista' ? 'Elegiste: Nos vemos en la pista — no me quites la mirada que me pierdo'
            : ''
  const el = q('meeting-plan-display'); if (el) el.textContent = txt
}
function showConfetti() {
  try {
    const container = document.createElement('div')
    container.className = 'confetti'
    const colors = ['#7d88ff','#5868ff','#ff6b6b','#ffb84b','#38d49c','#d43b5a','#e0a33d','#2cab83']
    const pieces = 80
    for (let i = 0; i < pieces; i++) {
      const e = document.createElement('i')
      const c = colors[Math.floor(Math.random()*colors.length)]
      e.style.background = c
      e.style.left = Math.floor(Math.random()*100) + 'vw'
      e.style.animationDelay = (Math.random()*0.8) + 's'
      e.style.transform = `translateY(-20px) rotate(${Math.floor(Math.random()*180)}deg)`
      container.appendChild(e)
    }
    document.body.appendChild(container)
    setTimeout(() => { try { document.body.removeChild(container) } catch {} }, 2400)
  } catch {}
}

async function sendConsumption() {
  const product = q('product').value
  const toId = q('consumption-target').value.trim()
  const qty = Math.max(1, Number(q('quantity').value || 1))
  const note = (q('consumption-note') ? q('consumption-note').value.trim() : '')
  if (!toId) return
  const displayTo = (S.usersIndex && S.usersIndex[toId] ? S.usersIndex[toId].alias : toId)
  const items = (S.cart && S.cart.length) ? S.cart.slice() : (product ? [{ product, quantity: qty }] : [])
  if (!items.length) { showError('Selecciona producto(s)'); return }
  const listTxt = items.map(it => `${it.quantity} x ${it.product}`).join(', ')
  const phr = note ? `Vas a invitar consumo: ${listTxt} para ${displayTo}. Mensaje: "${note}". ¿Confirmas?`
                    : `Vas a invitar consumo: ${listTxt} para ${displayTo}. ¿Confirmas?`
  const ok = await confirmAction(phr)
  if (!ok) return
  if (items.length > 1) {
    await api('/api/consumption/invite/bulk', { method: 'POST', body: JSON.stringify({ fromId: S.user.id, toId, items, note }) })
  } else {
    await api('/api/consumption/invite', { method: 'POST', body: JSON.stringify({ fromId: S.user.id, toId, product: items[0].product, quantity: items[0].quantity, note }) })
  }
  S.cart = []
  renderCart()
  const inp = q('product'); if (inp) inp.value = ''
  const qn = q('quantity'); if (qn) qn.value = '1'
  show('screen-user-home')
}

async function orderSelf() {
  const product = q('product').value
  const qty = Math.max(1, Number(q('quantity').value || 1))
  const items = (S.cart && S.cart.length) ? S.cart.slice() : (product ? [{ product, quantity: qty }] : [])
  if (!items.length) { showError('Selecciona producto(s)'); return }
  const listTxt = items.map(it => `${it.quantity} x ${it.product}`).join(', ')
  const ok = await confirmAction(`Vas a ordenar: ${listTxt} para ti. ¿Confirmas?`)
  if (!ok) return
  for (const it of items) {
    await api('/api/order/new', { method: 'POST', body: JSON.stringify({ userId: S.user.id, product: it.product, quantity: it.quantity, for: 'self' }) })
  }
  S.cart = []
  renderCart()
  const inp = q('product'); if (inp) inp.value = ''
  const qn = q('quantity'); if (qn) qn.value = '1'
  showError('Pedido creado')
  setTimeout(() => showError(''), 1000)
  showConfetti()
}
async function orderTable() {
  const product = q('product').value
  const qty = Math.max(1, Number(q('quantity').value || 1))
  const items = (S.cart && S.cart.length) ? S.cart.slice() : (product ? [{ product, quantity: qty }] : [])
  if (!items.length) { showError('Selecciona producto(s)'); return }
  if (!S.user.tableId) { showError('Debes seleccionar tu mesa'); return }
  const listTxt = items.map(it => `${it.quantity} x ${it.product}`).join(', ')
  const ok = await confirmAction(`Vas a ordenar: ${listTxt} para la mesa ${S.user.tableId}. ¿Confirmas?`)
  if (!ok) return
  for (const it of items) {
    await api('/api/order/new', { method: 'POST', body: JSON.stringify({ userId: S.user.id, product: it.product, quantity: it.quantity, for: 'mesa' }) })
  }
  S.cart = []
  renderCart()
  const inp = q('product'); if (inp) inp.value = ''
  const qn = q('quantity'); if (qn) qn.value = '1'
  showConfetti()
  showError('Pedido para mesa creado')
  setTimeout(() => showError(''), 1000)
}

async function blockUser() {
  const targetId = q('block-id').value.trim()
  if (!targetId) return
  const ok = await confirmAction(`Vas a bloquear a ${targetId}. ¿Confirmas?`)
  if (!ok) return
  await api('/api/block', { method: 'POST', body: JSON.stringify({ userId: S.user.id, targetId }) })
}

async function reportUser() {
  const targetId = q('report-id').value.trim()
  const category = q('report-cat').value.trim()
  const note = q('report-note').value.trim()
  if (!targetId) return
  const ok = await confirmAction(`Vas a reportar a ${targetId} por "${category}". Nota: "${note}". ¿Confirmas?`)
  if (!ok) return
  await api('/api/report', { method: 'POST', body: JSON.stringify({ userId: S.user.id, targetId, category, note }) })
}

async function startStaffSession() {
  try {
    showError('')
    const pinInput = (q('join-pin') ? q('join-pin').value.trim() : '')
    try {
      const u = new URL(location.href)
      const v = u.searchParams.get('venueId') || ''
      if (v) S.venueId = v
    } catch {}
    let r = null
    try { r = await api(`/api/session/active${S.venueId ? ('?venueId=' + encodeURIComponent(S.venueId)) : ''}`) } catch {}
    if (!r || !r.sessionId) r = await api('/api/session/start', { method: 'POST', body: JSON.stringify({ venueId: S.venueId || 'default' }) })
    const joinCodeEl = q('join-code'); if (joinCodeEl) joinCodeEl.value = r.sessionId
    S.sessionId = r.sessionId
    S.venueId = r.venueId || (S.venueId || 'default')
    const pinToUse = pinInput || r.pin || ''
    if (!pinToUse) { showError('Ingresa el PIN'); return }
    const joinRes = await api('/api/join', { method: 'POST', body: JSON.stringify({ sessionId: r.sessionId, role: 'staff', pin: pinToUse }) })
    S.user = joinRes.user
    S.role = 'staff'
    try { saveLocalUser() } catch {}
    show('screen-staff')
    showStaffTab('panel')
    await loadSessionInfo()
    startStaffEvents()
    loadOrders()
    loadUsers()
    loadReports()
    loadAnalytics()
    loadStaffPromos()
    loadStaffCatalogEditor()
  } catch (e) {
    showError(String(e.message))
  }
}

async function endStaffSession() {
  if (!S.sessionId) return
  const ok = await confirmAction('¿Cerrar sesión de la noche y borrar datos?')
  if (!ok) return
  try { await saveStaffCatalog() } catch {}
  await api('/api/session/end', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId }) })
  try { if (S.sse) S.sse.close() } catch {}
  try { if (S.staffSSE) S.staffSSE.close() } catch {}
  S.sessionId = ''; S.user = null; S.role = ''; S.sse = null; S.staffSSE = null
  try { removeLocalUser(S.venueId) } catch {}
  show('screen-welcome')
  showError('Sesión destruida')
  setTimeout(() => showError(''), 1200)
}
async function restartStaffSession() {
  if (S.sessionId) {
    const ok = await confirmAction('¿Destruir sesión actual y crear una nueva?')
    if (!ok) return
    try { await saveStaffCatalog() } catch {}
    try { await api('/api/session/end', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId }) }) } catch {}
    try { if (S.sse) S.sse.close() } catch {}
    try { if (S.staffSSE) S.staffSSE.close() } catch {}
    S.sessionId = ''; S.user = null; S.role = ''; S.sse = null; S.staffSSE = null
    try { removeLocalUser(S.venueId) } catch {}
  }
  await startStaffSession()
}

function startStaffEvents() {
  if (!S.sessionId) return
  try { if (S.staffSSE) S.staffSSE.close() } catch {}
  S.staffSSE = new EventSource(`/api/events/staff?sessionId=${encodeURIComponent(S.sessionId)}`)
  S.staffSSE.onopen = () => { if (S.timers.staffReconnect) { try { clearTimeout(S.timers.staffReconnect) } catch {}; S.timers.staffReconnect = 0 } }
  S.staffSSE.onerror = () => { scheduleStaffSSEReconnect() }
  S.staffSSE.addEventListener('order_new', e => {
    scheduleStaffOrdersUpdate()
  })
  S.staffSSE.addEventListener('order_update', e => {
    scheduleStaffOrdersUpdate()
  })
  S.staffSSE.addEventListener('report', e => {
    scheduleStaffReportsUpdate()
  })
  S.staffSSE.addEventListener('waiter_call', e => {
    const data = JSON.parse(e.data)
    scheduleStaffWaiterUpdate()
    if (S.staffTab !== 'waiter') { showError(`Llamado de mesero: Mesa ${data.call.tableId || '-'}`); setTimeout(() => showError(''), 1500) }
  })
  S.staffSSE.addEventListener('waiter_update', e => {
    const data = JSON.parse(e.data)
    scheduleStaffWaiterUpdate()
    if (S.staffTab !== 'waiter') { showError(`Llamado actualizado: ${data.call.status}`); setTimeout(() => showError(''), 1200) }
  })
  S.staffSSE.addEventListener('table_closed', e => {
    scheduleStaffAnalyticsUpdate()
    viewStaffTableHistory()
  })
  S.staffSSE.addEventListener('catalog_update', e => {
    scheduleLater('staff_catalog', async () => { await loadStaffCatalogEditor() }, 500)
  })
  startStaffPolls()
}

async function ensureSessionActiveOffer() {
  try {
    let v = S.venueId
    if (!v) {
      try { const u = new URL(location.href); v = u.searchParams.get('venueId') || '' } catch {}
    }
    if (!v) v = 'default'
    const r = await api(`/api/session/active?venueId=${encodeURIComponent(v)}`).catch(() => null)
    if (r && r.sessionId && r.venueId === v) return true
    show('screen-staff-welcome')
    return false
  } catch { return false }
}
async function ensureCatalogIndex() {
  if (S.catalogIndex) return
  try {
    const r = await api(`/api/catalog${S.sessionId ? ('?sessionId=' + encodeURIComponent(S.sessionId)) : ''}`)
    const idx = {}
    for (const it of r.items || []) {
      const key = String(it.name || '').toLowerCase()
      if (key) idx[key] = it
    }
    S.catalogIndex = idx
  } catch {}
}
function formatOrderProductFull(name) {
  const key = String(name || '').toLowerCase()
  const it = S.catalogIndex ? S.catalogIndex[key] : null
  if (!it) return name
  const cats = { cervezas: 'Cerveza', botellas: 'Botella', cocteles: 'Coctel', sodas: 'Soda', otros: 'Otro' }
  const cat = cats[(it.category || '').toLowerCase()] || (it.category || '')
  const sub = String(it.subcategory || '').trim()
  if (cat && sub) return `${cat} • ${sub} • ${it.name}`
  if (cat) return `${cat} • ${it.name}`
  return it.name
}
function formatTimeShort(ms) {
  const t = Number(ms || 0)
  if (!t || Number.isNaN(t)) return ''
  const d = new Date(t)
  const pad = (n) => String(n).padStart(2, '0')
  const hh = pad(d.getHours()), mm = pad(d.getMinutes())
  const dd = pad(d.getDate()), mo = pad(d.getMonth() + 1)
  return `${hh}:${mm} · ${dd}/${mo}`
}
async function loadOrders(state = '') {
  const qs = state ? `&state=${encodeURIComponent(state)}` : ''
  const r = await api(`/api/staff/orders?sessionId=${encodeURIComponent(S.sessionId)}${qs}`)
  const container = q('staff-orders-list') || q('orders')
  container.innerHTML = ''
  const listAsc = (r.orders || []).slice().sort((a, b) => {
    const ta = Number(a.createdAt || 0), tb = Number(b.createdAt || 0)
    if (ta !== tb) return ta - tb
    const ia = String(a.id || ''), ib = String(b.id || '')
    return ia.localeCompare(ib)
  })
  for (const o of listAsc) {
    const div = document.createElement('div')
    div.className = 'card'
    const info = document.createElement('div')
    const chip = document.createElement('span')
    chip.className = 'chip ' + (o.status === 'pendiente_cobro' ? 'pending' : o.status)
    chip.textContent = o.status.replace('_', ' ')
    const mesaInfo = (o.mesaEntrega || o.receiverTable || o.emitterTable) ? ` • Mesa entrega ${o.mesaEntrega || o.receiverTable}` : ''
    const emAlias = (S.usersIndex && S.usersIndex[o.emitterId] ? S.usersIndex[o.emitterId].alias : o.emitterId)
    const reAlias = (S.usersIndex && S.usersIndex[o.receiverId] ? S.usersIndex[o.receiverId].alias : o.receiverId)
    const amountTxt = ` • $${o.total || 0}`
    await ensureCatalogIndex()
    const label = formatOrderProductFull(o.product)
    const timeTxt = o.createdAt ? ` • ${formatTimeShort(o.createdAt)}` : ''
    info.textContent = `${label} x${o.quantity || 1}${amountTxt} • Emisor ${emAlias} → Receptor ${reAlias}${mesaInfo}${timeTxt}`
    info.append(chip)
    if (o.isInvitation) {
      const invChip = document.createElement('span')
      invChip.className = 'chip'
      invChip.textContent = 'Invitación'
      info.append(invChip)
    }
    const row = document.createElement('div')
    row.className = 'row'
    const b1 = document.createElement('button'); b1.textContent = 'Cobrado'; b1.onclick = () => updateOrder(o.id, 'cobrado')
    const b0 = document.createElement('button'); b0.textContent = 'En preparación'; b0.onclick = () => updateOrder(o.id, 'en_preparacion')
    const b2 = document.createElement('button'); b2.textContent = 'Entregado'; b2.onclick = () => updateOrder(o.id, 'entregado')
    const b3 = document.createElement('button'); b3.textContent = 'Cancelar'; b3.onclick = () => updateOrder(o.id, 'cancelado')
    row.append(b0, b1, b2, b3)
    div.append(info, row)
    container.append(div)
  }
  const list = q('staff-orders-list')
  if (list) {
    list.onpointerdown = () => {
      S.ui = S.ui || {}; S.ui.freezeStaffOrders = true
      S.timers = S.timers || {}
      if (S.timers.ordersFreeze) { try { clearTimeout(S.timers.ordersFreeze) } catch {} }
      S.timers.ordersFreeze = setTimeout(() => { S.ui.freezeStaffOrders = false; S.timers.ordersFreeze = 0 }, 1500)
    }
    list.onpointerup = () => { S.ui = S.ui || {}; S.ui.freezeStaffOrders = false; if (S.timers.ordersFreeze) { try { clearTimeout(S.timers.ordersFreeze) } catch {}; S.timers.ordersFreeze = 0 } }
    list.onmouseleave = () => { S.ui = S.ui || {}; S.ui.freezeStaffOrders = false; if (S.timers.ordersFreeze) { try { clearTimeout(S.timers.ordersFreeze) } catch {}; S.timers.ordersFreeze = 0 } }
  }
}

async function updateOrder(id, status) {
  await api(`/api/staff/orders/${id}`, { method: 'POST', body: JSON.stringify({ status }) })
  loadOrders()
}

async function loadUsers() {
  const r = await api(`/api/staff/users?sessionId=${encodeURIComponent(S.sessionId)}`)
  S.usersIndex = {}
  const container = q('staff-users') || q('users')
  container.innerHTML = ''
  for (const u of r.users) {
    S.usersIndex[u.id] = { alias: u.alias || u.id }
    const div = document.createElement('div')
    div.className = 'card'
    const info = document.createElement('div')
    info.textContent = `${u.alias || u.id} • Selfie cargada • ${u.muted ? 'silenciado' : 'activo'}`
    const row = document.createElement('div')
    row.className = 'row'
    const mute = document.createElement('button'); mute.textContent = u.muted ? 'Activar' : 'Silenciar'; mute.onclick = () => moderateUser(u.id, !u.muted)
    row.append(mute)
    div.append(info, row)
    container.append(div)
  }
}

async function loadWaiterCalls() {
  const r = await api(`/api/staff/waiter?sessionId=${encodeURIComponent(S.sessionId)}`)
  const container = q('waiter-calls')
  container.innerHTML = ''
  const list = (r.calls || []).slice().sort((a, b) => Number(a.ts || 0) - Number(b.ts || 0))
  for (const c of list) {
    const div = document.createElement('div')
    div.className = 'card'
    const info = document.createElement('div')
    info.textContent = `Mesa ${c.tableId || '-'} • ${c.userAlias ? c.userAlias : c.userId} • ${c.reason} • ${c.status} • ${formatTimeShort(c.ts)}`
    const row = document.createElement('div')
    row.className = 'row'
    const b1 = document.createElement('button'); b1.textContent = 'En camino'; b1.onclick = async () => { await api(`/api/staff/waiter/${c.id}`, { method: 'POST', body: JSON.stringify({ status: 'en_camino' }) }); loadWaiterCalls() }
    const b2 = document.createElement('button'); b2.textContent = 'Atendido'; b2.onclick = async () => { await api(`/api/staff/waiter/${c.id}`, { method: 'POST', body: JSON.stringify({ status: 'atendido' }) }); loadWaiterCalls() }
    const b3 = document.createElement('button'); b3.textContent = 'Cancelar'; b3.onclick = async () => { await api(`/api/staff/waiter/${c.id}`, { method: 'POST', body: JSON.stringify({ status: 'cancelado' }) }); loadWaiterCalls() }
    row.append(b1, b2, b3)
    div.append(info, row)
    container.append(div)
  }
  const bWaiter = q('badge-tab-waiter')
  if (bWaiter) { const v = (r.calls || []).filter(x => x.status !== 'atendido' && x.status !== 'cancelado').length; bWaiter.classList.toggle('show', v > 0); bWaiter.textContent = v > 9 ? '9+' : String(v) }
}
async function approveSelfie(userId) {
  await api('/api/moderation/approve-selfie', { method: 'POST', body: JSON.stringify({ staffId: S.user.id, userId }) })
  loadUsers()
}

async function moderateUser(userId, muted) {
  await api('/api/staff/moderate', { method: 'POST', body: JSON.stringify({ staffId: S.user.id, userId, muted }) })
  loadUsers()
}

async function loadReports() {
  const r = await api(`/api/staff/reports?sessionId=${encodeURIComponent(S.sessionId)}`)
  const container = q('reports')
  container.innerHTML = ''
  for (const rep of r.reports) {
    const div = document.createElement('div')
    div.className = 'card'
    div.textContent = `De ${rep.fromId} sobre ${rep.targetId} • ${rep.category} • ${rep.note}`
    container.append(div)
  }
}
function bind() {
  const btnJoinUser = q('btn-join-user'); if (btnJoinUser) btnJoinUser.onclick = () => join('user')
  const btnJoinStaff = q('btn-join-staff'); if (btnJoinStaff) btnJoinStaff.onclick = startStaffSession
  const btnSaveProfile = q('btn-save-profile'); if (btnSaveProfile) btnSaveProfile.onclick = saveProfile
  const swAvail = q('switch-available'); if (swAvail) swAvail.onchange = setAvailable
  const receiveModeEl = q('receive-mode'); if (receiveModeEl) receiveModeEl.onchange = setAvailable
  const zoneEl = q('zone'); if (zoneEl) zoneEl.oninput = setAvailable
  const btnViewAvail = q('btn-view-available'); if (btnViewAvail) btnViewAvail.onclick = () => { setActiveNav('disponibles'); showAvailableChoice() }
  const btnViewMenu = q('btn-view-menu'); if (btnViewMenu) btnViewMenu.onclick = () => { setActiveNav('carta'); openMenu() }
  for (const b of document.querySelectorAll('.btn-invite-msg')) b.onclick = chooseMsg
  const btnInviteSend = q('btn-invite-send'); if (btnInviteSend) btnInviteSend.onclick = sendInvite
  const btnInviteAccept = q('btn-invite-accept'); if (btnInviteAccept) btnInviteAccept.onclick = () => respondInvite(true)
  const btnInvitePass = q('btn-invite-pass'); if (btnInvitePass) btnInvitePass.onclick = () => respondInvite(false)
  q('btn-meeting-cancel').onclick = cancelMeeting
  const btnMeetingHome = q('btn-meeting-home'); if (btnMeetingHome) btnMeetingHome.onclick = goHome
  const btnInviteConsumption = q('btn-invite-consumption'); if (btnInviteConsumption) btnInviteConsumption.onclick = openConsumption
  q('btn-consumption-send').onclick = sendConsumption
  const btnAddCart = q('btn-add-to-cart'); if (btnAddCart) btnAddCart.onclick = addToCart
  const btnViewOrders = q('btn-view-orders'); if (btnViewOrders) btnViewOrders.onclick = () => { setActiveNav('orders'); loadUserOrders(); loadUserInvitesHistory(); show('screen-orders-user') }
  const btnWaiterOrder = q('btn-waiter-order'); if (btnWaiterOrder) btnWaiterOrder.onclick = callWaiterOrder
  for (const b of document.querySelectorAll('.btn-waiter-reason')) b.onclick = chooseWaiterReason
  const btnBack = q('btn-back'); if (btnBack) btnBack.onclick = goBack
  const nc = q('nav-carta'), nd = q('nav-disponibles'), nm = q('nav-mesas'), no = q('nav-orders'), nf = q('nav-perfil')
  if (nc) nc.onclick = () => { setActiveNav('carta'); openMenu() }
  if (nd) nd.onclick = () => { setActiveNav('disponibles'); showAvailableChoice() }
  if (nm) nm.onclick = () => { setActiveNav('mesas'); exploreMesas() }
  if (no) no.onclick = () => { setActiveNav('orders'); loadUserOrders(); show('screen-orders-user') }
  const btnShowAllInv = q('btn-invite-show-all'); if (btnShowAllInv) btnShowAllInv.onclick = openInvitesInbox
  const btnPassAllInv = q('btn-pass-all-invites'); if (btnPassAllInv) btnPassAllInv.onclick = passAllDanceInvites
  const btnPassAllCons = q('btn-pass-all-consumption'); if (btnPassAllCons) btnPassAllCons.onclick = passAllConsumptionInvites
  document.addEventListener('visibilitychange', () => {
    if (!document.hidden && Array.isArray(S.missed) && S.missed.length) {
      const msg = S.missed.join('\n')
      showModal('Notificaciones', msg, 'info')
      S.missed = []
    }
  })
  if (nf) nf.onclick = () => { setActiveNav('perfil'); renderUserHeader(); show('screen-user-home') }
  const ua = q('user-alias'); if (ua) { ua.style.cursor = 'pointer'; ua.onclick = () => openEditProfileFocus('alias') }
  const ut = q('user-table'); if (ut) { ut.style.cursor = 'pointer'; ut.onclick = () => openEditProfileFocus('table') }
  const linkStaff = q('link-staff'); if (linkStaff) linkStaff.onclick = (e) => { e.preventDefault(); show('screen-staff-welcome') }
  const fab = q('fab-call'); if (fab) fab.onclick = openCallWaiter
  const bAT = q('btn-avail-by-table'); if (bAT) bAT.onclick = exploreMesas
  const bAA = q('btn-avail-all'); if (bAA) bAA.onclick = viewAvailable
  const btnOrderSelf = q('btn-order-self'); if (btnOrderSelf) btnOrderSelf.onclick = orderSelf
  const btnOrderTable = q('btn-order-table'); if (btnOrderTable) btnOrderTable.onclick = orderTable
  q('btn-block').onclick = blockUser
  q('btn-report').onclick = reportUser
  const btnTheme = q('btn-theme-toggle'); if (btnTheme) btnTheme.onclick = toggleTheme
  const btnThemeTop = q('btn-theme-toggle-top'); if (btnThemeTop) btnThemeTop.onclick = toggleTheme
  const btnThemeWelcome = q('btn-theme-toggle-welcome'); if (btnThemeWelcome) btnThemeWelcome.onclick = toggleTheme
  const modalClose = q('modal-close'); if (modalClose) modalClose.onclick = () => { const m = q('modal'); if (m) m.classList.remove('show') }
  const modalEl = q('modal'); if (modalEl) modalEl.onclick = (e) => { if (e.target && e.target.id === 'modal') modalEl.classList.remove('show') }
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') { const m = q('modal'); if (m) m.classList.remove('show') } })
  const homeBtn = q('btn-home'); if (homeBtn) homeBtn.onclick = goHome
  const tabPanel = q('tab-staff-panel'); if (tabPanel) tabPanel.onclick = () => showStaffTab('panel')
  const tabSession = q('tab-staff-session'); if (tabSession) tabSession.onclick = () => showStaffTab('session')
  const tabOrders = q('tab-staff-orders'); if (tabOrders) tabOrders.onclick = () => showStaffTab('orders')
  const tabMesas = q('tab-staff-mesas'); if (tabMesas) tabMesas.onclick = () => showStaffTab('mesas')
  const tabUsers = q('tab-staff-users'); if (tabUsers) tabUsers.onclick = () => showStaffTab('users')
  const tabWaiter = q('tab-staff-waiter'); if (tabWaiter) tabWaiter.onclick = () => showStaffTab('waiter')
  const tabReportes = q('tab-staff-reportes'); if (tabReportes) tabReportes.onclick = () => showStaffTab('reportes')
  const tabPromos = q('tab-staff-promos'); if (tabPromos) tabPromos.onclick = () => showStaffTab('promos')
  const tabCatalog = q('tab-staff-catalog'); if (tabCatalog) tabCatalog.onclick = () => showStaffTab('catalog')
  const tabAnalytics = q('tab-staff-analytics'); if (tabAnalytics) tabAnalytics.onclick = () => showStaffTab('analytics')
  const btnStaffAnalytics = q('btn-staff-analytics'); if (btnStaffAnalytics) btnStaffAnalytics.onclick = () => showStaffTab('analytics')
  q('btn-start-session-welcome').onclick = startStaffSession
  const btnScan = q('btn-scan-qr'); if (btnScan) btnScan.onclick = startScanQR
  q('btn-end-session').onclick = endStaffSession
  const btnExploreMesas = q('btn-explore-mesas'); if (btnExploreMesas) btnExploreMesas.onclick = exploreMesas
  q('btn-edit-profile').onclick = openEditProfile
  q('btn-edit-save').onclick = saveEditProfile
  // Pausa social eliminada
  const btnViewPromos = q('btn-view-promos'); if (btnViewPromos) btnViewPromos.onclick = viewPromos
  const btnCallWaiter = q('btn-call-waiter'); if (btnCallWaiter) btnCallWaiter.onclick = openCallWaiter
  q('btn-waiter-send').onclick = sendWaiterCall
  q('mesa-only-available').onchange = () => loadMesaPeople(S.currentTableId)
  q('btn-select-table').onclick = openSelectTable
  const heroSelfie = q('user-selfie-hero'); if (heroSelfie) heroSelfie.onclick = () => { const url = S.user?.selfie || ''; if (url) showImageModal(url) }
  q('btn-select-table-save').onclick = saveSelectTable
  q('btn-invite-block').onclick = blockFromInvite
  q('btn-invite-report').onclick = reportFromInvite
  q('btn-meeting-confirm').onclick = confirmMeeting
  const endBtn = q('btn-end-dance'); if (endBtn) endBtn.onclick = finishDance
  const btnMeetCome = q('btn-meet-come'); if (btnMeetCome) btnMeetCome.onclick = () => setMeetingPlan('come')
  const btnMeetGo = q('btn-meet-go'); if (btnMeetGo) btnMeetGo.onclick = () => setMeetingPlan('go')
  const btnMeetPista = q('btn-meet-pista'); if (btnMeetPista) btnMeetPista.onclick = () => setMeetingPlan('pista')
  const btnStaffTableView = q('btn-staff-table-view'); if (btnStaffTableView) btnStaffTableView.onclick = viewStaffTableHistory
  const btnStaffTableClose = q('btn-staff-table-close'); if (btnStaffTableClose) btnStaffTableClose.onclick = closeStaffTable
  const btnStaff2TableView = q('btn-staff2-table-view'); if (btnStaff2TableView) btnStaff2TableView.onclick = viewStaffTableHistory2
  const btnStaff2TableClose = q('btn-staff2-table-close'); if (btnStaff2TableClose) btnStaff2TableClose.onclick = closeStaffTable2
  const btnStaffCatalogSave = q('btn-staff-catalog-save'); if (btnStaffCatalogSave) btnStaffCatalogSave.onclick = saveStaffCatalog
  const btnStaffCatalogUseGlobal = q('btn-staff-catalog-use-global'); if (btnStaffCatalogUseGlobal) btnStaffCatalogUseGlobal.onclick = useGlobalCatalog
  const btnStaffCatalogCopyGlobal = q('btn-staff-catalog-copy-global'); if (btnStaffCatalogCopyGlobal) btnStaffCatalogCopyGlobal.onclick = copyGlobalToSession
  const btnStaffCatalogAdd = q('btn-staff-catalog-add'); if (btnStaffCatalogAdd) btnStaffCatalogAdd.onclick = () => {
    const name = q('staff-catalog-add-name')?.value.trim()
    const price = Number(q('staff-catalog-add-price')?.value || 0)
    const catVal = (q('staff-catalog-add-category')?.value || 'otros').toLowerCase()
    const subVal = (q('staff-catalog-add-subcategory')?.value || '').trim()
    const container = q('staff-catalog-list')
    if (!container || !name) return
    const row = document.createElement('div')
    row.className = 'row'
    const nameInput = document.createElement('input'); nameInput.type = 'text'; nameInput.placeholder = 'Nombre'; nameInput.value = name
    const priceInput = document.createElement('input'); priceInput.type = 'number'; priceInput.min = '0'; priceInput.value = price
    const category = document.createElement('select')
    for (const opt of ['cervezas','botellas','cocteles','sodas','otros']) {
      const o = document.createElement('option')
      o.value = opt
      o.textContent = opt.charAt(0).toUpperCase() + opt.slice(1)
      category.append(o)
    }
    category.value = catVal
    const subInput = document.createElement('input'); subInput.type = 'text'; subInput.placeholder = 'Subcategoría'; subInput.value = subVal
    nameInput.oninput = scheduleCatalogSave
    priceInput.oninput = scheduleCatalogSave
    category.oninput = scheduleCatalogSave
    subInput.oninput = scheduleCatalogSave
    const del = document.createElement('button'); del.textContent = 'Eliminar'; del.onclick = () => { try { row.remove(); scheduleCatalogSave() } catch {} }
    row.append(nameInput, priceInput, category, subInput, del)
    container.append(row)
    const inpName = q('staff-catalog-add-name'); if (inpName) inpName.value = ''
    const inpPrice = q('staff-catalog-add-price'); if (inpPrice) inpPrice.value = ''
    const inpCat = q('staff-catalog-add-category'); if (inpCat) inpCat.value = 'otros'
    const inpSub = q('staff-catalog-add-subcategory'); if (inpSub) inpSub.value = ''
    saveStaffCatalog()
  }
  const anUsers = q('an-users'); if (anUsers) anUsers.onclick = () => { showStaffTab('users'); loadUsers() }
  const anMesas = q('an-mesas'); if (anMesas) anMesas.onclick = () => { showStaffTab('mesas') }
  const anInv = q('an-invites'); if (anInv) anInv.onclick = () => { showStaffTab('orders'); loadOrders() }
  const anAcc = q('an-accepted'); if (anAcc) anAcc.onclick = () => { showStaffTab('orders'); loadOrders('pendiente_cobro') }
  const filt = q('staff-orders-filter'); if (filt) filt.onchange = () => loadOrders(filt.value)
  const copyBtn = q('btn-copy-link')
  if (copyBtn) copyBtn.onclick = async () => {
    try {
      const href = q('share-url')?.href || ''
      await navigator.clipboard.writeText(href)
      showError('Link copiado')
      setTimeout(() => showError(''), 1000)
    } catch (e) { showError('No se pudo copiar') }
  }
  const catalogSearch = q('catalog-search'); if (catalogSearch) catalogSearch.oninput = applyCatalogSearch
  const btnWelcomeVenuePinSend = q('btn-welcome-venue-pin-send'); if (btnWelcomeVenuePinSend) btnWelcomeVenuePinSend.onclick = sendVenuePinAdminWelcome
  const savePB = q('btn-save-public-base')
  if (savePB) savePB.onclick = async () => {
    try {
      const val = q('public-base').value.trim()
      await api('/api/session/public-base', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId, publicBaseUrl: val }) })
      showError('URL pública guardada')
      setTimeout(() => showError(''), 1000)
      await startStaffSession()
    } catch (e) { showError(String(e.message)) }
  }
}

async function exploreMesas() {
  await loadMesasActive()
  show('screen-mesas')
}

async function loadMesasActive() {
  const r = await api(`/api/mesas/active?sessionId=${encodeURIComponent(S.sessionId)}`)
  const container = q('mesas-list')
  container.innerHTML = ''
  for (const m of r.mesas) {
    const div = document.createElement('div')
    div.className = 'card'
    const info = document.createElement('div')
    info.textContent = `${m.tableId} • Personas ${m.people} • Disponibles ${m.disponibles}`
    const tags = document.createElement('div')
    tags.className = 'tags'
    if (Array.isArray(m.tags)) {
      for (const t of m.tags) {
        const chip = document.createElement('span')
        chip.className = 'chip'
        chip.textContent = t
        tags.append(chip)
      }
    }
    const btn = document.createElement('button')
    btn.textContent = 'Ver mesa'
    btn.onclick = () => openMesaView(m.tableId)
    div.append(info, tags, btn)
    container.append(div)
  }
}

function openMesaView(tableId) {
  S.currentTableId = tableId
  q('mesa-title').textContent = `Mesa ${tableId}`
  loadMesaPeople(tableId)
  loadMesaOrders(tableId)
  try { if (S.mesaOrdersInterval) clearInterval(S.mesaOrdersInterval) } catch {}
  S.mesaOrdersInterval = setInterval(() => {
    if (S.currentTableId) loadMesaOrders(S.currentTableId)
  }, 5000)
  show('screen-mesa-view')
}

async function loadMesaPeople(tableId) {
  const only = q('mesa-only-available').checked
  const r = await api(`/api/users/available?sessionId=${encodeURIComponent(S.sessionId)}&onlyAvailable=${only ? 'true':'false'}`)
  const list = r.users.filter(u => u.tableId === tableId)
  const container = q('mesa-people')
  container.innerHTML = ''
  for (const u of list) {
    const div = document.createElement('div')
    div.className = 'item'
    const img = document.createElement('img')
    img.width = 48; img.height = 48
    img.style.borderRadius = '50%'
    img.src = u.selfie || ''
    const alias = document.createElement('div')
    alias.className = 'alias'
    alias.textContent = u.alias || u.id
    const tagsEl = document.createElement('div')
    tagsEl.className = 'tags'
    tagsEl.textContent = (Array.isArray(u.tags) && u.tags.length) ? `Tags: ${u.tags.join(', ')}` : ''
    const row = document.createElement('div')
    row.className = 'row'
    const bInvite = document.createElement('button'); bInvite.textContent = 'Bailar'; bInvite.onclick = () => openInvite(u)
    const bConsumo = document.createElement('button'); bConsumo.textContent = 'Invitar'; bConsumo.onclick = () => { q('consumption-target').value = u.id; openConsumption() }
    row.append(bInvite, bConsumo)
    div.append(img, alias, tagsEl, row)
    container.append(div)
  }
}

async function loadMesaOrders(tableId) {
  const r = await api(`/api/table/orders?sessionId=${encodeURIComponent(S.sessionId)}&tableId=${encodeURIComponent(tableId)}&userId=${encodeURIComponent(S.user.id)}`)
  const container = q('mesa-orders')
  container.innerHTML = ''
  for (const o of r.orders) {
    const div = document.createElement('div')
    div.className = 'card'
    const info = document.createElement('div')
    const chip = document.createElement('span')
    chip.className = 'chip ' + (o.status === 'pendiente_cobro' ? 'pending' : o.status)
    chip.textContent = o.status.replace('_', ' ')
    await ensureCatalogIndex()
    const label = formatOrderProductFull(o.product)
    info.textContent = `${label} • ${o.emitterAlias || o.emitterId} → ${o.receiverAlias || o.receiverId}`
    const row = document.createElement('div')
    row.className = 'row'
    info.append(chip)
    if (o.isInvitation) {
      const invChip = document.createElement('span')
      invChip.className = 'chip'
      invChip.textContent = 'Invitación'
      info.append(invChip)
    }
    div.append(info, row)
    container.append(div)
  }
}

// Reacciones retiradas (saludo, brindis): UI y backend eliminados

function openEditProfile() {
  q('edit-alias').value = S.user.alias || ''
  q('edit-tags').value = (Array.isArray(S.user.prefs?.tags) ? S.user.prefs.tags.join(',') : '')
  q('edit-table').value = S.user.tableId || ''
  show('screen-edit-profile')
}
function renderUserHeader() {
  const ua = q('user-alias'), us = q('user-selfie'), ut = q('user-table')
  if (ua) ua.textContent = S.user?.alias || S.user?.id || ''
  if (us) us.src = S.user?.selfie || ''
  if (ut) ut.textContent = S.user?.tableId || '-'
  const ush = q('user-selfie-hero'); if (ush) ush.src = S.user?.selfie || ''
  const hm = q('user-hero-main')
  if (hm) hm.textContent = S.user?.alias || S.user?.id || 'Tu perfil'
  const tc = q('user-table-chip'); if (tc) tc.textContent = `Mesa ${S.user?.tableId || '-'}`
  const uds = q('user-dance-status')
  if (uds) {
    const st = S.user?.danceState || 'idle'
    const p = S.user?.partnerAlias || ''
    uds.textContent = st === 'waiting' ? (`Esperando para bailar con ${p || 'pareja'}`) :
                      st === 'dancing' ? (`Bailando con ${p || 'pareja'}`) : ''
  }
  const hs = q('user-hero-sub')
  if (hs) {
    const st = S.user?.danceState || 'idle'
    const p = S.user?.partnerAlias || ''
    hs.textContent = st === 'waiting' ? (`Esperando para bailar con ${p || 'pareja'}`) :
                     st === 'dancing' ? (`Bailando con ${p || 'pareja'}`) : ''
  }
  const endBtn = q('btn-end-dance')
  if (endBtn) {
    const st = S.user?.danceState || 'idle'
    endBtn.style.display = (st === 'dancing') ? '' : 'none'
  }
  const header = q('user-header')
  const eq = q('user-equalizer')
  const heq = q('user-hero-equalizer')
  const st2 = S.user?.danceState || 'idle'
  if (header) header.classList.toggle('party', st2 === 'dancing')
  if (eq) eq.style.display = st2 === 'dancing' ? '' : 'none'
  if (heq) heq.style.display = st2 === 'dancing' ? '' : 'none'
}
function openEditProfileFocus(field) {
  openEditProfile()
  const map = { alias: 'edit-alias', tags: 'edit-tags', table: 'edit-table' }
  const id = map[field]
  const el = id ? q(id) : null
  if (el) el.focus()
}

async function saveEditProfile() {
  const alias = q('edit-alias').value.trim()
  const tags = q('edit-tags').value.split(',').map(s => s.trim()).filter(Boolean)
  const tableRaw = q('edit-table').value.trim()
  const tableId = normalizeTableId(tableRaw)
  await api('/api/user/update', { method: 'POST', body: JSON.stringify({ userId: S.user.id, alias, tags, tableId }) })
  showError('Perfil actualizado')
  setTimeout(() => showError(''), 1000)
  show('screen-user-home')
}

 

function openCallWaiter() {
  S.waiterReason = ''
  for (const b of document.querySelectorAll('.btn-waiter-reason')) { b.classList.remove('active') }
  show('screen-call-waiter')
}

async function sendWaiterCall() {
  let reason = ''
  if (S.waiterReason === 'hielo') reason = 'Me puedes traer hielo'
  else if (S.waiterReason === 'pasabocas') reason = 'Me puedes traer pasabocas'
  else if (S.waiterReason === 'limpieza') reason = '¿Puedes limpiar la mesa?'
  else { reason = 'Atención' }
  const phr = reason ? `Vas a llamar al mesero: ${reason}. ¿Confirmas?` : `Vas a llamar al mesero. ¿Confirmas?`
  const ok = await confirmAction(phr)
  if (!ok) return
  if (!S.user.tableId) { showError('Debes seleccionar tu mesa'); setTimeout(() => showError(''), 1200); openSelectTable(); return }
  await api('/api/waiter/call', { method: 'POST', body: JSON.stringify({ userId: S.user.id, reason: (reason || 'Atención') }) })
  showError('Mesero llamado')
  setTimeout(() => showError(''), 1000)
  show('screen-user-home')
}
async function callWaiterQuick() {
  const ok = await confirmAction('¿Confirmas llamar al mesero?')
  if (!ok) return
  if (!S.user.tableId) { showError('Debes seleccionar tu mesa'); setTimeout(() => showError(''), 1200); openSelectTable(); return }
  await api('/api/waiter/call', { method: 'POST', body: JSON.stringify({ userId: S.user.id, reason: 'Atención' }) })
  showError('Mesero llamado')
  setTimeout(() => showError(''), 1000)
  show('screen-user-home')
}
async function callWaiterOrder() {
  const ok = await confirmAction(`Vas a pedir tomar orden en tu mesa ${S.user.tableId || '-'}. ¿Confirmas?`)
  if (!ok) return
  if (!S.user.tableId) { showError('Debes seleccionar tu mesa'); setTimeout(() => showError(''), 1200); openSelectTable(); return }
  await api('/api/waiter/call', { method: 'POST', body: JSON.stringify({ userId: S.user.id, reason: 'Tomar orden en mesa' }) })
  showError('Mesero pedido para tomar orden')
  setTimeout(() => showError(''), 1200)
}
function chooseWaiterReason(e) {
  const el = e.currentTarget
  const val = el && el.getAttribute('data-reason') || ''
  S.waiterReason = val
  for (const b of document.querySelectorAll('.btn-waiter-reason')) b.classList.remove('active')
  el.classList.add('active')
  const other = q('waiter-other'); if (other) other.style.display = val === 'otro' ? '' : 'none'
  if (val && val !== 'otro') { sendWaiterCall() }
}

async function viewPromos() {
  const r = await api(`/api/promos?sessionId=${encodeURIComponent(S.sessionId)}`)
  const container = q('promos-list')
  container.innerHTML = ''
  for (const p of r.promos || []) {
    const div = document.createElement('div')
    div.className = 'card'
    div.textContent = p.title || ''
    container.append(div)
  }
  show('screen-promos')
}

function openSelectTable() {
  loadSelectTableList()
  q('select-table-manual').value = ''
  show('screen-select-table')
}

async function loadSelectTableList() {
  const r = await api(`/api/mesas/active?sessionId=${encodeURIComponent(S.sessionId)}`)
  const container = q('select-table-list')
  container.innerHTML = ''
  for (const m of r.mesas) {
    const div = document.createElement('div')
    div.className = 'item'
    div.textContent = `${m.tableId} • Personas ${m.people}`
    div.onclick = async () => {
      await api('/api/user/change-table', { method: 'POST', body: JSON.stringify({ userId: S.user.id, newTable: m.tableId }) })
      show('screen-user-home')
    }
    container.append(div)
  }
}

async function saveSelectTable() {
  const manualRaw = q('select-table-manual').value.trim()
  const manual = normalizeTableId(manualRaw)
  if (!manual) { show('screen-user-home'); return }
  await api('/api/user/change-table', { method: 'POST', body: JSON.stringify({ userId: S.user.id, newTable: manual }) })
  S.user.tableId = manual
  const ut = q('user-table'); if (ut) ut.textContent = S.user.tableId || '-'
  show('screen-user-home')
}

async function loadUserOrders() {
  const r = await api(`/api/user/orders?userId=${encodeURIComponent(S.user.id)}`)
  const container = q('user-orders')
  if (!container) return
  container.innerHTML = ''
  const listAsc = (r.orders || []).slice().sort((a, b) => {
    const ta = Number(a.createdAt || 0), tb = Number(b.createdAt || 0)
    if (ta !== tb) return ta - tb
    const ia = String(a.id || ''), ib = String(b.id || '')
    return ia.localeCompare(ib)
  })
  for (const o of listAsc) {
    const div = document.createElement('div')
    div.className = 'card'
    const chip = document.createElement('span')
    chip.className = 'chip ' + (o.status === 'pendiente_cobro' ? 'pending' : o.status)
    chip.textContent = o.status.replace('_',' ')
    const invChip = document.createElement('span')
    if (o.isInvitation) { invChip.className = 'chip'; invChip.textContent = 'Invitación' }
    const forEmitter = o.emitterId === S.user.id
    const amountTxt = ` • $${o.total || 0}`
    const otherAlias = forEmitter ? (o.receiverAlias || o.receiverId) : (o.emitterAlias || o.emitterId)
    await ensureCatalogIndex()
    const label = formatOrderProductFull(o.product)
    const timeTxt = o.createdAt ? ` • ${formatTimeShort(o.createdAt)}` : ''
    div.textContent = `${label} x${o.quantity || 1}${amountTxt} • ${forEmitter ? 'Enviado a' : 'Recibido de'} ${otherAlias}${timeTxt}`
    div.append(chip)
    if (o.isInvitation) div.append(invChip)
    container.append(div)
  }
}
async function loadPendingInvites() {
  try {
    if (!S.user || !S.user.id) return
    const r = await api(`/api/user/invites?userId=${encodeURIComponent(S.user.id)}`)
    const list = Array.isArray(r.invites) ? r.invites : []
    for (const inv of list) {
      S.invitesQueue.push({ type: 'dance', id: inv.id, invite: { id: inv.id, from: inv.from, expiresAt: Number(inv.expiresAt || 0) } })
      S.notifications.invites = (S.notifications.invites || 0) + 1
    }
    setBadgeNav('disponibles', S.notifications.invites)
    if (list.length > 0 && !S.inInviteFlow) { showNextInvite() }
  } catch {}
}
function showNextInvite() {
  if (S.inInviteFlow) return
  const next = S.invitesQueue.shift()
  if (!next) return
  S.inInviteFlow = true
  if (next.type === 'dance') {
    S.currentInvite = { id: next.invite.id, from: next.invite.from, expiresAt: Number(next.invite.expiresAt || 0) }
    const mesaTxt = S.currentInvite.from.tableId ? ` • Mesa ${S.currentInvite.from.tableId}` : ''
    const zoneTxt = S.currentInvite.from.zone ? ` • Zona ${S.currentInvite.from.zone}` : ''
    const info = q('invite-received-info'); if (info) info.textContent = `${S.currentInvite.from.alias} te invita${mesaTxt}${zoneTxt}`
    const img = q('invite-from-selfie'); if (img) img.src = S.currentInvite.from.selfie || ''
    startInviteCountdown(S.currentInvite.expiresAt)
    show('screen-invite-received')
  } else if (next.type === 'consumption') {
    S.consumptionReq = next.data
    const msg = S.consumptionReq.note ? ` • Mensaje: ${S.consumptionReq.note}` : ''
    const mesaTxt = S.consumptionReq.from.tableId ? ` • Mesa ${S.consumptionReq.from.tableId}` : ''
    const listTxt = (Array.isArray(S.consumptionReq.items) ? S.consumptionReq.items.map(it => `${it.quantity} x ${it.product}`).join(', ') : S.consumptionReq.product)
    const info = q('invite-received-info'); if (info) info.textContent = `${S.consumptionReq.from.alias} te invita ${listTxt}${mesaTxt}${msg}`
    stopInviteCountdown()
    show('screen-invite-received')
  }
  S.notifications.invites = Math.max(0, (S.notifications.invites || 0) - 1)
  setBadgeNav('disponibles', S.notifications.invites)
}
function openInvitesInbox() {
  const container = q('invites-inbox'); if (!container) return
  container.innerHTML = ''
  const snapshot = []
  if (S.currentInvite) snapshot.push({ type: 'dance', id: S.currentInvite.id, invite: { id: S.currentInvite.id, from: S.currentInvite.from, expiresAt: S.currentInvite.expiresAt } })
  if (S.consumptionReq) snapshot.push({ type: 'consumption', data: S.consumptionReq })
  const list = [...snapshot, ...S.invitesQueue]
  for (const item of list) {
    const div = document.createElement('div')
    div.className = 'card'
    if (item.type === 'dance') {
      const from = item.invite.from || {}
      const ttl = item.invite.expiresAt ? ` • expira ${formatTimeShort(item.invite.expiresAt)}` : ''
      div.textContent = `Baile de ${from.alias || from.id}${ttl}`
      const row = document.createElement('div'); row.className = 'row'
      const bA = document.createElement('button'); bA.textContent = 'Aceptar'; bA.onclick = async () => { await api('/api/invite/respond', { method: 'POST', body: JSON.stringify({ inviteId: item.id, action: 'accept', note: '' }) }); show('screen-user-home') }
      const bP = document.createElement('button'); bP.textContent = 'Pasar'; bP.onclick = async () => { await api('/api/invite/respond', { method: 'POST', body: JSON.stringify({ inviteId: item.id, action: 'pass', note: '' }) }); S.invitesQueue = S.invitesQueue.filter(x => !(x.type === 'dance' && String(x.id || '') === String(item.id || ''))); openInvitesInbox() }
      row.append(bA, bP); div.append(row)
    } else if (item.type === 'consumption') {
      const data = item.data
      const listTxt = (Array.isArray(data.items) ? data.items.map(it => `${it.quantity} x ${it.product}`).join(', ') : `${data.quantity || 1} x ${data.product}`)
      div.textContent = `Consumo de ${data.from.alias}: ${listTxt}`
      const row = document.createElement('div'); row.className = 'row'
      const bA = document.createElement('button'); bA.textContent = 'Aceptar'; bA.onclick = async () => { if (Array.isArray(data.items) && data.items.length) { await api('/api/consumption/respond/bulk', { method: 'POST', body: JSON.stringify({ fromId: data.from.id, toId: S.user.id, items: data.items, action: 'accept', requestId: data.requestId || '' }) }) } else { await api('/api/consumption/respond', { method: 'POST', body: JSON.stringify({ fromId: data.from.id, toId: S.user.id, product: data.product, action: 'accept', requestId: data.requestId || '' }) }) } show('screen-user-home') }
      const bP = document.createElement('button'); bP.textContent = 'Pasar'; bP.onclick = async () => { if (Array.isArray(data.items) && data.items.length) { await api('/api/consumption/respond/bulk', { method: 'POST', body: JSON.stringify({ fromId: data.from.id, toId: S.user.id, items: data.items, action: 'pass', requestId: data.requestId || '' }) }) } else { await api('/api/consumption/respond', { method: 'POST', body: JSON.stringify({ fromId: data.from.id, toId: S.user.id, product: data.product, action: 'pass', requestId: data.requestId || '' }) }) } S.invitesQueue = S.invitesQueue.filter(x => !(x.type === 'consumption' && x.data && x.data.requestId === data.requestId)); openInvitesInbox() }
      row.append(bA, bP); div.append(row)
    }
    container.append(div)
  }
  show('screen-invites-inbox')
}
async function passAllDanceInvites() {
  const ids = []
  if (S.currentInvite && S.currentInvite.id) ids.push(S.currentInvite.id)
  for (const it of S.invitesQueue) if (it.type === 'dance' && it.id) ids.push(it.id)
  if (ids.length === 0) { showModal('Invitaciones', 'No hay invitaciones de baile para pasar', 'info'); return }
  const ok = await confirmAction(`Vas a pasar ${ids.length} invitación${ids.length > 1 ? 'es' : ''} de baile. ¿Confirmas?`)
  if (!ok) return
  for (const id of ids) { try { await api('/api/invite/respond', { method: 'POST', body: JSON.stringify({ inviteId: id, action: 'pass', note: '' }) }) } catch {} }
  S.invitesQueue = S.invitesQueue.filter(x => x.type !== 'dance')
  stopInviteCountdown()
  S.currentInvite = null
  S.inInviteFlow = false
  showNextInvite()
  openInvitesInbox()
  showSuccess('Invitaciones de baile pasadas')
}
async function passAllConsumptionInvites() {
  const items = []
  if (S.consumptionReq) items.push(S.consumptionReq)
  for (const it of S.invitesQueue) if (it.type === 'consumption' && it.data) items.push(it.data)
  if (items.length === 0) { showModal('Invitaciones', 'No hay invitaciones de consumo para pasar', 'info'); return }
  const ok = await confirmAction(`Vas a pasar ${items.length} invitación${items.length > 1 ? 'es' : ''} de consumo. ¿Confirmas?`)
  if (!ok) return
  for (const data of items) {
    try {
      if (Array.isArray(data.items) && data.items.length) {
        await api('/api/consumption/respond/bulk', { method: 'POST', body: JSON.stringify({ fromId: data.from.id, toId: S.user.id, items: data.items, action: 'pass', requestId: data.requestId || '' }) })
      } else {
        await api('/api/consumption/respond', { method: 'POST', body: JSON.stringify({ fromId: data.from.id, toId: S.user.id, product: data.product, action: 'pass', requestId: data.requestId || '' }) })
      }
    } catch {}
  }
  S.invitesQueue = S.invitesQueue.filter(x => x.type !== 'consumption')
  S.consumptionReq = null
  S.inInviteFlow = false
  showNextInvite()
  openInvitesInbox()
  showSuccess('Invitaciones de consumo pasadas')
}
function startInviteCountdown(expiresAt) {
  try { if (S.timers && S.timers.inviteCountdown) { clearInterval(S.timers.inviteCountdown); S.timers.inviteCountdown = 0 } } catch {}
  const target = Number(expiresAt || 0)
  if (!target) { const txt = q('invite-ring-txt'); if (txt) txt.textContent = ''; const el = q('invite-ring'); if (el) el.style.setProperty('--deg','0deg'); return }
  const el = q('invite-ring')
  const txt = q('invite-ring-txt')
  if (!el || !txt) return
  S.inviteTTL = Math.max(1, Math.ceil((target - Date.now()) / 1000))
  const tick = () => {
    const remMs = target - Date.now()
    const remSec = Math.max(0, Math.ceil(remMs / 1000))
    if (remSec <= 0) {
      txt.textContent = '00:00'
      el.style.setProperty('--deg', '360deg')
      try { clearInterval(S.timers.inviteCountdown) } catch {}
      S.timers.inviteCountdown = 0
      return
    }
    const mm = String(Math.floor(remSec / 60)).padStart(2, '0')
    const ss = String(remSec % 60).padStart(2, '0')
    txt.textContent = `${mm}:${ss}`
    const pct = (remSec / Math.max(1, S.inviteTTL))
    const deg = Math.floor(pct * 360)
    el.style.setProperty('--deg', `${deg}deg`)
  }
  tick()
  S.timers.inviteCountdown = setInterval(tick, 1000)
}
function stopInviteCountdown() {
  try { if (S.timers && S.timers.inviteCountdown) { clearInterval(S.timers.inviteCountdown); S.timers.inviteCountdown = 0 } } catch {}
  const el = q('invite-ring'); if (el) el.style.setProperty('--deg', '0deg')
  const txt = q('invite-ring-txt'); if (txt) txt.textContent = ''
}
async function loadUserInvitesHistory() {
  const r = await api(`/api/user/invites/history?userId=${encodeURIComponent(S.user.id)}`)
  const container = q('user-invites')
  if (!container) return
  container.innerHTML = ''
  const listAsc = (r.invites || []).slice().sort((a, b) => {
    const ta = Number(a.createdAt || 0), tb = Number(b.createdAt || 0)
    if (ta !== tb) return ta - tb
    const ia = String(a.id || ''), ib = String(b.id || '')
    return ia.localeCompare(ib)
  })
  for (const inv of listAsc) {
    const div = document.createElement('div')
    div.className = 'card'
    const chip = document.createElement('span')
    chip.className = 'chip ' + (inv.status === 'pendiente' ? 'pending' : (inv.status || ''))
    chip.textContent = inv.status || ''
    const meSender = inv.from && inv.from.id === (S.user ? S.user.id : '')
    const otherAlias = meSender ? (inv.to.alias || inv.to.id) : (inv.from.alias || inv.from.id)
    const timeTxt = inv.createdAt ? ` • ${formatTimeShort(inv.createdAt)}` : ''
    const dirTxt = meSender ? 'Enviada a' : 'Recibida de'
    const msgTxt = inv.msg === 'invitoCancion' ? '¿Te invito una canción?' : '¿Bailamos?'
    div.textContent = `${dirTxt} ${otherAlias} • ${msgTxt}${timeTxt}`
    div.append(chip)
    container.append(div)
  }
}
async function finishDance() {
  const st = S.user?.danceState || 'idle'
  if (st !== 'dancing') return
  const ok = await confirmAction('¿Terminaste de bailar? Esto cerrará tu estado de baile.')
  if (!ok) return
  await api('/api/dance/finish', { method: 'POST', body: JSON.stringify({ userId: S.user.id }) })
  showError('Marcado: baile terminado')
  setTimeout(() => showError(''), 1200)
  renderUserHeader()
}

async function blockFromInvite() {
  if (!S.currentInvite || !S.currentInvite.from) return
  await api('/api/block', { method: 'POST', body: JSON.stringify({ userId: S.user.id, targetId: S.currentInvite.from.id }) })
  show('screen-user-home')
}

async function reportFromInvite() {
  if (!S.currentInvite || !S.currentInvite.from) return
  await api('/api/report', { method: 'POST', body: JSON.stringify({ userId: S.user.id, targetId: S.currentInvite.from.id, category: 'invite', note: '' }) })
  show('screen-user-home')
}
async function startScanQR() {
  try {
    showError('')
    const video = q('qr-video')
    if (!('BarcodeDetector' in window)) { showError('Tu navegador no soporta escaneo QR'); return }
    const detector = new BarcodeDetector({ formats: ['qr_code'] })
    const stream = await navigator.mediaDevices.getUserMedia({ video: { facingMode: 'environment' } })
    video.srcObject = stream
    await video.play()
    video.style.display = 'block'
    const tick = async () => {
      try {
        const codes = await detector.detect(video)
        if (codes && codes.length) {
          const data = codes[0].rawValue || codes[0].data || ''
          try {
            const u = new URL(data)
            const sid = u.searchParams.get('sessionId') || u.searchParams.get('s')
            if (sid) {
              const tracks = stream.getTracks(); tracks.forEach(t => t.stop())
              location.href = data
              return
            }
          } catch {
            if (data.startsWith('sess_')) {
              const base = location.origin
              const url = `${base}/?venueId=${encodeURIComponent(S.venueId || 'default')}&sessionId=${encodeURIComponent(data)}&aj=1`
              const tracks = stream.getTracks(); tracks.forEach(t => t.stop())
              location.href = url
              return
            }
          }
        }
      } catch {}
      requestAnimationFrame(tick)
    }
    tick()
  } catch (e) {
    showError(String(e.message))
  }
}

function init() {
  bind()
  try {
    const u = new URL(location.href)
    const vid = u.searchParams.get('venueId') || ''
    if (vid) S.venueId = vid
    const sid = u.searchParams.get('sessionId') || u.searchParams.get('s')
    if (sid && q('join-code')) q('join-code').value = sid
    const aj = u.searchParams.get('aj')
    const staffParam = u.searchParams.get('staff')
    restoreLocalUser().then(ok => {
      if (ok) return
      if (sid && aj === '1') { setTimeout(() => join('user', sid), 50) }
      else if (staffParam === '1') { show('screen-staff-welcome') }
      else { show('screen-welcome') }
    })
  } catch {}
  if ('serviceWorker' in navigator) navigator.serviceWorker.register('/sw.js')
  try {
    setTheme('dark')
    try { localStorage.setItem('discos_theme', 'dark') } catch {}
  } catch { setTheme('dark') }
}

init()
function getLocalUsers() {
  try {
    const rawMap = localStorage.getItem('discos_users')
    if (rawMap) {
      const obj = JSON.parse(rawMap || '{}')
      return typeof obj === 'object' && obj ? obj : {}
    }
    const raw = localStorage.getItem('discos_user')
    if (raw) {
      const d = JSON.parse(raw || '{}')
      if (d && d.venueId) {
        const m = {}
        m[d.venueId] = { sessionId: d.sessionId || '', role: d.role || '', userId: d.userId || '' }
        return m
      }
    }
  } catch {}
  return {}
}
function setLocalUsers(map) {
  try { localStorage.setItem('discos_users', JSON.stringify(map || {})) } catch {}
}
function saveLocalUser() {
  const v = S.venueId || 'default'
  const m = getLocalUsers()
  m[v] = { sessionId: S.sessionId || '', role: S.role || (S.user ? S.user.role : ''), userId: S.user ? S.user.id : '' }
  setLocalUsers(m)
  try { localStorage.setItem('discos_last_venue', v) } catch {}
}
function removeLocalUser(venueId) {
  const v = venueId || (S.venueId || 'default')
  const m = getLocalUsers()
  if (m[v]) { delete m[v] }
  setLocalUsers(m)
}
async function restoreLocalUser() {
  try {
    let sidParam = ''
    let ajParam = ''
    let venueParam = ''
    let staffParam = ''
    try {
      const u = new URL(location.href)
      sidParam = u.searchParams.get('sessionId') || u.searchParams.get('s') || ''
      ajParam = u.searchParams.get('aj') || ''
      venueParam = u.searchParams.get('venueId') || ''
      staffParam = u.searchParams.get('staff') || ''
    } catch {}
    const m = getLocalUsers()
    const lastVenue = (() => { try { return localStorage.getItem('discos_last_venue') || '' } catch { return '' } })()
    const key = venueParam || lastVenue
    const d = (key && m[key]) ? { sessionId: m[key].sessionId, userId: m[key].userId, role: m[key].role, venueId: key } : null
    if (!d || !d.sessionId || !d.userId || !d.role) return false
    if (sidParam && ajParam === '1' && sidParam !== d.sessionId) { return false }
    S.venueId = d.venueId || (S.venueId || 'default')
    if (d.role === 'staff') {
      S.user = { id: d.userId, role: 'staff', sessionId: d.sessionId }
      S.sessionId = d.sessionId
      S.role = 'staff'
    } else {
      const r = await api(`/api/user/get?userId=${encodeURIComponent(d.userId)}`).catch(() => null)
      if (!r || !r.user) return false
      S.user = r.user
      S.sessionId = r.user.sessionId
      S.role = r.user.role
    }
    if (S.role === 'staff') {
      const okActive = await ensureSessionActiveOffer()
      if (!okActive) { show('screen-staff-welcome'); return true }
      show('screen-staff')
      showStaffTab('session')
      await loadSessionInfo()
      startStaffEvents()
      loadOrders(); loadUsers(); loadReports(); loadAnalytics(); loadStaffPromos()
      return true
    } else if (S.role === 'user') {
      startEvents()
      show('screen-user-home')
      const swAvail = q('switch-available')
      if (swAvail) swAvail.checked = !!(S.user && S.user.available)
      const rc = q('restore-chip')
      if (rc) {
        rc.textContent = 'Sesión restaurada'
        rc.style.display = 'inline-block'
        setTimeout(() => { rc.style.display = 'none' }, 1500)
      }
      const ua = q('user-alias'), us = q('user-selfie'), ut = q('user-table')
      if (ua) ua.textContent = S.user.alias || S.user.id
      if (ut) ut.textContent = S.user.tableId || '-'
      if (us) us.src = S.user.selfie || ''
      renderUserHeader()
      return true
    }
    return false
  } catch { return false }
}
async function loadAnalytics() {
  const r = await api(`/api/staff/analytics?sessionId=${encodeURIComponent(S.sessionId)}`)
  const users = q('an-users'), mesas = q('an-mesas'), inv = q('an-invites'), acc = q('an-accepted'), ratio = q('an-ratio')
  if (users) users.textContent = `Usuarios: ${r.usersCount}`
  if (mesas) mesas.textContent = `Mesas activas: ${r.mesasActivas}`
  if (inv) inv.textContent = `Invitaciones: ${r.invitesSent}`
  if (acc) acc.textContent = `Aceptadas: ${r.invitesAccepted}`
  const pct = r.invitesSent ? Math.round((r.invitesAccepted / r.invitesSent) * 100) : 0
  if (ratio) ratio.textContent = `Ratio: ${pct}%`
  const orders = q('an-orders')
  if (orders) {
    orders.innerHTML = ''
    for (const key of Object.keys(r.orders || {})) {
      const chip = document.createElement('span')
      chip.className = 'chip ' + (key === 'pendiente_cobro' ? 'pending' : key)
      chip.textContent = `${key}: ${r.orders[key]}`
      chip.onclick = () => { showStaffTab('orders'); loadOrders(key) }
      orders.append(chip)
    }
  }
  const bOrders = q('badge-tab-orders')
  if (bOrders) { const v = r.orders?.pendiente_cobro || 0; bOrders.classList.toggle('show', v > 0); bOrders.textContent = v > 9 ? '9+' : String(v) }
  const bMesas = q('badge-tab-mesas')
  if (bMesas) { const v = r.mesasActivas || 0; bMesas.classList.toggle('show', v > 0); bMesas.textContent = v > 9 ? '9+' : String(v) }
  const bUsers = q('badge-tab-usuarios')
  if (bUsers) { const v = r.usersCount || 0; bUsers.classList.toggle('show', v > 0); bUsers.textContent = v > 9 ? '9+' : String(v) }
  const top = q('an-top')
  if (top) {
    top.innerHTML = ''
    for (const name of Object.keys(r.topItems || {})) {
      const chip = document.createElement('span')
      chip.className = 'chip'
      chip.textContent = `${name}: ${r.topItems[name]}`
      top.append(chip)
    }
  }
}
function addStaffPromo() {
  const inp = q('staff-promos-input')
  const list = q('staff-promos-list')
  if (!inp || !list) return
  const title = inp.value.trim()
  if (!title) return
  const div = document.createElement('div')
  div.className = 'card'
  div.textContent = title
  list.append(div)
  inp.value = ''
}
async function saveStaffPromos() {
  const list = q('staff-promos-list')
  if (!list) return
  const promos = []
  for (const el of list.children) promos.push({ title: el.textContent })
  await api('/api/staff/promos', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId, promos }) })
  showError('Promos guardadas')
  setTimeout(() => showError(''), 1000)
}
async function openConsumption() {
  await loadCatalog()
  const target = q('consumption-target'), sendBtn = q('btn-consumption-send')
  if (target) target.style.display = ''
  if (sendBtn) sendBtn.style.display = ''
  const title = q('consumption-title'); if (title) title.textContent = 'Invitar consumo'
  const targetLabel = q('consumption-target-label')
  if (targetLabel) targetLabel.style.display = ''
  const noteRow = q('consumption-note-row')
  if (noteRow) noteRow.style.display = ''
  const btnSelf = q('btn-order-self'), btnTable = q('btn-order-table')
  if (btnSelf) btnSelf.style.display = 'none'
  if (btnTable) btnTable.style.display = 'none'
  const cartSection = q('cart-section'); if (cartSection) { cartSection.style.display = ''; renderCart() }
  const addCart = q('btn-add-to-cart'); if (addCart) addCart.style.display = ''
  const cats = q('catalog-cats'), grid = q('catalog-list'), back = q('btn-catalog-back')
  if (cats) cats.style.display = ''
  if (grid) grid.style.display = 'none'
  if (back) back.style.display = 'none'
  showCatalogTop()
  show('screen-consumption')
}
async function openMenu() {
  await loadCatalog()
  const target = q('consumption-target'), sendBtn = q('btn-consumption-send')
  if (target) target.style.display = 'none'
  if (sendBtn) sendBtn.style.display = 'none'
  const title = q('consumption-title'); if (title) title.textContent = 'Carta'
  const targetLabel = q('consumption-target-label')
  if (targetLabel) targetLabel.style.display = 'none'
  const noteRow = q('consumption-note-row')
  if (noteRow) noteRow.style.display = 'none'
  const btnSelf = q('btn-order-self'), btnTable = q('btn-order-table')
  if (btnSelf) btnSelf.style.display = ''
  if (btnTable) btnTable.style.display = ''
  const cartSection = q('cart-section'); if (cartSection) { cartSection.style.display = ''; renderCart() }
  const addCart = q('btn-add-to-cart'); if (addCart) addCart.style.display = ''
  const cats = q('catalog-cats'), grid = q('catalog-list'), back = q('btn-catalog-back')
  if (cats) cats.style.display = ''
  if (grid) grid.style.display = 'none'
  if (back) back.style.display = 'none'
  showCatalogTop()
  show('screen-consumption')
}
async function loadCatalog() {
  try {
    const r = await api(`/api/catalog${S.sessionId ? ('?sessionId=' + encodeURIComponent(S.sessionId)) : ''}`)
    const catsEl = q('catalog-cats')
    const itemsEl = q('catalog-list')
    if (!catsEl || !itemsEl) return
    catsEl.innerHTML = ''
    itemsEl.innerHTML = ''
    const groups = {}
    for (const it of r.items || []) {
      const cat = (it.category || 'otros').toLowerCase()
      if (!groups[cat]) groups[cat] = []
      groups[cat].push(it)
    }
    const order = ['cervezas','botellas','cocteles','sodas','otros']
    const labels = {
      cervezas: 'Cervezas',
      botellas: 'Botellas',
      cocteles: 'Cocteles',
      sodas: 'Sodas y sin alcohol',
      otros: 'Otros'
    }
    S.catalogGroups = groups
    renderCatalogCats(order, labels)
  } catch {}
}
function renderCatalogCats(order, labels) {
  const catsEl = q('catalog-cats'), itemsEl = q('catalog-list'), back = q('btn-catalog-back')
  if (!catsEl || !itemsEl) return
  catsEl.innerHTML = ''
  itemsEl.innerHTML = ''
  if (back) { back.style.display = 'none'; back.textContent = 'Volver a categorías' }
  if (itemsEl) itemsEl.style.display = 'none'
  if (catsEl) catsEl.style.display = ''
  for (const cat of order) {
    const items = S.catalogGroups[cat]
    if (!items || !items.length) continue
    const div = document.createElement('div')
    div.className = 'card'
    const name = document.createElement('div')
    name.textContent = labels[cat] || cat
    const count = document.createElement('span')
    count.className = 'chip'
    count.textContent = `${items.length}`
    div.onclick = () => renderCatalogCategory(cat, labels)
    div.append(name, count)
    catsEl.append(div)
  }
}
function renderCatalogCategory(cat, labels) {
  S.catalogCat = cat
  const items = S.catalogGroups[cat] || []
  const subgroups = {}
  for (const it of items) {
    const sub = String(it.subcategory || '').trim()
    if (!sub) continue
    if (!subgroups[sub]) subgroups[sub] = []
    subgroups[sub].push(it)
  }
  const names = Object.keys(subgroups)
  if (names.length) {
    renderCatalogSubcats(cat, labels, names, subgroups)
  } else {
    renderCatalogItems(cat, labels, items)
  }
}
function renderCatalogSubcats(cat, labels, names, subgroups) {
  const catsEl = q('catalog-cats'), itemsEl = q('catalog-list'), back = q('btn-catalog-back')
  if (!catsEl || !itemsEl) return
  S.catalogSubcat = ''
  catsEl.style.display = 'none'
  itemsEl.style.display = ''
  itemsEl.innerHTML = ''
  if (back) { back.style.display = ''; back.textContent = 'Volver a categorías'; back.onclick = () => { S.catalogCat=''; S.catalogSubcat=''; renderCatalogCats(['cervezas','botellas','cocteles','sodas','otros'], { cervezas:'Cervezas', botellas:'Botellas', cocteles:'Cocteles', sodas:'Sodas y sin alcohol', otros:'Otros' }) } }
  const title = document.createElement('h3')
  title.textContent = labels[cat] || cat
  itemsEl.append(title)
  for (const sub of names) {
    const div = document.createElement('div')
    div.className = 'card'
    const name = document.createElement('div')
    name.textContent = sub
    const count = document.createElement('span')
    count.className = 'chip'
    count.textContent = `${(subgroups[sub]||[]).length}`
    div.onclick = () => { S.catalogSubcat = sub; renderCatalogItems(cat, labels, subgroups[sub] || []) }
    div.append(name, count)
    itemsEl.append(div)
  }
}
function renderCatalogItems(cat, labels, items) {
  const catsEl = q('catalog-cats'), itemsEl = q('catalog-list'), back = q('btn-catalog-back')
  if (!catsEl || !itemsEl) return
  catsEl.style.display = 'none'
  itemsEl.style.display = ''
  itemsEl.innerHTML = ''
  if (back) {
    back.style.display = ''
    back.textContent = S.catalogSubcat ? 'Volver a subcategorías' : 'Volver a categorías'
    back.onclick = () => {
      if (S.catalogSubcat) { S.catalogSubcat=''; renderCatalogCategory(cat, labels) }
      else { S.catalogCat=''; renderCatalogCats(['cervezas','botellas','cocteles','sodas','otros'], { cervezas:'Cervezas', botellas:'Botellas', cocteles:'Cocteles', sodas:'Sodas y sin alcohol', otros:'Otros' }) }
    }
  }
  const title = document.createElement('h3')
  title.textContent = S.catalogSubcat ? `${labels[cat] || cat} • ${S.catalogSubcat}` : (labels[cat] || cat)
  itemsEl.append(title)
  for (const it of items) {
    const div = document.createElement('div')
    div.className = 'card'
    const name = document.createElement('div')
    name.textContent = it.name
    const price = document.createElement('span')
    price.className = 'chip'
    price.textContent = formatPriceShort(it.price)
    div.onclick = () => { const p = q('product'); if (p) p.value = it.name }
    div.append(name, price)
    itemsEl.append(div)
  }
}
async function renderCart() {
  const list = q('cart-list')
  if (!list) return
  list.innerHTML = ''
  await ensureCatalogIndex().catch(() => {})
  let total = 0
  for (let i = 0; i < S.cart.length; i++) {
    const it = S.cart[i]
    const row = document.createElement('div')
    row.className = 'row'
    const label = document.createElement('div')
    const key = String(it.product || '').toLowerCase()
    const price = S.catalogIndex && S.catalogIndex[key] ? Number(S.catalogIndex[key].price || 0) : 0
    const subtotal = price * Math.max(1, Number(it.quantity || 1))
    total += subtotal
    label.textContent = `${it.quantity} x ${it.product} • ${formatPriceShort(subtotal)}`
    const del = document.createElement('button')
    del.textContent = 'Eliminar'
    del.onclick = () => { try { S.cart.splice(i, 1); renderCart() } catch {} }
    row.append(label, del)
    list.append(row)
  }
  const totalRow = document.createElement('div')
  totalRow.className = 'row'
  const ttl = document.createElement('div')
  ttl.textContent = `Total: ${formatPriceShort(total)}`
  totalRow.append(ttl)
  list.append(totalRow)
}
function addToCart() {
  const product = q('product') ? q('product').value : ''
  const qty = Math.max(1, Number(q('quantity') ? q('quantity').value : 1))
  if (!product) { showError('Selecciona un producto'); return }
  S.cart.push({ product, quantity: qty })
  const inp = q('product'); if (inp) inp.value = ''
  const qn = q('quantity'); if (qn) qn.value = '1'
  renderCart()
}
function applyCatalogSearch() {
  const inp = q('catalog-search')
  const catsEl = q('catalog-cats'), itemsEl = q('catalog-list'), back = q('btn-catalog-back')
  if (!inp || !catsEl || !itemsEl) return
  const qv = String(inp.value || '').trim().toLowerCase()
  if (!qv) { S.catalogCat=''; S.catalogSubcat=''; renderCatalogCats(['cervezas','botellas','cocteles','sodas','otros'], { cervezas:'Cervezas', botellas:'Botellas', cocteles:'Cocteles', sodas:'Sodas y sin alcohol', otros:'Otros' }); return }
  const all = []
  for (const arr of Object.values(S.catalogGroups || {})) for (const it of arr || []) all.push(it)
  const matches = all.filter(it => String(it.name || '').toLowerCase().includes(qv))
  S.catalogCat = 'search'
  S.catalogSubcat = ''
  catsEl.style.display = 'none'
  renderCatalogItems('otros', { otros: 'Resultados' }, matches)
}
async function showCatalogTop() {
  try {
    const r = await api(`/api/staff/analytics?sessionId=${encodeURIComponent(S.sessionId)}`)
    const top = q('catalog-top')
    if (!top) return
    top.innerHTML = ''
    const items = r.topItems || {}
    const names = Object.keys(items)
    for (const name of names.slice(0, 8)) {
      const chip = document.createElement('span')
      chip.className = 'chip'
      chip.textContent = `${name} • ${items[name]}`
      chip.onclick = () => { const p = q('product'); if (p) p.value = name }
      top.append(chip)
    }
  } catch {}
}
async function loadStaffCatalogEditor() {
  try {
    const r = await api(`/api/catalog${S.sessionId ? ('?sessionId=' + encodeURIComponent(S.sessionId)) : ''}`)
    const srcEl = q('staff-catalog-source')
    if (srcEl) {
      const map = { session: 'Fuente: Carta del venue (sesión)', global: 'Fuente: Carta global', file: 'Fuente: Carta global (archivo)' }
      srcEl.textContent = map[r.source] || 'Fuente: Carta'
    }
    const container = q('staff-catalog-list')
    if (!container) return
    container.innerHTML = ''
    for (const it of r.items || []) {
      const row = document.createElement('div')
      row.className = 'row'
      const name = document.createElement('input')
      name.type = 'text'
      name.placeholder = 'Nombre'
      name.value = it.name
      const price = document.createElement('input')
      price.type = 'number'
      price.min = '0'
      price.value = Number(it.price || 0)
      const category = document.createElement('select')
      for (const opt of ['cervezas','botellas','cocteles','sodas','otros']) {
        const o = document.createElement('option')
        o.value = opt
        o.textContent = opt.charAt(0).toUpperCase() + opt.slice(1)
        category.append(o)
      }
      category.value = (it.category || 'otros').toLowerCase()
      const subInput = document.createElement('input')
      subInput.type = 'text'
      subInput.placeholder = 'Subcategoría'
      subInput.value = String(it.subcategory || '')
      name.oninput = scheduleCatalogSave
      price.oninput = scheduleCatalogSave
      category.oninput = scheduleCatalogSave
      subInput.oninput = scheduleCatalogSave
      const del = document.createElement('button'); del.textContent = 'Eliminar'; del.onclick = () => { try { row.remove(); scheduleCatalogSave() } catch {} }
      row.append(name, price, category, subInput, del)
      container.append(row)
    }
  } catch {}
}
async function saveStaffCatalog() {
  try {
    const list = q('staff-catalog-list')
    if (!list) return
    const items = []
    for (const row of list.children) {
      const nameInput = row.querySelector('input[type="text"]')
      const priceInput = row.querySelector('input[type="number"]')
      const catSelect = row.querySelector('select')
      const subInput = (() => { const arr = row.querySelectorAll('input[type=\"text\"]'); return arr.length > 1 ? arr[1] : null })()
      if (!nameInput || !priceInput || !catSelect) continue
      const name = nameInput.value.trim()
      const price = Number(priceInput.value || 0)
      const category = (catSelect.value || 'otros').toLowerCase()
      const subcategory = subInput ? String(subInput.value || '').trim() : ''
      items.push({ name, price, category, subcategory })
    }
    await api('/api/staff/catalog', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId, items }) })
    showError('Carta guardada')
    setTimeout(() => showError(''), 1000)
    await loadStaffCatalogEditor()
  } catch (e) { showError(String(e.message)) }
}
async function useGlobalCatalog() {
  try {
    await api('/api/staff/catalog', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId, items: [] }) })
    await loadStaffCatalogEditor()
  } catch (e) { showError(String(e.message)) }
}
async function copyGlobalToSession() {
  try {
    const r = await api('/api/catalog')
    const items = r.items || []
    await api('/api/staff/catalog', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId, items }) })
    await loadStaffCatalogEditor()
  } catch (e) { showError(String(e.message)) }
}
async function loadStaffPromos() {
  try {
    const r = await api(`/api/promos?sessionId=${encodeURIComponent(S.sessionId)}`)
    const container = q('staff-promos-list')
    if (!container) return
    container.innerHTML = ''
    for (const p of r.promos || []) {
      const div = document.createElement('div')
      div.className = 'card'
      div.textContent = p.title || ''
      container.append(div)
    }
  } catch {}
}
async function viewStaffTableHistory() {
  const raw = q('staff-table-id')?.value.trim()
  const t = normalizeTableId(raw)
  if (!t) return
  const r = await api(`/api/staff/table/orders?sessionId=${encodeURIComponent(S.sessionId)}&tableId=${encodeURIComponent(t)}`)
  const container = q('staff-table-orders')
  if (!container) return
  container.innerHTML = ''
  const listAsc = (r.orders || []).slice().sort((a, b) => {
    const ta = Number(a.createdAt || 0), tb = Number(b.createdAt || 0)
    if (ta !== tb) return ta - tb
    const ia = String(a.id || ''), ib = String(b.id || '')
    return ia.localeCompare(ib)
  })
  for (const o of listAsc) {
    const div = document.createElement('div')
    div.className = 'card'
    const info = document.createElement('div')
    await ensureCatalogIndex()
    const label = formatOrderProductFull(o.product)
    const timeTxt = o.createdAt ? ` • ${formatTimeShort(o.createdAt)}` : ''
    info.textContent = `${label} x${o.quantity || 1} • $${o.total || 0} • ${o.emitterAlias || o.emitterId}→${o.receiverAlias || o.receiverId}${timeTxt}`
    const chip = document.createElement('span')
    chip.className = 'chip ' + (o.status === 'pendiente_cobro' ? 'pending' : o.status)
    chip.textContent = o.status.replace('_', ' ')
    info.append(chip)
    div.append(info)
    container.append(div)
  }
}
async function viewStaffTableHistory2() {
  const raw = q('staff2-table-id')?.value.trim()
  const t = normalizeTableId(raw)
  if (!t) return
  const r = await api(`/api/staff/table/orders?sessionId=${encodeURIComponent(S.sessionId)}&tableId=${encodeURIComponent(t)}`)
  const container = q('staff2-table-orders')
  if (!container) return
  container.innerHTML = ''
  const listAsc = (r.orders || []).slice().sort((a, b) => {
    const ta = Number(a.createdAt || 0), tb = Number(b.createdAt || 0)
    if (ta !== tb) return ta - tb
    const ia = String(a.id || ''), ib = String(b.id || '')
    return ia.localeCompare(ib)
  })
  for (const o of listAsc) {
    const div = document.createElement('div')
    div.className = 'card'
    const info = document.createElement('div')
    await ensureCatalogIndex()
    const label = formatOrderProductFull(o.product)
    const timeTxt = o.createdAt ? ` • ${formatTimeShort(o.createdAt)}` : ''
    info.textContent = `${label} x${o.quantity || 1} • $${o.total || 0} • ${o.emitterAlias || o.emitterId}→${o.receiverAlias || o.receiverId}${timeTxt}`
    const chip = document.createElement('span')
    chip.className = 'chip ' + (o.status === 'pendiente_cobro' ? 'pending' : o.status)
    chip.textContent = o.status.replace('_', ' ')
    info.append(chip)
    div.append(info)
    container.append(div)
  }
}
async function closeStaffTable() {
  const raw = q('staff-table-id')?.value.trim()
  const t = normalizeTableId(raw)
  if (!t) return
  await api('/api/staff/table/close', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId, tableId: t, closed: true }) })
  showError('Mesa cerrada (historial oculto)')
  setTimeout(() => showError(''), 1000)
  viewStaffTableHistory()
}
async function closeStaffTable2() {
  const raw = q('staff2-table-id')?.value.trim()
  const t = normalizeTableId(raw)
  if (!t) return
  await api('/api/staff/table/close', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId, tableId: t, closed: true }) })
  showError('Mesa cerrada (historial oculto)')
  setTimeout(() => showError(''), 1000)
  viewStaffTableHistory2()
}
