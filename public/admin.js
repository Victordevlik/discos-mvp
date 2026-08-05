function escapeHtml(str) {
  return String(str == null ? '' : str).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]))
}
async function api(path, { method = 'GET', headers = {}, body } = {}) {
  const res = await fetch(path, {
    method,
    headers: Object.assign({ 'Content-Type': 'application/json' }, headers),
    body
  })
  const text = await res.text()
  let j = null
  try { j = JSON.parse(text) } catch { j = { raw: text } }
  if (!res.ok) throw new Error(j && j.error ? j.error : ('HTTP ' + res.status))
  return j
}
function showError(msg) {
  if (!msg) return
  const m = document.getElementById('modal')
  const t = document.getElementById('modal-text')
  const tt = document.getElementById('modal-title')
  if (!m || !t) return
  m.classList.remove('type-error','type-info','type-success')
  m.classList.add('type-error')
  if (tt) tt.textContent = 'Error'
  t.textContent = msg || ''
  m.classList.add('show')
}
let VENUES_CACHE = []
function renderSummary(list) {
  const total = list.length
  const active = list.filter(v => v.active).length
  const credits = list.reduce((a, v) => a + Number(v.credits || 0), 0)
  const st = document.getElementById('sum-total'); if (st) st.textContent = String(total)
  const sa = document.getElementById('sum-active'); if (sa) sa.textContent = String(active)
  const sc = document.getElementById('sum-credits'); if (sc) sc.textContent = String(credits)
}
function renderVenues(list) {
  const container = document.getElementById('venues-list')
  container.innerHTML = ''
  if (!list.length) { container.textContent = 'No hay locales'; return }
  for (const v of list) {
    const card = document.createElement('div')
    card.className = 'venue-card'
    const head = document.createElement('div')
    head.className = 'venue-head'
    const title = document.createElement('div')
    title.textContent = `${v.name} (${v.venueId})`
    const badge = document.createElement('span')
    badge.className = 'badge ' + (v.active ? 'active' : 'inactive')
    badge.textContent = v.active ? 'Activo' : 'Inactivo'
    head.appendChild(title)
    head.appendChild(badge)
    const info = document.createElement('div')
    const url = `${location.origin}/?venueId=${encodeURIComponent(v.venueId)}`
    const d = v.discounts || { nocturno: 0, premium: 0 }
    info.textContent = `Créditos: ${v.credits} • PIN: ${v.pin ? v.pin : '-'} • Email: ${v.email ? v.email : '-'} • Desc. Nocturno: ${d.nocturno || 0}% • Desc. Premium: ${d.premium || 0}%`
    const link = document.createElement('a')
    link.href = url; link.target = '_blank'; link.rel = 'noopener'; link.textContent = 'Abrir'
    const inputs = document.createElement('div')
    inputs.className = 'row compact'
    const nameInput = document.createElement('input')
    nameInput.type = 'text'; nameInput.placeholder = 'Nombre del venue'; nameInput.value = v.name || ''
    const nameBtn = document.createElement('button')
    nameBtn.className = 'success'
    nameBtn.textContent = 'Guardar nombre'
    nameBtn.onclick = async () => {
      try {
        const secret = document.getElementById('admin-secret').value.trim()
        const name = nameInput.value.trim()
        if (!secret) { showError('Ingresa clave admin'); return }
        if (!name) { showError('Ingresa un nombre'); return }
        await api('/api/admin/venues/name', { method: 'POST', headers: { 'X-Admin-Secret': secret }, body: JSON.stringify({ venueId: v.venueId, name }) })
        await loadVenues()
      } catch (e) { showError(String(e.message)) }
    }
    const pinInput = document.createElement('input')
    pinInput.type = 'password'; pinInput.placeholder = 'PIN (4 dígitos)'
    const pinBtn = document.createElement('button')
    pinBtn.className = 'success'
    pinBtn.textContent = 'Guardar PIN'
    pinBtn.onclick = async () => {
      try {
        const secret = document.getElementById('admin-secret').value.trim()
        const pin = pinInput.value.trim()
        if (!secret) { showError('Ingresa clave admin'); return }
        if (!pin) { showError('Ingresa un PIN'); return }
        await api('/api/admin/venues/pin', { method: 'POST', headers: { 'X-Admin-Secret': secret }, body: JSON.stringify({ venueId: v.venueId, pin }) })
        await loadVenues()
      } catch (e) { showError(String(e.message)) }
    }
    const emailInput = document.createElement('input')
    emailInput.type = 'email'; emailInput.placeholder = 'Email del venue'
    const emailBtn = document.createElement('button')
    emailBtn.className = 'success'
    emailBtn.textContent = 'Guardar email'
    emailBtn.onclick = async () => {
      try {
        const secret = document.getElementById('admin-secret').value.trim()
        const email = emailInput.value.trim()
        if (!secret) { showError('Ingresa clave admin'); return }
        if (!email) { showError('Ingresa un email'); return }
        await api('/api/admin/venues/email', { method: 'POST', headers: { 'X-Admin-Secret': secret }, body: JSON.stringify({ venueId: v.venueId, email }) })
        await loadVenues()
      } catch (e) { showError(String(e.message)) }
    }
    inputs.appendChild(pinInput)
    inputs.appendChild(pinBtn)
    inputs.appendChild(emailInput)
    inputs.appendChild(emailBtn)
    inputs.appendChild(nameInput)
    inputs.appendChild(nameBtn)
    // Descuentos por tier
    const discRow = document.createElement('div')
    discRow.className = 'row compact'
    discRow.style.marginTop = '8px'
    const discLabel = document.createElement('div')
    discLabel.textContent = 'Descuentos suscriptores:'
    discLabel.style.width = '100%'
    const noctInput = document.createElement('input')
    noctInput.type = 'number'; noctInput.min = '0'; noctInput.max = '100'; noctInput.placeholder = 'Nocturno %'
    noctInput.value = String(d.nocturno || 0); noctInput.style.width = '90px'
    const premInput = document.createElement('input')
    premInput.type = 'number'; premInput.min = '0'; premInput.max = '100'; premInput.placeholder = 'Premium %'
    premInput.value = String(d.premium || 0); premInput.style.width = '90px'
    const discBtn = document.createElement('button')
    discBtn.className = 'info'; discBtn.textContent = 'Guardar descuentos'
    discBtn.onclick = async () => {
      try {
        const secret = document.getElementById('admin-secret').value.trim()
        if (!secret) { showError('Ingresa clave admin'); return }
        await api('/api/admin/venues/discounts', { method: 'POST', headers: { 'X-Admin-Secret': secret }, body: JSON.stringify({ venueId: v.venueId, nocturno: Number(noctInput.value || 0), premium: Number(premInput.value || 0) }) })
        await loadVenues()
      } catch (e) { showError(String(e.message)) }
    }
    discRow.appendChild(discLabel); discRow.appendChild(noctInput); discRow.appendChild(premInput); discRow.appendChild(discBtn)
    inputs.appendChild(discRow)
    const actions = document.createElement('div')
    actions.className = 'actions'
    const addBtn = document.createElement('button')
    addBtn.className = 'success'
    addBtn.textContent = '+1 noche'
    addBtn.onclick = async () => {
      try {
        const secret = document.getElementById('admin-secret').value.trim()
        if (!secret) { showError('Ingresa clave admin'); return }
        await api('/api/admin/venues/credit', { method: 'POST', headers: { 'X-Admin-Secret': secret }, body: JSON.stringify({ venueId: v.venueId, amount: 1 }) })
        await loadVenues()
      } catch (e) { showError(String(e.message)) }
    }
    const subBtn = document.createElement('button')
    subBtn.className = 'warning'
    subBtn.textContent = '-1 noche'
    subBtn.onclick = async () => {
      try {
        const secret = document.getElementById('admin-secret').value.trim()
        if (!secret) { showError('Ingresa clave admin'); return }
        await api('/api/admin/venues/credit', { method: 'POST', headers: { 'X-Admin-Secret': secret }, body: JSON.stringify({ venueId: v.venueId, amount: -1 }) })
        await loadVenues()
      } catch (e) { showError(String(e.message)) }
    }
    const resetBtn = document.createElement('button')
    resetBtn.className = 'warning'
    resetBtn.textContent = 'Reset créditos'
    resetBtn.onclick = async () => {
      try {
        const secret = document.getElementById('admin-secret').value.trim()
        if (!secret) { showError('Ingresa clave admin'); return }
        const amount = -Number(v.credits || 0)
        await api('/api/admin/venues/credit', { method: 'POST', headers: { 'X-Admin-Secret': secret }, body: JSON.stringify({ venueId: v.venueId, amount }) })
        await loadVenues()
      } catch (e) { showError(String(e.message)) }
    }
    const actBtn = document.createElement('button')
    actBtn.className = v.active ? 'warning' : 'success'
    actBtn.textContent = v.active ? 'Desactivar' : 'Activar'
    actBtn.onclick = async () => {
      try {
        const secret = document.getElementById('admin-secret').value.trim()
        if (!secret) { showError('Ingresa clave admin'); return }
        await api('/api/admin/venues/active', { method: 'POST', headers: { 'X-Admin-Secret': secret }, body: JSON.stringify({ venueId: v.venueId, active: !v.active }) })
        await loadVenues()
      } catch (e) { showError(String(e.message)) }
    }
    const delBtn = document.createElement('button')
    delBtn.className = 'danger'
    delBtn.textContent = 'Eliminar'
    delBtn.onclick = async () => {
      try {
        const secret = document.getElementById('admin-secret').value.trim()
        if (!secret) { showError('Ingresa clave admin'); return }
        const ok = await confirmModal(`¿Eliminar el local "${v.name}" (${v.venueId})?`)
        if (!ok) return
        await api('/api/admin/venues/delete', { method: 'POST', headers: { 'X-Admin-Secret': secret }, body: JSON.stringify({ venueId: v.venueId }) })
        await loadVenues()
      } catch (e) { showError(String(e.message)) }
    }
    actions.appendChild(addBtn)
    actions.appendChild(subBtn)
    actions.appendChild(resetBtn)
    actions.appendChild(actBtn)
    actions.appendChild(delBtn)
    card.appendChild(head)
    card.appendChild(info)
    card.appendChild(link)
    card.appendChild(inputs)
    card.appendChild(actions)
    container.appendChild(card)
  }
}
async function loadVenues() {
  showError('')
  const secret = document.getElementById('admin-secret').value.trim()
  if (!secret) { showError('Ingresa clave admin'); return }
  try {
    const st = await api('/api/admin/db-status', { headers: { 'X-Admin-Secret': secret } })
    const el = document.getElementById('db-status')
    if (el) {
      let msg = st.connected ? 'DB conectada: los cambios persisten entre reinicios y deploys' : 'Sin DB: los cambios se guardan en archivo y pueden perderse en reinicios/deploys'
      if (st && st.error) msg += ` • Error: ${st.error}`
      el.textContent = msg
      el.className = 'info ' + (st.connected ? 'ok' : 'warn')
    }
  } catch (e) {
    const el = document.getElementById('db-status')
    if (el) {
      el.textContent = String(e.message || e || 'Error consultando DB')
      el.className = 'info warn'
    }
  }
  const r = await api('/api/admin/venues', { headers: { 'X-Admin-Secret': secret } })
  VENUES_CACHE = r.venues || []
  renderSummary(VENUES_CACHE)
  const f = document.getElementById('filter-text')
  const q = f ? f.value.trim().toLowerCase() : ''
  const filtered = q ? VENUES_CACHE.filter(v => (String(v.name || '').toLowerCase().includes(q) || String(v.venueId || '').toLowerCase().includes(q))) : VENUES_CACHE
  renderVenues(filtered)
}
document.getElementById('btn-load').onclick = () => loadVenues().catch(e => showError(String(e.message)))
document.getElementById('btn-refresh').onclick = () => loadVenues().catch(e => showError(String(e.message)))
document.getElementById('btn-db-init').onclick = async () => {
  showError('')
  const secret = document.getElementById('admin-secret').value.trim()
  if (!secret) { showError('Ingresa clave admin'); return }
  try {
    const r = await api('/api/admin/db-init', { method: 'POST', headers: { 'X-Admin-Secret': secret } })
    const list = Array.isArray(r.tables) ? r.tables.join(', ') : ''
    await confirmModal(`DB inicializada. Tablas: ${list || '-'}`)
    await loadVenues()
  } catch (e) { showError(String(e.message)) }
}
document.getElementById('modal-close').onclick = () => { const m = document.getElementById('modal'); if (m) m.classList.remove('show') }
document.getElementById('modal').onclick = (e) => { if (e.target && e.target.id === 'modal') { const m = document.getElementById('modal'); if (m) m.classList.remove('show') } }
async function confirmModal(text) {
  return await new Promise(resolve => {
    const m = document.getElementById('modal')
    const t = document.getElementById('modal-text')
    const row = document.querySelector('#modal .row')
    const closeBtn = document.getElementById('modal-close')
    if (!m || !t || !row || !closeBtn) { resolve(true); return }
    m.classList.remove('type-error','type-info','type-success')
    m.classList.add('type-info')
    t.textContent = text || ''
    m.classList.add('show')
    let btn = document.getElementById('modal-action')
    if (btn) { try { btn.remove() } catch {} }
    btn = document.createElement('button')
    btn.className = 'info'
    btn.id = 'modal-action'
    btn.textContent = 'Aceptar'
    btn.onclick = () => { try { m.classList.remove('show') } catch {}; resolve(true) }
    if (closeBtn && closeBtn.parentElement === row) row.insertBefore(btn, closeBtn)
    else row.appendChild(btn)
    const onCancel = () => { try { m.classList.remove('show') } catch {}; resolve(false) }
    closeBtn.onclick = onCancel
    const onOverlay = (e) => { if (e.target && e.target.id === 'modal') { onCancel() } }
    m.addEventListener('click', onOverlay, { once: true })
  })
}
document.getElementById('btn-add-credit').onclick = async () => {
  showError('')
  const secret = document.getElementById('admin-secret').value.trim()
  const venueId = document.getElementById('venue-new-id').value.trim()
  const amountStr = document.getElementById('venue-new-amount').value
  const amount = Math.max(1, Number(amountStr || 1))
  if (!secret) { showError('Ingresa clave admin'); return }
  if (!venueId) { showError('Ingresa un venueId'); return }
  try {
    await api('/api/admin/venues/credit', {
      method: 'POST',
      headers: { 'X-Admin-Secret': secret },
      body: JSON.stringify({ venueId, amount })
    })
    await loadVenues()
  } catch (e) { showError(String(e.message)) }
}
document.getElementById('btn-del-credit').onclick = async () => {
  showError('')
  const secret = document.getElementById('admin-secret').value.trim()
  const venueId = document.getElementById('venue-del-id').value.trim()
  const amountStr = document.getElementById('venue-del-amount').value
  const amount = Math.max(1, Number(amountStr || 1))
  if (!secret) { showError('Ingresa clave admin'); return }
  if (!venueId) { showError('Ingresa un venueId'); return }
  try {
    await api('/api/admin/venues/credit', {
      method: 'POST',
      headers: { 'X-Admin-Secret': secret },
      body: JSON.stringify({ venueId, amount: -amount })
    })
    await loadVenues()
  } catch (e) { showError(String(e.message)) }
}
document.getElementById('btn-reset-credit').onclick = async () => {
  showError('')
  const secret = document.getElementById('admin-secret').value.trim()
  const venueId = document.getElementById('venue-reset-id').value.trim()
  if (!secret) { showError('Ingresa clave admin'); return }
  if (!venueId) { showError('Ingresa un venueId'); return }
  const target = VENUES_CACHE.find(v => v.venueId === venueId)
  if (!target) { showError('Venue no encontrado. Cárgalo y revisa el ID.'); return }
  try {
    const amount = -Number(target.credits || 0)
    await api('/api/admin/venues/credit', {
      method: 'POST',
      headers: { 'X-Admin-Secret': secret },
      body: JSON.stringify({ venueId, amount })
    })
    await loadVenues()
  } catch (e) { showError(String(e.message)) }
}
const ft = document.getElementById('filter-text')
if (ft) ft.oninput = () => {
  const q = ft.value.trim().toLowerCase()
  const filtered = q ? VENUES_CACHE.filter(v => (String(v.name || '').toLowerCase().includes(q) || String(v.venueId || '').toLowerCase().includes(q))) : VENUES_CACHE
  renderVenues(filtered)
}
const cf = document.getElementById('btn-clear-filter')
if (cf) cf.onclick = () => { const f = document.getElementById('filter-text'); if (f) f.value = ''; renderVenues(VENUES_CACHE) }

// Códigos de suscripción
async function loadSubCodes() {
  const secret = document.getElementById('admin-secret').value.trim()
  if (!secret) { showError('Ingresa clave admin'); return }
  try {
    const r = await api('/api/admin/subscription/codes', { headers: { 'X-Admin-Secret': secret } })
    const container = document.getElementById('sub-codes-list')
    if (!container) return
    container.innerHTML = ''
    if (!(r.codes || []).length) { container.textContent = 'Sin códigos'; return }
    for (const c of r.codes) {
      const row = document.createElement('div')
      row.className = 'row compact'
      row.style.marginTop = '4px'
      const lbl = document.createElement('span')
      const tierColors = { nocturno: '#2c6bff', premium: '#a855f7' }
      lbl.innerHTML = `<strong>${escapeHtml(c.code)}</strong> — <span style="color:${tierColors[c.tier]||'#ccc'}">${escapeHtml(c.tier)}</span> — Usos: ${Number(c.usedCount)}/${Number(c.maxUses)}`
      lbl.style.flex = '1'
      const delBtn = document.createElement('button')
      delBtn.className = 'danger'; delBtn.textContent = 'Eliminar'
      delBtn.onclick = async () => {
        try {
          const s = document.getElementById('admin-secret').value.trim()
          await api('/api/admin/subscription/codes/delete', { method: 'POST', headers: { 'X-Admin-Secret': s }, body: JSON.stringify({ code: c.code }) })
          await loadSubCodes()
        } catch (e) { showError(String(e.message)) }
      }
      row.appendChild(lbl); row.appendChild(delBtn)
      container.appendChild(row)
    }
  } catch (e) { showError(String(e.message)) }
}
document.getElementById('btn-create-code').onclick = async () => {
  const secret = document.getElementById('admin-secret').value.trim()
  const tier = document.getElementById('new-code-tier').value
  const maxUses = Number(document.getElementById('new-code-uses').value || 100)
  const customCode = document.getElementById('new-code-custom').value.trim()
  if (!secret) { showError('Ingresa clave admin'); return }
  try {
    const body = { tier, maxUses }
    if (customCode) body.code = customCode
    await api('/api/admin/subscription/codes', { method: 'POST', headers: { 'X-Admin-Secret': secret }, body: JSON.stringify(body) })
    document.getElementById('new-code-custom').value = ''
    await loadSubCodes()
  } catch (e) { showError(String(e.message)) }
}
document.getElementById('btn-load-codes').onclick = () => loadSubCodes().catch(e => showError(String(e.message)))

async function loadVenueProfiles() {
  const secret = document.getElementById('admin-secret').value.trim()
  if (!secret) { showError('Ingresa clave admin'); return }
  const container = document.getElementById('venue-profiles-list')
  container.innerHTML = '<div style="color:#aaa;font-size:13px">Cargando...</div>'
  const res = await api('/api/venues/directory', { headers: { 'X-Admin-Secret': secret } })
  const venues = res.venues || []
  container.innerHTML = ''
  if (!venues.length) { container.innerHTML = '<div style="color:#666">No hay venues</div>'; return }
  for (const v of venues) {
    const card = document.createElement('div')
    card.className = 'venue-card'
    card.style.marginBottom = '12px'
    card.innerHTML = `
      <strong>${escapeHtml(v.name)}</strong> <span style="color:#aaa;font-size:12px">(${escapeHtml(v.venueId)})</span>
      <div style="margin-top:8px;display:flex;flex-direction:column;gap:6px">
        <input id="vp-desc-${escapeHtml(v.venueId)}" placeholder="Descripción (ej. Club underground con música house)" value="${escapeHtml(v.description || '')}" style="width:100%">
        <div class="row" style="gap:6px">
          <input id="vp-genre-${escapeHtml(v.venueId)}" placeholder="Género musical (ej. Techno, House)" value="${escapeHtml(v.musicGenre || '')}" style="flex:1">
          <input id="vp-loc-${escapeHtml(v.venueId)}" placeholder="Ubicación (ej. Palermo, CABA)" value="${escapeHtml(v.location || '')}" style="flex:1">
        </div>
        <button id="btn-save-vp-${v.venueId}" class="success" style="align-self:flex-start">Guardar perfil</button>
      </div>
      <div style="margin-top:12px;border-top:1px solid #333;padding-top:10px">
        <strong style="font-size:13px">Próximos eventos</strong>
        <div id="evt-list-${v.venueId}" style="margin:8px 0"></div>
        <div class="row" style="gap:6px;flex-wrap:wrap;margin-top:6px">
          <input id="evt-name-${v.venueId}" placeholder="Nombre del evento" style="flex:2;min-width:120px">
          <input id="evt-dj-${v.venueId}" placeholder="DJ" style="flex:1;min-width:80px">
          <input id="evt-date-${v.venueId}" type="datetime-local" style="flex:1;min-width:140px">
          <input id="evt-cover-${v.venueId}" type="number" placeholder="Cover $" style="width:80px">
          <button id="btn-add-evt-${v.venueId}" class="info" style="white-space:nowrap">+ Agregar</button>
        </div>
      </div>`
    container.appendChild(card)
    document.getElementById(`btn-save-vp-${v.venueId}`).onclick = async () => {
      await api('/api/admin/venue/profile', { method: 'POST', headers: { 'X-Admin-Secret': secret }, body: JSON.stringify({ venueId: v.venueId, description: document.getElementById(`vp-desc-${v.venueId}`).value, musicGenre: document.getElementById(`vp-genre-${v.venueId}`).value, location: document.getElementById(`vp-loc-${v.venueId}`).value }) })
      showSuccess('Perfil guardado')
    }
    const renderEvents = (events) => {
      const evList = document.getElementById(`evt-list-${v.venueId}`)
      evList.innerHTML = ''
      if (!events || !events.length) { evList.innerHTML = '<div style="color:#666;font-size:12px">Sin eventos cargados</div>'; return }
      for (const ev of events) {
        const row = document.createElement('div')
        row.style.cssText = 'display:flex;align-items:center;gap:8px;padding:4px 0;border-bottom:1px solid #222'
        const d = new Date(ev.date); const ds = `${d.getDate()}/${d.getMonth()+1} ${d.getHours()}:${String(d.getMinutes()).padStart(2,'0')}`
        row.innerHTML = `<span style="flex:1;font-size:13px">${escapeHtml(ev.eventName || 'Evento')} — ${ds}${ev.djName ? ' / DJ: '+escapeHtml(ev.djName) : ''}</span><button class="warning" style="font-size:11px;padding:3px 8px">Eliminar</button>`
        row.querySelector('button').onclick = async () => { await api('/api/admin/venue/event', { method: 'DELETE', headers: { 'X-Admin-Secret': secret }, body: JSON.stringify({ venueId: v.venueId, eventId: ev.id }) }); await loadVenueProfiles() }
        evList.appendChild(row)
      }
    }
    renderEvents(v.upcomingEvents || [])
    document.getElementById(`btn-add-evt-${v.venueId}`).onclick = async () => {
      const dateVal = document.getElementById(`evt-date-${v.venueId}`).value
      if (!dateVal) { alert('Ingresa fecha y hora'); return }
      const r = await api('/api/admin/venue/event', { method: 'POST', headers: { 'X-Admin-Secret': secret }, body: JSON.stringify({ venueId: v.venueId, eventName: document.getElementById(`evt-name-${v.venueId}`).value, djName: document.getElementById(`evt-dj-${v.venueId}`).value, date: new Date(dateVal).getTime(), coverPrice: Number(document.getElementById(`evt-cover-${v.venueId}`).value || 0) }) })
      renderEvents(r.events || [])
    }
  }
}
document.getElementById('btn-load-venue-profiles').onclick = () => loadVenueProfiles().catch(e => showError(String(e.message)))

// ─── Pagos de suscripción ───────────────────────────────────────────────
function showSuccess(msg) {
  const m = document.getElementById('modal')
  const t = document.getElementById('modal-text')
  if (!m || !t) return
  m.classList.remove('type-error','type-info','type-success')
  m.classList.add('type-success')
  t.textContent = msg
  m.classList.add('show')
}

async function loadPayments() {
  const secret = document.getElementById('admin-secret').value.trim()
  if (!secret) { showError('Ingresa clave admin'); return }
  const statusFilter = document.getElementById('payments-filter').value
  const container = document.getElementById('payments-list')
  container.innerHTML = '<div style="color:#aaa;font-size:13px">Cargando...</div>'
  try {
    const url = '/api/admin/payments' + (statusFilter ? `?status=${statusFilter}` : '')
    const r = await api(url, { headers: { 'X-Admin-Secret': secret } })
    const payments = r.payments || []
    container.innerHTML = ''
    if (!payments.length) { container.innerHTML = '<div style="color:#666;font-size:13px">Sin pagos encontrados</div>'; return }
    const STATUS_LABELS = { pending: 'Pendiente', approved: 'Aprobado', rejected: 'Rechazado' }
    const STATUS_COLORS = { pending: '#fbbf24', approved: '#4ade80', rejected: '#f87171' }
    for (const p of payments) {
      const card = document.createElement('div')
      card.style.cssText = 'border:1px solid #333;border-radius:8px;padding:12px;margin-bottom:10px'
      const d = new Date(p.createdAt)
      const dateStr = `${d.getDate()}/${d.getMonth()+1}/${d.getFullYear()} ${d.getHours()}:${String(d.getMinutes()).padStart(2,'0')}`
      const tierColors = { nocturno: '#2c6bff', premium: '#a855f7' }
      const color = STATUS_COLORS[p.status] || '#aaa'
      const tierColor = tierColors[p.plan] || '#aaa'
      card.innerHTML = `
        <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:8px">
          <div>
            <span style="font-weight:700;color:${tierColor};font-size:14px">Plan ${escapeHtml(p.plan)}</span>
            <span style="margin-left:8px;font-size:13px;color:#aaa">$${Number(p.price || 0).toLocaleString('es-CO')}</span>
          </div>
          <span style="font-size:12px;color:${color};font-weight:600">${escapeHtml(STATUS_LABELS[p.status] || p.status)}</span>
        </div>
        <div style="font-size:12px;color:#aaa;margin-bottom:4px">ID suscriptor: <code style="color:#ccc">${escapeHtml(p.subscriberId)}</code></div>
        <div style="font-size:12px;color:#aaa;margin-bottom:4px">Método: ${escapeHtml(p.method)} · Ref: <code style="color:#ccc">${escapeHtml(p.reference)}</code></div>
        ${p.payerName ? `<div style="font-size:12px;color:#aaa;margin-bottom:4px">Titular: ${escapeHtml(p.payerName)}</div>` : ''}
        <div style="font-size:11px;color:#555;margin-bottom:8px">${dateStr} · ID pago: ${escapeHtml(p.id)}</div>
        ${p.proofUrl && /^https:\/\//i.test(p.proofUrl) ? `<div style="margin-bottom:8px"><img src="${escapeHtml(p.proofUrl)}" style="max-width:180px;max-height:120px;border-radius:6px;border:1px solid #333;cursor:pointer"></div>` : ''}
        ${p.rejectReason ? `<div style="font-size:12px;color:#f87171;margin-bottom:8px">Motivo rechazo: ${escapeHtml(p.rejectReason)}</div>` : ''}
      `
      const proofImg = card.querySelector('img')
      if (proofImg) proofImg.onclick = () => window.open(proofImg.src, '_blank')
      if (p.status === 'pending') {
        const actRow = document.createElement('div')
        actRow.style.cssText = 'display:flex;gap:8px;margin-top:4px'
        const approveBtn = document.createElement('button')
        approveBtn.className = 'success'; approveBtn.textContent = 'Aprobar'
        approveBtn.style.cssText = 'flex:1;font-size:13px'
        approveBtn.onclick = async () => {
          try {
            const s = document.getElementById('admin-secret').value.trim()
            await api('/api/admin/payments/approve', { method: 'POST', headers: { 'X-Admin-Secret': s }, body: JSON.stringify({ paymentId: p.id }) })
            showSuccess('Pago aprobado — suscripción activada')
            await loadPayments()
          } catch (e) { showError(String(e.message)) }
        }
        const rejectBtn = document.createElement('button')
        rejectBtn.className = 'danger'; rejectBtn.textContent = 'Rechazar'
        rejectBtn.style.cssText = 'flex:1;font-size:13px'
        rejectBtn.onclick = async () => {
          const reason = prompt('Motivo del rechazo (opcional):') || ''
          try {
            const s = document.getElementById('admin-secret').value.trim()
            await api('/api/admin/payments/reject', { method: 'POST', headers: { 'X-Admin-Secret': s }, body: JSON.stringify({ paymentId: p.id, reason }) })
            showSuccess('Pago rechazado')
            await loadPayments()
          } catch (e) { showError(String(e.message)) }
        }
        actRow.appendChild(approveBtn); actRow.appendChild(rejectBtn)
        card.appendChild(actRow)
      }
      container.appendChild(card)
    }
  } catch (e) { showError(String(e.message)) }
}
document.getElementById('btn-load-payments').onclick = () => loadPayments().catch(e => showError(String(e.message)))
