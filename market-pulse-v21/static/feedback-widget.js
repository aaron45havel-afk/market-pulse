/* FocusedOps Feedback Widget
 *
 * Drop-in client-feedback widget. Include once in the prototype's HTML:
 *
 *   <script
 *     src="https://market-pulse-production-628a.up.railway.app/static/feedback-widget.js"
 *     data-token="<the-prototype-feedback-token>"
 *     defer
 *   ></script>
 *
 * Renders a floating button bottom-right. Click opens a comment box.
 * Submit POSTs to /api/feedback/<token> on market-pulse, which
 * appends to the prototype's feedback log and emails the team.
 */
(function () {
  var script = document.currentScript;
  if (!script) {
    // currentScript is null for deferred scripts in some old browsers;
    // fall back to scanning for ourselves.
    var all = document.querySelectorAll('script[data-token][src*="feedback-widget"]');
    if (all.length) script = all[all.length - 1];
  }
  if (!script) return;
  var TOKEN  = script.getAttribute('data-token');
  // Match the host page's brand when it exposes --primary (Market Pulse
  // does); fall back to the brand indigo on external embeds.
  var COLOR  = script.getAttribute('data-color') ||
      (getComputedStyle(document.documentElement).getPropertyValue('--primary') || '').trim() ||
      '#5b4de0';
  var LABEL  = script.getAttribute('data-label')  || '💬 Feedback';
  if (!TOKEN) {
    console.warn('[focusedops-feedback] no data-token attribute set');
    return;
  }

  // Origin to POST to. Derived from the script src so the widget works
  // when copied across staging / prod / preview environments without
  // hard-coding a host.
  var SCRIPT_SRC = script.getAttribute('src') || '';
  var API_ORIGIN = '';
  try { API_ORIGIN = new URL(SCRIPT_SRC, location.href).origin; }
  catch (_) { API_ORIGIN = ''; }
  if (!API_ORIGIN) {
    console.warn('[focusedops-feedback] could not derive API origin');
    return;
  }
  var SUBMIT_URL = API_ORIGIN + '/api/feedback/' + encodeURIComponent(TOKEN);

  function el(tag, attrs, children) {
    var n = document.createElement(tag);
    if (attrs) for (var k in attrs) {
      if (k === 'style') Object.assign(n.style, attrs[k]);
      else if (k === 'onclick') n.addEventListener('click', attrs[k]);
      else n.setAttribute(k, attrs[k]);
    }
    (children || []).forEach(function (c) {
      if (typeof c === 'string') n.appendChild(document.createTextNode(c));
      else if (c) n.appendChild(c);
    });
    return n;
  }

  // ── Floating launcher button ──────────────────────────────────
  var btn = el('button', {
    id: '__fo_fb_btn',
    type: 'button',
    'aria-label': 'Send feedback',
    style: {
      position: 'fixed', bottom: '20px', right: '20px',
      background: COLOR, color: '#fff',
      border: 'none', borderRadius: '999px',
      padding: '12px 18px',
      fontSize: '14px', fontWeight: '600',
      fontFamily: 'var(--sans, -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif)',
      cursor: 'pointer', zIndex: '2147483646',
      boxShadow: '0 6px 22px rgba(0,0,0,0.18)',
      transition: 'transform 0.12s, box-shadow 0.12s',
    },
    onclick: function () { openModal(); },
  }, [LABEL]);
  btn.addEventListener('mouseenter', function () {
    btn.style.transform = 'translateY(-1px)';
    btn.style.boxShadow = '0 8px 28px rgba(0,0,0,0.22)';
  });
  btn.addEventListener('mouseleave', function () {
    btn.style.transform = ''; btn.style.boxShadow = '0 6px 22px rgba(0,0,0,0.18)';
  });

  // ── Modal ─────────────────────────────────────────────────────
  var modal = null, statusEl = null, msgEl = null, emailEl = null, nameEl = null, shotEl = null;

  function ensureModal() {
    if (modal) return;
    var backdrop = el('div', {
      id: '__fo_fb_bd',
      style: {
        position: 'fixed', inset: '0', background: 'rgba(15,26,44,0.45)',
        display: 'none', alignItems: 'flex-end', justifyContent: 'center',
        zIndex: '2147483647',
        padding: '0',
      },
      onclick: function (e) { if (e.target === backdrop) closeModal(); },
    });
    var card = el('div', {
      style: {
        background: '#fff', color: 'var(--ink, #1a1917)',
        width: 'min(440px, 100%)', maxHeight: '80vh',
        borderRadius: '14px 14px 0 0',
        padding: '1.1rem 1.2rem 1.2rem',
        boxShadow: '0 -12px 40px rgba(0,0,0,0.18)',
        fontFamily: 'var(--sans, -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif)',
        fontSize: '14px', lineHeight: '1.5',
        overflowY: 'auto',
      },
    });
    var title = el('div', {
      style: { fontSize: '16px', fontWeight: '700', marginBottom: '4px' }
    }, ['Send feedback']);
    var sub = el('div', {
      style: { fontSize: '13px', color: '#4B5563', marginBottom: '12px' }
    }, ['Rough notes are gold — even half a sentence helps.']);

    nameEl  = inputField('Name (optional)',  'text');
    emailEl = inputField('Email (optional)', 'email');
    msgEl   = el('textarea', {
      placeholder: "What's on your mind? What should change?",
      style: {
        width: '100%', boxSizing: 'border-box',
        border: '1px solid #D1D5DB', borderRadius: '8px',
        padding: '0.55rem 0.65rem',
        minHeight: '110px', resize: 'vertical',
        fontFamily: 'inherit', fontSize: '14px', lineHeight: '1.5',
        outline: 'none', marginTop: '8px',
      },
    });
    msgEl.addEventListener('paste', handlePaste);

    var shotWrap = el('div', { style: { marginTop: '10px' } });
    var shotLabel = el('label', {
      style: { display: 'block', fontSize: '11px', fontWeight: '700', textTransform: 'uppercase', letterSpacing: '0.05em', color: '#4B5563', marginBottom: '4px' }
    }, ['Screenshot (optional, paste-image works too)']);
    shotEl = el('input', { type: 'file', accept: 'image/*',
      style: { fontFamily: 'inherit', fontSize: '13px' } });
    shotWrap.appendChild(shotLabel);
    shotWrap.appendChild(shotEl);

    var hintEl = el('div', {
      id: '__fo_fb_hint',
      style: { fontSize: '12px', color: '#6B7280', marginTop: '4px' }
    }, ['']);
    shotWrap.appendChild(hintEl);

    var actions = el('div', {
      style: { display: 'flex', alignItems: 'center', gap: '0.6rem', marginTop: '14px' }
    });
    var send = el('button', {
      type: 'button',
      style: {
        background: COLOR, color: '#fff',
        border: 'none', borderRadius: '8px',
        padding: '0.6rem 1.2rem',
        fontSize: '14px', fontWeight: '600',
        cursor: 'pointer', fontFamily: 'inherit',
      },
      onclick: function () { submit(); },
    }, ['Send →']);
    var cancel = el('button', {
      type: 'button',
      style: {
        background: 'transparent', color: '#6B7280',
        border: 'none', padding: '0.6rem 0.8rem',
        fontSize: '13px', cursor: 'pointer', fontFamily: 'inherit',
      },
      onclick: function () { closeModal(); },
    }, ['Cancel']);
    statusEl = el('span', {
      style: { marginLeft: 'auto', fontSize: '12px', color: '#6B7280' }
    }, ['']);
    actions.appendChild(send);
    actions.appendChild(cancel);
    actions.appendChild(statusEl);

    card.appendChild(title);
    card.appendChild(sub);
    card.appendChild(nameEl);
    card.appendChild(emailEl);
    card.appendChild(msgEl);
    card.appendChild(shotWrap);
    card.appendChild(actions);

    backdrop.appendChild(card);
    document.body.appendChild(backdrop);
    modal = backdrop;
  }

  function inputField(placeholder, type) {
    return el('input', {
      type: type, placeholder: placeholder,
      style: {
        width: '100%', boxSizing: 'border-box',
        border: '1px solid #D1D5DB', borderRadius: '8px',
        padding: '0.5rem 0.65rem',
        fontSize: '14px', outline: 'none',
        fontFamily: 'inherit', marginTop: '8px',
      },
    });
  }

  function openModal() {
    ensureModal();
    modal.style.display = 'flex';
    setTimeout(function () { msgEl && msgEl.focus(); }, 30);
  }
  function closeModal() {
    if (modal) modal.style.display = 'none';
    if (statusEl) statusEl.textContent = '';
  }

  function handlePaste(e) {
    if (!e.clipboardData || !e.clipboardData.items) return;
    for (var i = 0; i < e.clipboardData.items.length; i++) {
      var item = e.clipboardData.items[i];
      if (item.type && item.type.indexOf('image/') === 0) {
        var blob = item.getAsFile();
        if (!blob) continue;
        var dt = new DataTransfer();
        dt.items.add(blob);
        shotEl.files = dt.files;
        var hint = document.getElementById('__fo_fb_hint');
        if (hint) hint.textContent = '✓ Screenshot pasted (' + Math.round(blob.size / 1024) + ' KB)';
        break;
      }
    }
  }

  async function blobToBase64(blob) {
    return new Promise(function (resolve, reject) {
      var fr = new FileReader();
      fr.onerror = reject;
      fr.onload  = function () {
        var s = fr.result || '';
        var idx = String(s).indexOf(',');
        resolve(idx >= 0 ? String(s).slice(idx + 1) : '');
      };
      fr.readAsDataURL(blob);
    });
  }

  async function submit() {
    var text = (msgEl.value || '').trim();
    if (!text) { statusEl.textContent = 'Add a note first.'; return; }
    statusEl.textContent = 'Sending…';

    var payload = {
      feedback: text,
      name:  (nameEl.value || '').trim(),
      email: (emailEl.value || '').trim(),
      page_url: location.href,
    };
    if (shotEl.files && shotEl.files[0]) {
      try {
        var b64 = await blobToBase64(shotEl.files[0]);
        if (b64 && b64.length < 5_000_000) {
          payload.screenshot_filename = shotEl.files[0].name || 'screenshot.png';
          payload.screenshot_b64 = b64;
        }
      } catch (_) {}
    }
    try {
      var r = await fetch(SUBMIT_URL, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(payload),
      });
      if (!r.ok) throw new Error('HTTP ' + r.status);
      statusEl.textContent = 'Sent ✓';
      msgEl.value = ''; nameEl.value = ''; emailEl.value = '';
      if (shotEl) shotEl.value = '';
      var hint = document.getElementById('__fo_fb_hint');
      if (hint) hint.textContent = '';
      setTimeout(closeModal, 1100);
    } catch (e) {
      statusEl.textContent = 'Failed — try again.';
      console.warn('[focusedops-feedback] submit failed:', e);
    }
  }

  function mount() {
    if (!document.body) return setTimeout(mount, 40);
    document.body.appendChild(btn);
  }
  mount();
})();
