const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {test} = require('node:test');
const html = fs.readFileSync(path.join(__dirname, 'dashboard.html'), 'utf8');
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
const fixture = () => ({total: 5, by_hazard: {SEISMIC: 3, TSUNAMI: 0, VOLCANIC: 0, WILDFIRE: 2, COASTAL: 0},
  daily: [{date: '2026-11-01', counts: {SEISMIC: 3}}, {date: '2026-11-03', counts: {WILDFIRE: 2}}],
  by_severity: {NORMAL: 3, WASPADA: 0, SIAGA: 0, AWAS: 2}, by_area: {'Jawa Barat': 3, 'Bali': 2}, partial: false, unavailable_units: []});

function page() {
  const elements = new Map();
  class Element {
    constructor(tag = '') { this.tag = tag; this.children = []; this.style = {}; this.dataset = {}; this.attributes = {}; this.listeners = {}; this.value = ''; }
    set id(value) { this._id = value; elements.set(value, this); }
    get id() { return this._id; }
    set textContent(value) { this.children = []; this.text = String(value); }
    get textContent() { return (this.text || '') + this.children.map(child => child.textContent).join(''); }
    append(...nodes) { this.children.push(...nodes); }
    replaceChildren(...nodes) { this.text = ''; this.children = nodes; }
    setAttribute(key, value) { this.attributes[key] = String(value); }
    addEventListener(name, listener) { this.listeners[name] = listener; }
    set innerHTML(_) { throw new Error('Untrusted source fields must use DOM text, not innerHTML'); }
  }
  for (const match of html.matchAll(/\bid="([^"]+)"/g)) new Element().id = match[1];
  const document = {getElementById: id => elements.get(id), createElement: tag => new Element(tag),
    createElementNS: (_, tag) => new Element(tag), createTextNode: text => { const node = new Element(); node.textContent = text; return node; }};
  let result = fixture(), status = 200;
  const calls = [];
  vm.runInNewContext(script, {document, Intl, Date, URLSearchParams, AbortSignal, performance,
    fetch: async (url, options) => { calls.push({url, options}); return {ok: status === 200, status, json: async () => result}; }});
  elements.get('token').value = ' synthetic-token ';
  return {get: id => elements.get(id), calls,
    response(data, code = 200) { result = data; status = code; },
    submit: () => elements.get('controls').listeners.submit({preventDefault() {}})};
}

test('complete analytics renders real totals, five series and zero-filled UTC gaps', async () => {
  const p = page(); await p.submit();
  assert.equal(p.get('total').textContent, '5');
  assert.equal(p.get('critical').textContent, '2');
  assert.equal(p.get('area-count').textContent, '2');
  assert.equal(p.get('connection').textContent, 'Data lengkap');
  assert.equal(p.get('panels').dataset.stale, 'false');
  const lines = p.get('chart').children.filter(node => node.tag === 'polyline');
  assert.equal(lines.length, 5);
  assert.equal(lines[0].attributes.points.split(' ').length, 3);
  assert.equal(p.calls[0].options.headers.Authorization, 'Bearer synthetic-token');
  assert.equal(p.calls[0].options.cache, 'no-store');
  assert.equal(p.get('load').disabled, false);
});

test('partial and lagging replicas are visibly distinguished from complete data', async () => {
  const p = page(); p.response({...fixture(), partial: true, unavailable_units: ['store-b']}); await p.submit();
  assert.equal(p.get('connection').textContent, 'Data parsial');
  assert.match(p.get('status').textContent, /store-b/);
  p.response({...fixture(), replication_degraded: true, lagging_units: ['store-c']}); await p.submit();
  assert.equal(p.get('connection').textContent, 'Replika tertinggal');
  assert.match(p.get('status').textContent, /store-c/);
});

test('expired tokens keep previous numbers explicitly stale instead of zeroing them', async () => {
  const p = page(); await p.submit(); p.response({}, 401); await p.submit();
  assert.equal(p.get('total').textContent, '5');
  assert.equal(p.get('connection').textContent, 'Data lama');
  assert.equal(p.get('panels').dataset.stale, 'true');
  assert.match(p.get('status').textContent, /HTTP 401/);
  assert.equal(p.get('panels').attributes['aria-busy'], 'false');
});

test('initial errors do not invent metrics', async () => {
  const p = page(); p.response({}, 503); await p.submit();
  assert.equal(p.get('connection').textContent, 'Gagal memuat');
  assert.match(p.get('status').textContent, /Belum ada angka/);
});

test('empty ranges and single-day charts render without NaN coordinates', async () => {
  const p = page(); p.response({...fixture(), total: 0, daily: [], by_hazard: {}, by_area: {}, by_severity: {}}); await p.submit();
  assert.equal(p.get('total').textContent, '0');
  assert.match(p.get('chart').textContent, /Tidak ada kejadian/);
  p.response({...fixture(), daily: [fixture().daily[0]]}); await p.submit();
  const circles = p.get('chart').children.filter(node => node.tag === 'circle');
  assert.equal(circles.length, 5);
  assert.equal(circles[0].attributes.cx, '575');
  assert.ok(circles.every(node => Number.isFinite(Number(node.attributes.cy))));
});

test('range controls use UTC bounds and reject reversed dates without fetching', async () => {
  const p = page(); p.get('from').value = '2026-11-01'; p.get('to').value = '2026-11-03'; await p.submit();
  const url = new URL(p.calls[0].url, 'http://localhost');
  assert.equal(url.searchParams.get('from'), '2026-11-01T00:00:00Z');
  assert.equal(url.searchParams.get('to'), '2026-11-03T23:59:59.999999999Z');
  p.get('from').value = '2026-11-10'; await p.submit();
  assert.equal(p.calls.length, 1);
  assert.equal(p.get('connection').textContent, 'Rentang tidak valid');
});

test('source area names remain inert text', async () => {
  const p = page(), name = '<img src=x onerror=alert(1)>';
  p.response({...fixture(), by_area: {[name]: 5}}); await p.submit();
  assert.match(p.get('areas').textContent, /<img src=x onerror=alert\(1\)>/);
  assert.equal(p.get('connection').textContent, 'Data lengkap');
});

test('malformed analytics cannot overwrite a valid previous result', async () => {
  const p = page(); await p.submit(); p.response({total: 20}); await p.submit();
  assert.equal(p.get('total').textContent, '5');
  assert.equal(p.get('connection').textContent, 'Data lama');
});

test('narrow charts retain pixel-sized labels and fit their actual viewport', async () => {
  const p = page(); p.get('chart').clientWidth = 340; p.get('chart').clientHeight = 200; await p.submit();
  assert.equal(p.get('chart').attributes.viewBox, '0 0 340 200');
  const lines = p.get('chart').children.filter(node => node.tag === 'polyline');
  for (const line of lines) for (const point of line.attributes.points.split(' ')) {
    const [x, y] = point.split(',').map(Number);
    assert.ok(x >= 65 && x <= 325 && y >= 20 && y <= 160);
  }
});
