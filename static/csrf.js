/*
 * Double-submit CSRF cookie — see main.py's CSRFMiddleware for the server
 * side of this. Mints a random token into a same-origin, JS-readable cookie
 * (a cross-site page can't read or set it — same-origin policy — so it
 * can't produce a request carrying a matching header), then patches
 * window.fetch so every existing fetch() call in this app attaches it
 * automatically. No template needs to know this file exists beyond
 * including the <script> tag.
 */
(function () {
  var COOKIE_NAME = "csrf_token";
  var HEADER_NAME = "X-CSRF-Token";
  var SAFE_METHODS = { GET: true, HEAD: true, OPTIONS: true, TRACE: true };

  function readCookie(name) {
    var match = document.cookie.match(
      new RegExp("(?:^|; )" + name.replace(/([.$?*|{}()[\]\\/+^])/g, "\\$1") + "=([^;]*)")
    );
    return match ? decodeURIComponent(match[1]) : null;
  }

  function randomToken() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    // Fallback for older browsers / non-secure contexts where randomUUID
    // is unavailable — still cryptographically random, just formatted plainly.
    var bytes = new Uint8Array(24);
    (window.crypto || {}).getRandomValues ? crypto.getRandomValues(bytes) : bytes.forEach(function (_, i) { bytes[i] = Math.floor(Math.random() * 256); });
    return Array.prototype.map.call(bytes, function (b) { return b.toString(16).padStart(2, "0"); }).join("");
  }

  function ensureToken() {
    var token = readCookie(COOKIE_NAME);
    if (token) return token;
    token = randomToken();
    var secure = location.protocol === "https:" ? "; Secure" : "";
    // 7 days, matching the session cookie's own lifetime (main.py's
    // SessionMiddleware max_age) — no reason for this to expire independently
    // mid-session and start failing requests.
    document.cookie = COOKIE_NAME + "=" + encodeURIComponent(token) +
      "; Path=/; Max-Age=" + (60 * 60 * 24 * 7) + "; SameSite=Lax" + secure;
    return token;
  }

  var token = ensureToken();

  function isSameOrigin(input) {
    try {
      var url = new URL(input, location.href);
      return url.origin === location.origin;
    } catch (e) {
      return false;
    }
  }

  var originalFetch = window.fetch;
  window.fetch = function (input, init) {
    var method = ((init && init.method) || (input && input.method) || "GET").toUpperCase();
    var target = (input && input.url) || input;
    if (!SAFE_METHODS[method] && isSameOrigin(target)) {
      init = init ? Object.assign({}, init) : {};
      var headers = new Headers(init.headers || (input && input.headers) || {});
      headers.set(HEADER_NAME, token);
      init.headers = headers;
    }
    return originalFetch.call(this, input, init);
  };
})();
