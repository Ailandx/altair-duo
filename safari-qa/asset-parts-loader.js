/* Serves WASM as a decoded gzip stream when supported, otherwise <=20 MiB raw parts. */
(function () {
  'use strict';
  if (window.__aidenAssetParts) return;
  const originalFetch = window.fetch.bind(window);
  const base = new URL('.', document.currentScript.src);
  const manifestURL = new URL('asset-parts.json', base);
  const names = ['index.wasm', 'index.pck'];
  const targets = new Map(names.map(name => [new URL(name, base).href, name]));
  const info = window.__aidenAssetParts = { version: 1, requests: [] };
  let manifestPromise;

  function manifest() {
    if (!manifestPromise) {
      manifestPromise = originalFetch(manifestURL, { cache: 'no-store', credentials: 'same-origin' })
        .then(async response => {
          if (!response.ok) throw new Error('游戏资源清单暂不可用，请稍后重试。');
          const data = await response.json();
          if (data.version !== 1 || !data.assets) throw new Error('游戏资源清单格式错误。');
          for (const name of names) {
            const entry = data.assets[name];
            if (!entry || !Number.isSafeInteger(entry.bytes) || entry.bytes <= 0 || !Array.isArray(entry.parts) || !entry.parts.length) {
              throw new Error('游戏资源清单缺少 ' + name);
            }
            let total = 0;
            for (const part of entry.parts) {
              const url = new URL(part.url, base);
              if (url.origin !== base.origin || !url.pathname.startsWith(base.pathname) || !Number.isSafeInteger(part.bytes) || part.bytes <= 0 || part.bytes > 20 * 1024 * 1024) {
                throw new Error('游戏资源分片配置错误。');
              }
              total += part.bytes;
            }
            if (total !== entry.bytes) throw new Error('游戏资源分片大小不匹配。');
            if (entry.gzip) {
              const compressedURL = new URL(entry.gzip.url, base);
              if (name !== 'index.wasm' || entry.gzip.encoding !== 'gzip' || entry.gzip.uncompressedBytes !== entry.bytes ||
                  !Number.isSafeInteger(entry.gzip.bytes) || entry.gzip.bytes <= 0 || entry.gzip.bytes > 20 * 1024 * 1024 ||
                  compressedURL.origin !== base.origin || !compressedURL.pathname.startsWith(base.pathname)) {
                throw new Error('压缩游戏资源配置错误。');
              }
            }
          }
          return data;
        });
    }
    return manifestPromise;
  }

  function notifyFailure(error) {
    if (error.name !== 'AbortError' && typeof window.dispatchEvent === 'function' && typeof CustomEvent === 'function') {
      window.dispatchEvent(new CustomEvent('aiden-asset-error', { detail: String(error.message || error) }));
    }
  }

  async function gzipResponse(entry, name, request, decoder) {
    const response = await originalFetch(new URL(entry.gzip.url, base), {
      method: 'GET', credentials: request.credentials, cache: request.cache,
      mode: 'same-origin', signal: request.signal
    });
    if (!response.ok || !response.body) {
      if (response.body) await response.body.cancel().catch(() => {});
      return joinedResponse(entry, name, request);
    }
    const reader = response.body.pipeThrough(decoder).getReader();
    const state = { name, total: entry.bytes, loaded: 0, parts: 1, completedParts: 0, status: 'loading',
      transport: 'gzip', compressedBytes: entry.gzip.bytes };
    info.requests.push(state);
    if (info.requests.length > 12) info.requests.shift();
    const stream = new ReadableStream({
      async pull(output) {
        try {
          const chunk = await reader.read();
          if (chunk.done) {
            if (state.loaded !== entry.bytes) throw new Error(name + ' 解压后长度不完整。');
            state.completedParts = 1;
            state.status = 'done';
            reader.releaseLock();
            output.close();
            return;
          }
          state.loaded += chunk.value.byteLength;
          if (state.loaded > entry.bytes) throw new Error(name + ' 解压后长度超出清单。');
          output.enqueue(chunk.value);
        } catch (error) {
          state.status = error.name === 'AbortError' ? 'aborted' : 'error';
          state.error = String(error.message || error);
          notifyFailure(error);
          output.error(error);
        }
      },
      async cancel(reason) {
        state.status = 'aborted';
        await reader.cancel(reason).catch(() => {});
      }
    });
    return new Response(stream, { status: 200,
      headers: { 'Content-Type': entry.contentType, 'Content-Length': String(entry.bytes) } });
  }

  function joinedResponse(entry, name, request) {
    const controller = new AbortController();
    const state = { name, total: entry.bytes, loaded: 0, parts: entry.parts.length, completedParts: 0, status: 'loading', transport: 'parts' };
    info.requests.push(state);
    if (info.requests.length > 12) info.requests.shift();
    let index = 0;
    let reader = null;
    let partBytes = 0;
    let stopped = false;
    const onAbort = () => controller.abort(request.signal.reason);
    request.signal.addEventListener('abort', onAbort, { once: true });
    function finish() {
      stopped = true;
      request.signal.removeEventListener('abort', onAbort);
    }
    const stream = new ReadableStream({
      async pull(output) {
        if (stopped) return;
        try {
          while (true) {
            if (controller.signal.aborted) throw new DOMException('资源下载已取消。', 'AbortError');
            if (index === entry.parts.length) {
              if (state.loaded !== entry.bytes) throw new Error(name + ' 下载长度不完整。');
              state.status = 'done';
              finish();
              output.close();
              return;
            }
            const part = entry.parts[index];
            if (!reader) {
              const response = await originalFetch(new URL(part.url, base), {
                method: 'GET', credentials: request.credentials, cache: request.cache,
                mode: 'same-origin', signal: controller.signal
              });
              if (!response.ok) throw new Error(name + ' 分片 ' + (index + 1) + ' 下载失败，请稍后重试。');
              if (!response.body) {
                const bytes = new Uint8Array(await response.arrayBuffer());
                if (bytes.byteLength !== part.bytes) throw new Error(name + ' 分片长度不完整。');
                state.loaded += bytes.byteLength;
                state.completedParts = ++index;
                output.enqueue(bytes);
                return;
              }
              reader = response.body.getReader();
              partBytes = 0;
            }
            const chunk = await reader.read();
            if (chunk.done) {
              reader.releaseLock();
              reader = null;
              if (partBytes !== part.bytes) throw new Error(name + ' 分片长度不完整。');
              state.completedParts = ++index;
              continue;
            }
            partBytes += chunk.value.byteLength;
            state.loaded += chunk.value.byteLength;
            if (partBytes > part.bytes || state.loaded > entry.bytes) throw new Error(name + ' 分片长度超出清单。');
            output.enqueue(chunk.value);
            return;
          }
        } catch (error) {
          state.status = error.name === 'AbortError' ? 'aborted' : 'error';
          state.error = String(error.message || error);
          notifyFailure(error);
          controller.abort();
          finish();
          output.error(error);
        }
      },
      async cancel(reason) {
        state.status = 'aborted';
        controller.abort(reason);
        if (reader) await reader.cancel(reason).catch(() => {});
        finish();
      }
    });
    return new Response(stream, {
      status: 200,
      headers: { 'Content-Type': entry.contentType, 'Content-Length': String(entry.bytes) }
    });
  }

  window.fetch = async function (input, options) {
    const inputURL = input instanceof Request ? input.url : String(input);
    const url = new URL(inputURL, document.baseURI);
    url.search = '';
    url.hash = '';
    const name = targets.get(url.href);
    if (!name) return originalFetch(input, options);
    const request = new Request(input instanceof Request ? input : new URL(inputURL, document.baseURI), options);
    if (request.method !== 'GET' && request.method !== 'HEAD') return originalFetch(input, options);
    if (request.signal.aborted) throw new DOMException('资源下载已取消。', 'AbortError');
    const data = await manifest();
    if (request.signal.aborted) throw new DOMException('资源下载已取消。', 'AbortError');
    const entry = data.assets[name];
    if (request.method === 'HEAD') {
      return new Response(null, { status: 200, headers: { 'Content-Type': entry.contentType, 'Content-Length': String(entry.bytes) } });
    }
    if (entry.gzip && typeof DecompressionStream === 'function') {
      let decoder;
      try { decoder = new DecompressionStream('gzip'); } catch (_) { /* Keep the raw-part fallback. */ }
      if (decoder) return gzipResponse(entry, name, request, decoder);
    }
    return joinedResponse(entry, name, request);
  };
}());
