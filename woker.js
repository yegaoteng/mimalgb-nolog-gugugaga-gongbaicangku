export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // 1. 接收 GitHub Action 的回调
    if (url.pathname === '/github_callback' && request.method === 'POST') {
      try {
        const formData = await request.formData();
        const taskId = formData.get('task_id');
        const status = formData.get('status');
        
        if (status === 'success') {
          const jarFile = formData.get('jar_file'); 
          if (jarFile) {
            const arrayBuffer = await jarFile.arrayBuffer();
            await env.BUILD_KV.put(`${taskId}_jar`, arrayBuffer, { expirationTtl: 3600 });
          }
          await env.BUILD_KV.put(`${taskId}_status`, JSON.stringify({ status: 'success' }), { expirationTtl: 3600 });
        } else {
          const errorLog = formData.get('error_log') || '未知错误';
          await env.BUILD_KV.put(`${taskId}_status`, JSON.stringify({ status: 'failed', log: errorLog }), { expirationTtl: 3600 });
        }
        return new Response("ok");
      } catch (e) {
        return new Response("Error parsing form data: " + e.message, { status: 500 });
      }
    }

    // 2. 前端轮询构建状态
    if (url.pathname === '/check_build' && request.method === 'GET') {
      const taskId = url.searchParams.get('task_id');
      const statusStr = await env.BUILD_KV.get(`${taskId}_status`);
      
      if (!statusStr) return new Response(JSON.stringify({ status: 'building' }), { headers: { 'Content-Type': 'application/json' } });
      
      const statusData = JSON.parse(statusStr);
      if (statusData.status === 'success') {
        return new Response(JSON.stringify({ status: 'success', file: `${taskId}.jar` }), { headers: { 'Content-Type': 'application/json' } });
      } else {
        return new Response(JSON.stringify({ status: 'error', log: statusData.log }), { headers: { 'Content-Type': 'application/json' } });
      }
    }

    // 3. 下载 Jar
    if (url.pathname === '/download' && request.method === 'GET') {
      const taskId = url.searchParams.get('task_id');
      const jarBuffer = await env.BUILD_KV.get(`${taskId}_jar`, { type: 'arrayBuffer' });
      if (!jarBuffer) return new Response("文件不存在或已过期", { status: 404 });
      
      return new Response(jarBuffer, {
        headers: {
          'Content-Type': 'application/java-archive',
          'Content-Disposition': `attachment; filename="${taskId}.jar"`
        }
      });
    }

    // 4. AI 对话路由
    if (url.pathname === '/chat' && request.method === 'POST') {
      const body = await request.json();
      return handleChat(body, env, request);
    }

    // 5. 返回前端 HTML
    return new Response(HTML_CONTENT, {
      headers: { 'Content-Type': 'text/html; charset=utf-8' },
    });
  }
};

// ================== AI 核心逻辑 ==================
async function handleChat(body, env, request) {
  const messages = body.messages || [];
  const configIndex = body.configIndex || 0;
  
  const AI_CONFIGS = [
    { name: 'Agents-A1', endpoint: 'https://discovery-api.intern-ai.org.cn/v1/chat/completions', token: 'sk-995826aecd7cceaf5ce748f695d385a468ff307c788bc1480c5b41ce4c14aef4', model: 'Agents-A1' },
    { name: 'Sage-A1.5', endpoint: 'https://discovery-api.intern-ai.org.cn/v1/chat/completions', token: 'sk-995826aecd7cceaf5ce748f695d385a468ff307c788bc1480c5b41ce4c14aef4', model: 'Sage-A1.5' },
    { name: 'Agent-3.0-flash', endpoint: 'https://apihub.agnes-ai.com/v1/chat/completions', token: 'sk-xeRTHrfT2jxzLENYJXKVtltCoX2Q2RSfVoZykvIOFdMaiDKg', model: 'agnes-3.0-flash' }
  ];

  const cfg = AI_CONFIGS[configIndex] || AI_CONFIGS[0];
  const systemPrompt = {
    role: 'system',
    content: '你是一个专业的 Minecraft 模组开发助手。\n你可以使用工具 package_mod_project 将代码打包并提交云端编译。\n【极度重要】：直接一次性把所有代码放入工具，不要分批！如果遇到构建失败，请根据日志修复代码中的 import 报错等。'
  };
  
  const cleanMessages = [systemPrompt, ...messages.map(m => ({ role: m.role, content: m.content }))];

  const tools = [{
    type: 'function',
    function: {
      name: 'package_mod_project',
      description: '提交代码到 GitHub 构建',
      parameters: {
        type: 'object',
        properties: {
          project_files: {
            type: 'array',
            items: { type: 'object', properties: { path: { type: 'string' }, content: { type: 'string' } }, required: ['path', 'content'] }
          },
          mod_name: { type: 'string' }
        },
        required: ['project_files', 'mod_name']
      }
    }
  }];

  const aiReq = {
    model: cfg.model,
    messages: cleanMessages,
    stream: true,
    tools: tools,
    tool_choice: 'auto'
  };

  const aiRes = await fetch(cfg.endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${cfg.token}` },
    body: JSON.stringify(aiReq)
  });

  if (!aiRes.ok) {
    return new Response(`event: error\ndata: ${JSON.stringify({message: `API Error: ${aiRes.status}`})}\n\n`, { headers: { 'Content-Type': 'text/event-stream' } });
  }

  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();
  const encoder = new TextEncoder();

  processStream(aiRes.body, writer, encoder, env, cfg, request).catch(console.error);

  return new Response(readable, {
    headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive' }
  });
}

async function processStream(stream, writer, encoder, env, cfg, request) {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let content = '';
  let toolCalls = {};
  
  const writeEvent = (event, data) => {
    writer.write(encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`));
  };

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      
      buffer += decoder.decode(value, { stream: true });
      let lines = buffer.split('\n');
      buffer = lines.pop();

      for (const line of lines) {
        if (!line.startsWith('data: ') || line === 'data: [DONE]') continue;
        try {
          const data = JSON.parse(line.slice(6));
          const delta = data.choices?.[0]?.delta;
          if (!delta) continue;

          if (delta.reasoning_content) writeEvent('reasoning', { text: delta.reasoning_content });
          if (delta.content) {
            content += delta.content;
            writeEvent('delta', { text: delta.content });
          }

          if (delta.tool_calls) {
            for (const tc of delta.tool_calls) {
              const idx = tc.index;
              if (!toolCalls[idx]) toolCalls[idx] = { id: tc.id || '', name: '', arguments: '' };
              if (tc.id) toolCalls[idx].id = tc.id;
              if (tc.function?.name) toolCalls[idx].name += tc.function.name;
              if (tc.function?.arguments) toolCalls[idx].arguments += tc.function.arguments;
            }
          }
        } catch (e) { }
      }
    }

    if (Object.keys(toolCalls).length > 0) {
      writeEvent('status', { text: '正在将源码提交至 GitHub Actions…' });
      
      const tc = toolCalls[0];
      const args = JSON.parse(tc.arguments);
      const taskId = 'task_' + Date.now().toString(36) + Math.random().toString(36).substring(2);
      
      try {
        const callbackUrl = new URL('/github_callback', request.url).toString();
        const payload = {
          event_type: 'build_mod',
          client_payload: {
            task_id: taskId,
            mod_name: args.mod_name || 'mcmod',
            project_zip_base64: btoa(unescape(encodeURIComponent(JSON.stringify(args.project_files)))), 
            callback_url: callbackUrl
          }
        };

        const ghRes = await fetch(`https://api.github.com/repos/${env.GITHUB_REPO}/dispatches`, {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${env.GITHUB_TOKEN}`,
            'Accept': 'application/vnd.github+json',
            'User-Agent': 'Cloudflare-Worker'
          },
          body: JSON.stringify(payload)
        });

        if (ghRes.status === 204) {
          writeEvent('build_task', { task_id: taskId });
        } else {
          const errData = await ghRes.text();
          writeEvent('error', { message: `GitHub 提交失败: ${ghRes.status} - ${errData}` });
        }
      } catch (err) {
        writeEvent('error', { message: `系统错误: ${err.message}` });
      }
    }
  } catch (e) {
    console.error(e);
  } finally {
    writeEvent('done', {});
    writer.close();
  }
}

// ================== 前端 HTML 内容 ==================
const HTML_CONTENT = `
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<title>AI 云端模组构建</title>
<style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    html, body { height: 100%; overflow: hidden; background: #f7f7f9; }
    body { font-family: -apple-system, sans-serif; display: flex; flex-direction: column; color: #222; height: 100dvh; width: 100vw; }
    header { background: #fff; border-bottom: 1px solid #ececec; padding: 14px 20px; font-size: 16px; font-weight: 600; display: flex; justify-content: space-between; align-items: center; flex-shrink: 0; }
    #new-chat-btn { background: #f0f2f5; color: #333; border: 1px solid #ddd; padding: 6px 12px; border-radius: 6px; font-size: 13px; font-weight: 500; cursor: pointer; }
    #chat { flex: 1; overflow-y: auto; padding: 16px 12px 24px; display: flex; flex-direction: column; gap: 12px; width: 100%; max-width: 1000px; margin: 0 auto; }
    .msg { line-height: 1.65; word-break: break-word; font-size: 15px; }
    .msg.user { max-width: 85%; align-self: flex-end; background: #007aff; color: #fff; padding: 10px 14px; border-radius: 14px; border-bottom-right-radius: 4px; white-space: pre-wrap; }
    .msg.assistant { align-self: flex-start; background: #fff; color: #222; padding: 14px 16px; border-radius: 14px; border: 1px solid #ececec; border-bottom-left-radius: 4px; width: 100%; }
    .msg.error { align-self: center; background: #fff2f2; color: #c62828; border: 1px solid #ffd4d4; font-size: 13px; padding: 10px 14px; border-radius: 8px; }
    
    .reasoning-box { align-self: flex-start; width: 100%; margin-bottom: 8px; background: #f8f9fa; border: 1px solid #d0d7de; border-radius: 8px; overflow: hidden; flex-shrink: 0; }
    .reasoning-header { padding: 12px 14px; cursor: pointer; display: flex; align-items: center; gap: 6px; font-size: 13.5px; color: #555; font-weight: 500; user-select: none; background: #f8f9fa; }
    .reasoning-arrow { font-size: 10px; transition: transform 0.3s ease; }
    .reasoning-box.collapsed .reasoning-arrow { transform: rotate(-90deg); }
    .reasoning-body-wrapper { display: grid; grid-template-rows: 1fr; transition: grid-template-rows 0.3s ease-out; }
    .reasoning-box.collapsed .reasoning-body-wrapper { grid-template-rows: 0fr; }
    .reasoning-content-inner { overflow: hidden; min-height: 0; }
    .reasoning-content { padding: 0 14px 14px; font-size: 13.5px; color: #666; white-space: pre-wrap; max-height: 300px; overflow-y: auto; border-top: 1px dashed #d0d7de; margin-top: 2px; padding-top: 10px; }
    .reasoning-step { display: block; margin-bottom: 6px; color: #888; font-size: 13px; }
    .reasoning-box.collapsed .reasoning-content { border-top-color: transparent; }

    .file-cards-container { margin-top: 14px; padding-top: 14px; border-top: 1px dashed #e0e2e6; display: flex; flex-direction: column; gap: 8px; }
    .file-card-inline { display: flex; align-items: center; gap: 10px; background: #f8f9fa; border: 1px solid #e9ecef; border-radius: 10px; padding: 10px 14px; font-size: 14px; white-space: normal; }
    .file-icon { font-size: 18px; line-height: 1; }
    .file-name { font-weight: 500; color: #222; }
    .file-download { margin-left: auto; color: #007aff; text-decoration: none; font-weight: 600; font-size: 13px; padding: 4px 10px; border-radius: 8px; background: #eef4ff; white-space: nowrap; }

    .markdown { white-space: normal; }
    .markdown > *:first-child { margin-top: 0; }
    .markdown > *:last-child  { margin-bottom: 0; }
    .markdown p { margin: 0 0 10px; }
    .markdown code { background: #eef4ff; color: #007aff; padding: 2px 6px; border-radius: 4px; font-family: monospace; font-size: 0.9em; }
    .markdown pre { margin: 14px 0; border: 1px solid #e0e2e6; border-radius: 8px; background: #fafafa; padding: 14px; overflow-x: auto; }
    .markdown pre code { background: none; padding: 0; color: #333; font-size: 0.88em; line-height: 1.55; }

    #composer { background: #fff; border-top: 1px solid #ececec; padding: 12px 12px calc(16px + env(safe-area-inset-bottom)); flex-shrink: 0; }
    .model-selector-wrapper { max-width: 1000px; margin: 0 auto 8px auto; }
    #model-select { padding: 6px 28px 6px 12px; border: 1px solid #ddd; border-radius: 8px; background-color: #fafafa; font-size: 13px; color: #333; outline: none; cursor: pointer; appearance: none; background-image: url("data:image/svg+xml;charset=US-ASCII,%3Csvg%20xmlns%3D%22http%3A%2F%2Fwww.w3.org%2F2000%2Fsvg%22%20width%3D%22292.4%22%20height%3D%22292.4%22%3E%3Cpath%20fill%3D%22%23666%22%20d%3D%22M287%2069.4a17.6%2017.6%200%200%200-13-5.4H18.4c-5%200-9.3%201.8-12.9%205.4A17.6%2017.6%200%200%200%200%2082.2c0%205%201.8%209.3%205.4%2012.9l128%20127.9c3.6%203.6%207.8%205.4%2012.8%205.4s9.2-1.8%2012.8-5.4L287%2095c3.5-3.5%205.4-7.8%205.4-12.8%200-5-1.9-9.2-5.5-12.8z%22%2F%3E%3C%2Fsvg%3E"); background-repeat: no-repeat; background-position: right 10px top 50%; background-size: 10px auto; }
    .composer-inner { max-width: 1000px; margin: 0 auto; display: flex; gap: 10px; align-items: flex-end; }
    textarea { flex: 1; resize: none; border: 1px solid #ddd; border-radius: 12px; padding: 11px 14px; font-size: 15px; outline: none; min-height: 46px; max-height: 120px; background: #fafafa; }
    button.send-btn { background: #007aff; color: #fff; border: none; border-radius: 12px; padding: 0 20px; font-size: 15px; font-weight: 600; cursor: pointer; height: 46px; white-space: nowrap; }
    button.send-btn.busy { background: #e5e5ea; color: #8e8e93; cursor: not-allowed; }
</style>
</head>
<body>
<header>
    <div class="title">AI 助手 (Cloudflare)</div>
    <button id="new-chat-btn">+ 新对话</button>
</header>
<div id="chat"></div>
<div id="composer">
    <div class="model-selector-wrapper">
        <select id="model-select">
            <option value="0">Agents-A1</option>
            <option value="1">Sage-A1.5</option>
            <option value="2">Agent-3.0-flash</option>
        </select>
    </div>
    <div class="composer-inner">
        <textarea id="input" rows="1" placeholder="发消息给 AI 助手…"></textarea>
        <button id="send" class="send-btn">发送</button>
    </div>
</div>

<script src="https://cdn.bootcdn.net/ajax/libs/marked/4.3.0/marked.min.js"></script>
<script>
const chatEl  = document.getElementById('chat');
const inputEl = document.getElementById('input');
const sendBtn = document.getElementById('send');
const modelSelect = document.getElementById('model-select');
const newChatBtn = document.getElementById('new-chat-btn');

let apiHistory = [];
let displayHistory = [];
let busy = false;
let globalTimers = {}; 

function scrollBottom(force) {
    if (force || chatEl.scrollHeight - chatEl.scrollTop - chatEl.clientHeight < 60) {
        chatEl.scrollTop = chatEl.scrollHeight;
    }
}
function escapeHtml(s) {
    return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}
function renderMarkdown(text) {
    if (typeof marked === 'undefined') return escapeHtml(text).replace(/\\n/g, '<br>');
    return marked.parse(text, { breaks: true, gfm: true });
}
function processCodeBlocks(container) {
    if (!container) return;
    container.querySelectorAll('pre').forEach(pre => {
        if (pre.parentNode.classList.contains('code-block-wrapper')) return;
        const code = pre.querySelector('code');
        if (!code) return;
        const wrapper = document.createElement('div');
        wrapper.className = 'code-block-wrapper';
        wrapper.style = "border:1px solid #e0e2e6; border-radius:8px; margin:14px 0; overflow:hidden;";
        const header = document.createElement('div');
        header.style = "display:flex; justify-content:space-between; padding:6px 12px; background:#f1f3f5; font-size:12px; border-bottom:1px solid #e0e2e6;";
        header.innerHTML = '<span>代码</span>';
        const copyBtn = document.createElement('button');
        copyBtn.textContent = '复制';
        copyBtn.style = "background:transparent; border:none; color:#007aff; cursor:pointer;";
        copyBtn.onclick = () => { navigator.clipboard.writeText(code.textContent).then(() => { copyBtn.textContent = '已复制'; setTimeout(() => copyBtn.textContent = '复制', 2000); }); };
        header.appendChild(copyBtn);
        pre.parentNode.insertBefore(wrapper, pre);
        wrapper.appendChild(header);
        wrapper.appendChild(pre);
    });
}

function loadSession() {
    const saved = localStorage.getItem('ai_chat_session_v5');
    if (saved) {
        try {
            const data = JSON.parse(saved);
            apiHistory = data.apiHistory || [];
            displayHistory = data.displayHistory || [];
            renderHistory();
        } catch (e) {}
    }
}
function saveSession() {
    localStorage.setItem('ai_chat_session_v5', JSON.stringify({ apiHistory, displayHistory }));
}

function renderHistory() {
    chatEl.innerHTML = '';
    displayHistory.forEach(item => {
        if (item.role === 'user') {
            const ub = document.createElement('div'); ub.className = 'msg user'; ub.textContent = item.content; chatEl.appendChild(ub);
        } else if (item.role === 'assistant') {
            if (item.reasoning) {
                const rb = document.createElement('div'); rb.className = 'msg reasoning-box collapsed'; 
                rb.innerHTML = '<div class="reasoning-header"><span class="reasoning-arrow">▼</span><span class="reasoning-header-text">思考过程 (点击展开)</span></div><div class="reasoning-body-wrapper"><div class="reasoning-content-inner"><div class="reasoning-content">' + escapeHtml(item.reasoning) + '</div></div></div>';
                rb.querySelector('.reasoning-header').addEventListener('click', () => rb.classList.toggle('collapsed'));
                chatEl.appendChild(rb);
            }
            if (item.content) {
                const ab = document.createElement('div'); ab.className = 'msg assistant markdown'; ab.innerHTML = renderMarkdown(item.content); processCodeBlocks(ab); chatEl.appendChild(ab);
            }
            if (item.buildStatus) {
                const card = document.createElement('div'); card.className = 'msg assistant';
                if (item.buildStatus === 'building') {
                    card.innerHTML = '<div style="display:flex; align-items:center; gap:8px; color:#007aff; font-weight:500;"><span style="display:inline-block; width:16px; height:16px; border:2px solid #007aff; border-top-color:transparent; border-radius:50%; animation:spin 1s linear infinite;"></span>云端编译中...</div><style>@keyframes spin { to { transform: rotate(360deg); } }</style>';
                    chatEl.appendChild(card);
                    if (!globalTimers[item.buildTaskId]) pollBuildStatus(item.buildTaskId, item, card);
                } else if (item.buildStatus === 'success') {
                    const file = item.files && item.files.length > 0 ? item.files[0] : null;
                    if (file) { card.innerHTML = '<div class="file-cards-container"><div class="file-card-inline"><span class="file-icon">📦</span><span class="file-name">编译成功，点击下载</span><a class="file-download" href="' + file.url + '">下载</a></div></div>'; chatEl.appendChild(card); }
                } else if (item.buildStatus === 'failed') { card.innerHTML = '<span style="color:#c62828;">❌ 编译失败</span>'; chatEl.appendChild(card); }
            }
        }
    });
    scrollBottom(true);
}

newChatBtn.addEventListener('click', () => {
    if (busy) { if(!confirm("确定要新开对话吗？")) return; }
    Object.values(globalTimers).forEach(clearInterval); globalTimers = {};
    localStorage.removeItem('ai_chat_session_v5');
    apiHistory = []; displayHistory = []; chatEl.innerHTML = ''; inputEl.focus();
});

function pollBuildStatus(taskId, msgObj, cardEl) {
    globalTimers[taskId] = setInterval(() => {
        fetch('/check_build?task_id=' + taskId)
            .then(res => res.json())
            .then(data => {
                if (data.status === 'success') {
                    clearInterval(globalTimers[taskId]); delete globalTimers[taskId];
                    msgObj.buildStatus = 'success'; msgObj.files = [{ name: 'Jar', url: '/download?task_id=' + taskId }]; saveSession();
                    cardEl.innerHTML = '<div class="file-cards-container"><div class="file-card-inline"><span class="file-icon">📦</span><span class="file-name">编译成功</span><a class="file-download" href="/download?task_id=' + taskId + '">下载</a></div></div>';
                    scrollBottom(true);
                } else if (data.status === 'error') {
                    clearInterval(globalTimers[taskId]); delete globalTimers[taskId];
                    msgObj.buildStatus = 'failed'; saveSession();
                    cardEl.innerHTML = '<span style="color:red; display:block; margin-bottom:8px;">❌ 编译失败，AI 自动修复中...</span><pre style="font-size:12px; color:#999; background:#f5f5f5; padding:8px; border-radius:6px; overflow-x:auto; max-height:150px;">' + escapeHtml(data.log) + '</pre>';
                    if (busy) return;
                    // 修复点：使用字符串拼接替代模板字符串中的反引号
                    let fixPrompt = "之前的代码在云端编译失败了，报错日志如下：\\n\\n" + "```" + "\\n" + (data.log || "未知错误") + "\\n" + "```" + "\\n\\n请仔细分析错误原因（比如是不是忘记 import 导包了），修改代码后，重新调用 package_mod_project 打包。";
                    inputEl.value = fixPrompt; send();
                }
            }).catch(() => {});
    }, 10000);
}

async function send() {
    const text = inputEl.value.trim();
    if (!text || busy) return;
    inputEl.value = ''; inputEl.style.height = 'auto'; busy = true; sendBtn.disabled = true; sendBtn.classList.add('busy'); sendBtn.textContent = 'AI回答中';

    const ub = document.createElement('div'); ub.className = 'msg user'; ub.textContent = text; chatEl.appendChild(ub);
    apiHistory.push({ role: 'user', content: text }); displayHistory.push({ role: 'user', content: text });
    const currentMsgObj = { role: 'assistant', reasoning: '', content: '', files: [], buildStatus: null };
    displayHistory.push(currentMsgObj); saveSession(); scrollBottom(true);

    let answerDiv = null, currentText = "", reasoningBox = null, reasoningContent = null, userToggledReasoning = false, firstDeltaReceived = false;

    try {
        const res = await fetch('/chat', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ messages: apiHistory, configIndex: parseInt(modelSelect.value, 10) || 0 })
        });
        const reader = res.body.getReader(); const decoder = new TextDecoder(); let sseBuf = '';
        while (true) {
            const { done, value } = await reader.read(); if (done) break;
            sseBuf += decoder.decode(value, { stream: true });
            let idx;
            while ((idx = sseBuf.indexOf('\\n\\n')) !== -1) {
                const rawEvent = sseBuf.slice(0, idx); sseBuf = sseBuf.slice(idx + 2);
                if (!rawEvent.trim()) continue;
                let type = 'message', dataLines = [];
                for (const line of rawEvent.split('\\n')) {
                    if (line.startsWith('event:')) type = line.slice(6).trim();
                    else if (line.startsWith('data:')) dataLines.push(line.slice(5));
                }
                if (!dataLines.length) continue;
                const data = JSON.parse(dataLines.join('\\n'));
                
                if (type === 'reasoning') {
                    if (!reasoningBox) {
                        reasoningBox = document.createElement('div'); reasoningBox.className = 'msg reasoning-box';
                        reasoningBox.innerHTML = '<div class="reasoning-header"><span class="reasoning-arrow">▼</span><span class="reasoning-header-text">正在思考...</span></div><div class="reasoning-body-wrapper"><div class="reasoning-content-inner"><div class="reasoning-content"></div></div></div>';
                        reasoningBox.querySelector('.reasoning-header').addEventListener('click', () => { userToggledReasoning = true; reasoningBox.classList.toggle('collapsed'); });
                        chatEl.appendChild(reasoningBox); reasoningContent = reasoningBox.querySelector('.reasoning-content');
                    }
                    reasoningContent.appendChild(document.createTextNode(data.text)); currentMsgObj.reasoning += data.text;
                    reasoningContent.scrollTop = reasoningContent.scrollHeight; scrollBottom(true); saveSession();
                }
                else if (type === 'delta') {
                    if (!firstDeltaReceived) { firstDeltaReceived = true; if(reasoningBox && !userToggledReasoning) reasoningBox.classList.add('collapsed'); }
                    if (!answerDiv) { answerDiv = document.createElement('div'); answerDiv.className = 'msg assistant markdown'; chatEl.appendChild(answerDiv); }
                    currentText += data.text; currentMsgObj.content = currentText;
                    answerDiv.innerHTML = renderMarkdown(currentText.replace(/^\\s+/, '')); processCodeBlocks(answerDiv); scrollBottom(false); saveSession();
                }
                else if (type === 'status') {
                    if (!reasoningBox) {
                        reasoningBox = document.createElement('div'); reasoningBox.className = 'msg reasoning-box';
                        reasoningBox.innerHTML = '<div class="reasoning-header"><span class="reasoning-arrow">▼</span><span class="reasoning-header-text">正在处理...</span></div><div class="reasoning-body-wrapper"><div class="reasoning-content-inner"><div class="reasoning-content"></div></div></div>';
                        reasoningBox.querySelector('.reasoning-header').addEventListener('click', () => { userToggledReasoning = true; reasoningBox.classList.toggle('collapsed'); });
                        chatEl.appendChild(reasoningBox); reasoningContent = reasoningBox.querySelector('.reasoning-content');
                    }
                    const stepEl = document.createElement('div'); stepEl.className = 'reasoning-step'; stepEl.innerHTML = '⚙️ ' + escapeHtml(data.text); reasoningContent.appendChild(stepEl);
                    reasoningContent.scrollTop = reasoningContent.scrollHeight; scrollBottom(true); saveSession();
                }
                else if (type === 'build_task') {
                    if (reasoningBox) reasoningBox.classList.add('collapsed');
                    currentMsgObj.buildTaskId = data.task_id; currentMsgObj.buildStatus = 'building'; saveSession();
                    const buildCardEl = document.createElement('div'); buildCardEl.className = 'msg assistant';
                    buildCardEl.innerHTML = '<div style="display:flex; align-items:center; gap:8px; color:#007aff; font-weight:500;"><span style="display:inline-block; width:16px; height:16px; border:2px solid #007aff; border-top-color:transparent; border-radius:50%; animation:spin 1s linear infinite;"></span>云端编译中...</div><style>@keyframes spin { to { transform: rotate(360deg); } }</style>';
                    chatEl.appendChild(buildCardEl); scrollBottom(true);
                    pollBuildStatus(data.task_id, currentMsgObj, buildCardEl);
                }
                else if (type === 'error') {
                    const eb = document.createElement('div'); eb.className = 'msg error'; eb.textContent = data.message; chatEl.appendChild(eb); scrollBottom(true);
                }
            }
        }
    } catch (e) {
        const eb = document.createElement('div'); eb.className = 'msg error'; eb.textContent = '网络错误：' + e.message; chatEl.appendChild(eb);
    } finally {
        busy = false; sendBtn.disabled = false; sendBtn.classList.remove('busy'); sendBtn.textContent = '发送'; inputEl.focus(); scrollBottom(false);
    }
}

sendBtn.addEventListener('click', send);
inputEl.addEventListener('keydown', (e) => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send(); } });
loadSession();
</script>
</body>
</html>
`;border-radius: 14px; border: 1px solid #ececec; border-bottom-left-radius: 4px; width: 100%; }
    .msg.error { align-self: center; background: #fff2f2; color: #c62828; border: 1px solid #ffd4d4; font-size: 13px; padding: 10px 14px; border-radius: 8px; }
    
    .reasoning-box { align-self: flex-start; width: 100%; margin-bottom: 8px; background: #f8f9fa; border: 1px solid #d0d7de; border-radius: 8px; overflow: hidden; flex-shrink: 0; }
    .reasoning-header { padding: 12px 14px; cursor: pointer; display: flex; align-items: center; gap: 6px; font-size: 13.5px; color: #555; font-weight: 500; user-select: none; background: #f8f9fa; }
    .reasoning-arrow { font-size: 10px; transition: transform 0.3s ease; }
    .reasoning-box.collapsed .reasoning-arrow { transform: rotate(-90deg); }
    .reasoning-body-wrapper { display: grid; grid-template-rows: 1fr; transition: grid-template-rows 0.3s ease-out; }
    .reasoning-box.collapsed .reasoning-body-wrapper { grid-template-rows: 0fr; }
    .reasoning-content-inner { overflow: hidden; min-height: 0; }
    .reasoning-content { padding: 0 14px 14px; font-size: 13.5px; color: #666; white-space: pre-wrap; max-height: 300px; overflow-y: auto; border-top: 1px dashed #d0d7de; margin-top: 2px; padding-top: 10px; }
    .reasoning-step { display: block; margin-bottom: 6px; color: #888; font-size: 13px; }
    .reasoning-box.collapsed .reasoning-content { border-top-color: transparent; }

    .file-cards-container { margin-top: 14px; padding-top: 14px; border-top: 1px dashed #e0e2e6; display: flex; flex-direction: column; gap: 8px; }
    .file-card-inline { display: flex; align-items: center; gap: 10px; background: #f8f9fa; border: 1px solid #e9ecef; border-radius: 10px; padding: 10px 14px; font-size: 14px; white-space: normal; }
    .file-icon { font-size: 18px; line-height: 1; }
    .file-name { font-weight: 500; color: #222; }
    .file-download { margin-left: auto; color: #007aff; text-decoration: none; font-weight: 600; font-size: 13px; padding: 4px 10px; border-radius: 8px; background: #eef4ff; white-space: nowrap; }

    .markdown { white-space: normal; }
    .markdown > *:first-child { margin-top: 0; }
    .markdown > *:last-child  { margin-bottom: 0; }
    .markdown p { margin: 0 0 10px; }
    .markdown code { background: #eef4ff; color: #007aff; padding: 2px 6px; border-radius: 4px; font-family: monospace; font-size: 0.9em; }
    .markdown pre { margin: 14px 0; border: 1px solid #e0e2e6; border-radius: 8px; background: #fafafa; padding: 14px; overflow-x: auto; }
    .markdown pre code { background: none; padding: 0; color: #333; font-size: 0.88em; line-height: 1.55; }

    #composer { background: #fff; border-top: 1px solid #ececec; padding: 12px 12px calc(16px + env(safe-area-inset-bottom)); flex-shrink: 0; }
    .model-selector-wrapper { max-width: 1000px; margin: 0 auto 8px auto; }
    #model-select { padding: 6px 28px 6px 12px; border: 1px solid #ddd; border-radius: 8px; background-color: #fafafa; font-size: 13px; color: #333; outline: none; cursor: pointer; appearance: none; background-image: url("data:image/svg+xml;charset=US-ASCII,%3Csvg%20xmlns%3D%22http%3A%2F%2Fwww.w3.org%2F2000%2Fsvg%22%20width%3D%22292.4%22%20height%3D%22292.4%22%3E%3Cpath%20fill%3D%22%23666%22%20d%3D%22M287%2069.4a17.6%2017.6%200%200%200-13-5.4H18.4c-5%200-9.3%201.8-12.9%205.4A17.6%2017.6%200%200%200%200%2082.2c0%205%201.8%209.3%205.4%2012.9l128%20127.9c3.6%203.6%207.8%205.4%2012.8%205.4s9.2-1.8%2012.8-5.4L287%2095c3.5-3.5%205.4-7.8%205.4-12.8%200-5-1.9-9.2-5.5-12.8z%22%2F%3E%3C%2Fsvg%3E"); background-repeat: no-repeat; background-position: right 10px top 50%; background-size: 10px auto; }
    .composer-inner { max-width: 1000px; margin: 0 auto; display: flex; gap: 10px; align-items: flex-end; }
    textarea { flex: 1; resize: none; border: 1px solid #ddd; border-radius: 12px; padding: 11px 14px; font-size: 15px; outline: none; min-height: 46px; max-height: 120px; background: #fafafa; }
    button.send-btn { background: #007aff; color: #fff; border: none; border-radius: 12px; padding: 0 20px; font-size: 15px; font-weight: 600; cursor: pointer; height: 46px; white-space: nowrap; }
    button.send-btn.busy { background: #e5e5ea; color: #8e8e93; cursor: not-allowed; }
</style>
</head>
<body>
<header>
    <div class="title">AI 助手 (Cloudflare)</div>
    <button id="new-chat-btn">+ 新对话</button>
</header>
<div id="chat"></div>
<div id="composer">
    <div class="model-selector-wrapper">
        <select id="model-select">
            <option value="0">Agents-A1</option>
            <option value="1">Sage-A1.5</option>
            <option value="2">Agent-3.0-flash</option>
        </select>
    </div>
    <div class="composer-inner">
        <textarea id="input" rows="1" placeholder="发消息给 AI 助手…"></textarea>
        <button id="send" class="send-btn">发送</button>
    </div>
</div>

<script src="https://cdn.bootcdn.net/ajax/libs/marked/4.3.0/marked.min.js"></script>
<script>
const chatEl  = document.getElementById('chat');
const inputEl = document.getElementById('input');
const sendBtn = document.getElementById('send');
const modelSelect = document.getElementById('model-select');
const newChatBtn = document.getElementById('new-chat-btn');

let apiHistory = [];
let displayHistory = [];
let busy = false;
let globalTimers = {}; 

function scrollBottom(force) {
    if (force || chatEl.scrollHeight - chatEl.scrollTop - chatEl.clientHeight < 60) {
        chatEl.scrollTop = chatEl.scrollHeight;
    }
}
function escapeHtml(s) {
    return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}
function renderMarkdown(text) {
    if (typeof marked === 'undefined') return escapeHtml(text).replace(/\\n/g, '<br>');
    return marked.parse(text, { breaks: true, gfm: true });
}
function processCodeBlocks(container) {
    if (!container) return;
    container.querySelectorAll('pre').forEach(pre => {
        if (pre.parentNode.classList.contains('code-block-wrapper')) return;
        const code = pre.querySelector('code');
        if (!code) return;
        const wrapper = document.createElement('div');
        wrapper.className = 'code-block-wrapper';
        wrapper.style = "border:1px solid #e0e2e6; border-radius:8px; margin:14px 0; overflow:hidden;";
        const header = document.createElement('div');
        header.style = "display:flex; justify-content:space-between; padding:6px 12px; background:#f1f3f5; font-size:12px; border-bottom:1px solid #e0e2e6;";
        header.innerHTML = '<span>代码</span>';
        const copyBtn = document.createElement('button');
        copyBtn.textContent = '复制';
        copyBtn.style = "background:transparent; border:none; color:#007aff; cursor:pointer;";
        copyBtn.onclick = () => { navigator.clipboard.writeText(code.textContent).then(() => { copyBtn.textContent = '已复制'; setTimeout(() => copyBtn.textContent = '复制', 2000); }); };
        header.appendChild(copyBtn);
        pre.parentNode.insertBefore(wrapper, pre);
        wrapper.appendChild(header);
        wrapper.appendChild(pre);
    });
}

function loadSession() {
    const saved = localStorage.getItem('ai_chat_session_v5');
    if (saved) {
        try {
            const data = JSON.parse(saved);
            apiHistory = data.apiHistory || [];
            displayHistory = data.displayHistory || [];
            renderHistory();
        } catch (e) {}
    }
}
function saveSession() {
    localStorage.setItem('ai_chat_session_v5', JSON.stringify({ apiHistory, displayHistory }));
}

function renderHistory() {
    chatEl.innerHTML = '';
    displayHistory.forEach(item => {
        if (item.role === 'user') {
            const ub = document.createElement('div'); ub.className = 'msg user'; ub.textContent = item.content; chatEl.appendChild(ub);
        } else if (item.role === 'assistant') {
            if (item.reasoning) {
                const rb = document.createElement('div'); rb.className = 'msg reasoning-box collapsed'; 
                rb.innerHTML = '<div class="reasoning-header"><span class="reasoning-arrow">▼</span><span class="reasoning-header-text">思考过程 (点击展开)</span></div><div class="reasoning-body-wrapper"><div class="reasoning-content-inner"><div class="reasoning-content">' + escapeHtml(item.reasoning) + '</div></div></div>';
                rb.querySelector('.reasoning-header').addEventListener('click', () => rb.classList.toggle('collapsed'));
                chatEl.appendChild(rb);
            }
            if (item.content) {
                const ab = document.createElement('div'); ab.className = 'msg assistant markdown'; ab.innerHTML = renderMarkdown(item.content); processCodeBlocks(ab); chatEl.appendChild(ab);
            }
            if (item.buildStatus) {
                const card = document.createElement('div'); card.className = 'msg assistant';
                if (item.buildStatus === 'building') {
                    card.innerHTML = '<div style="display:flex; align-items:center; gap:8px; color:#007aff; font-weight:500;"><span style="display:inline-block; width:16px; height:16px; border:2px solid #007aff; border-top-color:transparent; border-radius:50%; animation:spin 1s linear infinite;"></span>云端编译中...</div><style>@keyframes spin { to { transform: rotate(360deg); } }</style>';
                    chatEl.appendChild(card);
                    if (!globalTimers[item.buildTaskId]) pollBuildStatus(item.buildTaskId, item, card);
                } else if (item.buildStatus === 'success') {
                    const file = item.files && item.files.length > 0 ? item.files[0] : null;
                    if (file) { card.innerHTML = '<div class="file-cards-container"><div class="file-card-inline"><span class="file-icon">📦</span><span class="file-name">编译成功，点击下载</span><a class="file-download" href="' + file.url + '">下载</a></div></div>'; chatEl.appendChild(card); }
                } else if (item.buildStatus === 'failed') { card.innerHTML = '<span style="color:#c62828;">❌ 编译失败</span>'; chatEl.appendChild(card); }
            }
        }
    });
    scrollBottom(true);
}

newChatBtn.addEventListener('click', () => {
    if (busy) { if(!confirm("确定要新开对话吗？")) return; }
    Object.values(globalTimers).forEach(clearInterval); globalTimers = {};
    localStorage.removeItem('ai_chat_session_v5');
    apiHistory = []; displayHistory = []; chatEl.innerHTML = ''; inputEl.focus();
});

function pollBuildStatus(taskId, msgObj, cardEl) {
    globalTimers[taskId] = setInterval(() => {
        fetch('/check_build?task_id=' + taskId)
            .then(res => res.json())
            .then(data => {
                if (data.status === 'success') {
                    clearInterval(globalTimers[taskId]); delete globalTimers[taskId];
                    msgObj.buildStatus = 'success'; msgObj.files = [{ name: 'Jar', url: '/download?task_id=' + taskId }]; saveSession();
                    cardEl.innerHTML = '<div class="file-cards-container"><div class="file-card-inline"><span class="file-icon">📦</span><span class="file-name">编译成功</span><a class="file-download" href="/download?task_id=' + taskId + '">下载</a></div></div>';
                    scrollBottom(true);
                } else if (data.status === 'error') {
                    clearInterval(globalTimers[taskId]); delete globalTimers[taskId];
                    msgObj.buildStatus = 'failed'; saveSession();
                    cardEl.innerHTML = '<span style="color:red; display:block; margin-bottom:8px;">❌ 编译失败，AI 自动修复中...</span><pre style="font-size:12px; color:#999; background:#f5f5f5; padding:8px; border-radius:6px; overflow-x:auto; max-height:150px;">' + escapeHtml(data.log) + '</pre>';
                    if (busy) return;
                    let fixPrompt = "之前的代码在云端编译失败了，报错日志如下：\\n\\n\\\`\\\`\\\`\\n" + (data.log || "未知错误") + "\\n\\\`\\\`\\\`\\n\\n请仔细分析错误原因（比如是不是忘记 import 导包了），修改代码后，重新调用 package_mod_project 打包。";
                    inputEl.value = fixPrompt; send();
                }
            }).catch(() => {});
    }, 10000);
}

async function send() {
    const text = inputEl.value.trim();
    if (!text || busy) return;
    inputEl.value = ''; inputEl.style.height = 'auto'; busy = true; sendBtn.disabled = true; sendBtn.classList.add('busy'); sendBtn.textContent = 'AI回答中';

    const ub = document.createElement('div'); ub.className = 'msg user'; ub.textContent = text; chatEl.appendChild(ub);
    apiHistory.push({ role: 'user', content: text }); displayHistory.push({ role: 'user', content: text });
    const currentMsgObj = { role: 'assistant', reasoning: '', content: '', files: [], buildStatus: null };
    displayHistory.push(currentMsgObj); saveSession(); scrollBottom(true);

    let answerDiv = null, currentText = "", reasoningBox = null, reasoningContent = null, userToggledReasoning = false, firstDeltaReceived = false;

    try {
        const res = await fetch('/chat', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ messages: apiHistory, configIndex: parseInt(modelSelect.value, 10) || 0 })
        });
        const reader = res.body.getReader(); const decoder = new TextDecoder(); let sseBuf = '';
        while (true) {
            const { done, value } = await reader.read(); if (done) break;
            sseBuf += decoder.decode(value, { stream: true });
            let idx;
            while ((idx = sseBuf.indexOf('\\n\\n')) !== -1) {
                const rawEvent = sseBuf.slice(0, idx); sseBuf = sseBuf.slice(idx + 2);
                if (!rawEvent.trim()) continue;
                let type = 'message', dataLines = [];
                for (const line of rawEvent.split('\\n')) {
                    if (line.startsWith('event:')) type = line.slice(6).trim();
                    else if (line.startsWith('data:')) dataLines.push(line.slice(5));
                }
                if (!dataLines.length) continue;
                const data = JSON.parse(dataLines.join('\\n'));
                
                if (type === 'reasoning') {
                    if (!reasoningBox) {
                        reasoningBox = document.createElement('div'); reasoningBox.className = 'msg reasoning-box';
                        reasoningBox.innerHTML = '<div class="reasoning-header"><span class="reasoning-arrow">▼</span><span class="reasoning-header-text">正在思考...</span></div><div class="reasoning-body-wrapper"><div class="reasoning-content-inner"><div class="reasoning-content"></div></div></div>';
                        reasoningBox.querySelector('.reasoning-header').addEventListener('click', () => { userToggledReasoning = true; reasoningBox.classList.toggle('collapsed'); });
                        chatEl.appendChild(reasoningBox); reasoningContent = reasoningBox.querySelector('.reasoning-content');
                    }
                    reasoningContent.appendChild(document.createTextNode(data.text)); currentMsgObj.reasoning += data.text;
                    reasoningContent.scrollTop = reasoningContent.scrollHeight; scrollBottom(true); saveSession();
                }
                else if (type === 'delta') {
                    if (!firstDeltaReceived) { firstDeltaReceived = true; if(reasoningBox && !userToggledReasoning) reasoningBox.classList.add('collapsed'); }
                    if (!answerDiv) { answerDiv = document.createElement('div'); answerDiv.className = 'msg assistant markdown'; chatEl.appendChild(answerDiv); }
                    currentText += data.text; currentMsgObj.content = currentText;
                    answerDiv.innerHTML = renderMarkdown(currentText.replace(/^\\s+/, '')); processCodeBlocks(answerDiv); scrollBottom(false); saveSession();
                }
                else if (type === 'status') {
                    if (!reasoningBox) {
                        reasoningBox = document.createElement('div'); reasoningBox.className = 'msg reasoning-box';
                        reasoningBox.innerHTML = '<div class="reasoning-header"><span class="reasoning-arrow">▼</span><span class="reasoning-header-text">正在处理...</span></div><div class="reasoning-body-wrapper"><div class="reasoning-content-inner"><div class="reasoning-content"></div></div></div>';
                        reasoningBox.querySelector('.reasoning-header').addEventListener('click', () => { userToggledReasoning = true; reasoningBox.classList.toggle('collapsed'); });
                        chatEl.appendChild(reasoningBox); reasoningContent = reasoningBox.querySelector('.reasoning-content');
                    }
                    const stepEl = document.createElement('div'); stepEl.className = 'reasoning-step'; stepEl.innerHTML = '⚙️ ' + escapeHtml(data.text); reasoningContent.appendChild(stepEl);
                    reasoningContent.scrollTop = reasoningContent.scrollHeight; scrollBottom(true); saveSession();
                }
                else if (type === 'build_task') {
                    if (reasoningBox) reasoningBox.classList.add('collapsed');
                    currentMsgObj.buildTaskId = data.task_id; currentMsgObj.buildStatus = 'building'; saveSession();
                    const buildCardEl = document.createElement('div'); buildCardEl.className = 'msg assistant';
                    buildCardEl.innerHTML = '<div style="display:flex; align-items:center; gap:8px; color:#007aff; font-weight:500;"><span style="display:inline-block; width:16px; height:16px; border:2px solid #007aff; border-top-color:transparent; border-radius:50%; animation:spin 1s linear infinite;"></span>云端编译中...</div><style>@keyframes spin { to { transform: rotate(360deg); } }</style>';
                    chatEl.appendChild(buildCardEl); scrollBottom(true);
                    pollBuildStatus(data.task_id, currentMsgObj, buildCardEl);
                }
                else if (type === 'error') {
                    const eb = document.createElement('div'); eb.className = 'msg error'; eb.textContent = data.message; chatEl.appendChild(eb); scrollBottom(true);
                }
            }
        }
    } catch (e) {
        const eb = document.createElement('div'); eb.className = 'msg error'; eb.textContent = '网络错误：' + e.message; chatEl.appendChild(eb);
    } finally {
        busy = false; sendBtn.disabled = false; sendBtn.classList.remove('busy'); sendBtn.textContent = '发送'; inputEl.focus(); scrollBottom(false);
    }
}

sendBtn.addEventListener('click', send);
inputEl.addEventListener('keydown', (e) => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send(); } });
loadSession();
</script>
</body>
</html>
`;-direction: column; gap: 12px; width: 100%; max-width: 1000px; margin: 0 auto; }
    .msg { line-height: 1.65; word-break: break-word; font-size: 15px; }
    .msg.user { max-width: 85%; align-self: flex-end; background: #007aff; color: #fff; padding: 10px 14px; border-radius: 14px; border-bottom-right-radius: 4px; white-space: pre-wrap; }
    .msg.assistant { align-self: flex-start; background: #fff; color: #222; padding: 14px 16px; border-radius: 14px; border: 1px solid #ececec; border-bottom-left-radius: 4px; width: 100%; }
    .msg.error { align-self: center; background: #fff2f2; color: #c62828; border: 1px solid #ffd4d4; font-size: 13px; padding: 10px 14px; border-radius: 8px; }
    
    .reasoning-box { align-self: flex-start; width: 100%; margin-bottom: 8px; background: #f8f9fa; border: 1px solid #d0d7de; border-radius: 8px; overflow: hidden; flex-shrink: 0; }
    .reasoning-header { padding: 12px 14px; cursor: pointer; display: flex; align-items: center; gap: 6px; font-size: 13.5px; color: #555; font-weight: 500; user-select: none; background: #f8f9fa; }
    .reasoning-arrow { font-size: 10px; transition: transform 0.3s ease; }
    .reasoning-box.collapsed .reasoning-arrow { transform: rotate(-90deg); }
    .reasoning-body-wrapper { display: grid; grid-template-rows: 1fr; transition: grid-template-rows 0.3s ease-out; }
    .reasoning-box.collapsed .reasoning-body-wrapper { grid-template-rows: 0fr; }
    .reasoning-content-inner { overflow: hidden; min-height: 0; }
    .reasoning-content { padding: 0 14px 14px; font-size: 13.5px; color: #666; white-space: pre-wrap; max-height: 300px; overflow-y: auto; border-top: 1px dashed #d0d7de; margin-top: 2px; padding-top: 10px; }
    .reasoning-step { display: block; margin-bottom: 6px; color: #888; font-size: 13px; }
    .reasoning-box.collapsed .reasoning-content { border-top-color: transparent; }

    .file-cards-container { margin-top: 14px; padding-top: 14px; border-top: 1px dashed #e0e2e6; display: flex; flex-direction: column; gap: 8px; }
    .file-card-inline { display: flex; align-items: center; gap: 10px; background: #f8f9fa; border: 1px solid #e9ecef; border-radius: 10px; padding: 10px 14px; font-size: 14px; white-space: normal; }
    .file-icon { font-size: 18px; line-height: 1; }
    .file-name { font-weight: 500; color: #222; }
    .file-download { margin-left: auto; color: #007aff; text-decoration: none; font-weight: 600; font-size: 13px; padding: 4px 10px; border-radius: 8px; background: #eef4ff; white-space: nowrap; }

    .markdown { white-space: normal; }
    .markdown > *:first-child { margin-top: 0; }
    .markdown > *:last-child  { margin-bottom: 0; }
    .markdown p { margin: 0 0 10px; }
    .markdown code { background: #eef4ff; color: #007aff; padding: 2px 6px; border-radius: 4px; font-family: monospace; font-size: 0.9em; }
    .markdown pre { margin: 14px 0; border: 1px solid #e0e2e6; border-radius: 8px; background: #fafafa; padding: 14px; overflow-x: auto; }
    .markdown pre code { background: none; padding: 0; color: #333; font-size: 0.88em; line-height: 1.55; }

    #composer { background: #fff; border-top: 1px solid #ececec; padding: 12px 12px calc(16px + env(safe-area-inset-bottom)); flex-shrink: 0; }
    .model-selector-wrapper { max-width: 1000px; margin: 0 auto 8px auto; }
    #model-select { padding: 6px 28px 6px 12px; border: 1px solid #ddd; border-radius: 8px; background-color: #fafafa; font-size: 13px; color: #333; outline: none; cursor: pointer; appearance: none; background-image: url("data:image/svg+xml;charset=US-ASCII,%3Csvg%20xmlns%3D%22http%3A%2F%2Fwww.w3.org%2F2000%2Fsvg%22%20width%3D%22292.4%22%20height%3D%22292.4%22%3E%3Cpath%20fill%3D%22%23666%22%20d%3D%22M287%2069.4a17.6%2017.6%200%200%200-13-5.4H18.4c-5%200-9.3%201.8-12.9%205.4A17.6%2017.6%200%200%200%200%2082.2c0%205%201.8%209.3%205.4%2012.9l128%20127.9c3.6%203.6%207.8%205.4%2012.8%205.4s9.2-1.8%2012.8-5.4L287%2095c3.5-3.5%205.4-7.8%205.4-12.8%200-5-1.9-9.2-5.5-12.8z%22%2F%3E%3C%2Fsvg%3E"); background-repeat: no-repeat; background-position: right 10px top 50%; background-size: 10px auto; }
    .composer-inner { max-width: 1000px; margin: 0 auto; display: flex; gap: 10px; align-items: flex-end; }
    textarea { flex: 1; resize: none; border: 1px solid #ddd; border-radius: 12px; padding: 11px 14px; font-size: 15px; outline: none; min-height: 46px; max-height: 120px; background: #fafafa; }
    button.send-btn { background: #007aff; color: #fff; border: none; border-radius: 12px; padding: 0 20px; font-size: 15px; font-weight: 600; cursor: pointer; height: 46px; white-space: nowrap; }
    button.send-btn.busy { background: #e5e5ea; color: #8e8e93; cursor: not-allowed; }
</style>
</head>
<body>
<header>
    <div class="title">AI 助手 (Cloudflare)</div>
    <button id="new-chat-btn">+ 新对话</button>
</header>
<div id="chat"></div>
<div id="composer">
    <div class="model-selector-wrapper">
        <select id="model-select">
            <option value="0">Agents-A1</option>
            <option value="1">Sage-A1.5</option>
            <option value="2">Agent-3.0-flash</option>
        </select>
    </div>
    <div class="composer-inner">
        <textarea id="input" rows="1" placeholder="发消息给 AI 助手…"></textarea>
        <button id="send" class="send-btn">发送</button>
    </div>
</div>

<script src="https://cdn.bootcdn.net/ajax/libs/marked/4.3.0/marked.min.js"></script>
<script>
const chatEl  = document.getElementById('chat');
const inputEl = document.getElementById('input');
const sendBtn = document.getElementById('send');
const modelSelect = document.getElementById('model-select');
const newChatBtn = document.getElementById('new-chat-btn');

let apiHistory = [];
let displayHistory = [];
let busy = false;
let globalTimers = {}; 

function scrollBottom(force) {
    if (force || chatEl.scrollHeight - chatEl.scrollTop - chatEl.clientHeight < 60) {
        chatEl.scrollTop = chatEl.scrollHeight;
    }
}
function escapeHtml(s) {
    return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}
function renderMarkdown(text) {
    if (typeof marked === 'undefined') return escapeHtml(text).replace(/\\n/g, '<br>');
    return marked.parse(text, { breaks: true, gfm: true });
}
function processCodeBlocks(container) {
    if (!container) return;
    container.querySelectorAll('pre').forEach(pre => {
        if (pre.parentNode.classList.contains('code-block-wrapper')) return;
        const code = pre.querySelector('code');
        if (!code) return;
        const wrapper = document.createElement('div');
        wrapper.className = 'code-block-wrapper';
        wrapper.style = "border:1px solid #e0e2e6; border-radius:8px; margin:14px 0; overflow:hidden;";
        const header = document.createElement('div');
        header.style = "display:flex; justify-content:space-between; padding:6px 12px; background:#f1f3f5; font-size:12px; border-bottom:1px solid #e0e2e6;";
        header.innerHTML = '<span>代码</span>';
        const copyBtn = document.createElement('button');
        copyBtn.textContent = '复制';
        copyBtn.style = "background:transparent; border:none; color:#007aff; cursor:pointer;";
        copyBtn.onclick = () => { navigator.clipboard.writeText(code.textContent).then(() => { copyBtn.textContent = '已复制'; setTimeout(() => copyBtn.textContent = '复制', 2000); }); };
        header.appendChild(copyBtn);
        pre.parentNode.insertBefore(wrapper, pre);
        wrapper.appendChild(header);
        wrapper.appendChild(pre);
    });
}

function loadSession() {
    const saved = localStorage.getItem('ai_chat_session_v5');
    if (saved) {
        try {
            const data = JSON.parse(saved);
            apiHistory = data.apiHistory || [];
            displayHistory = data.displayHistory || [];
            renderHistory();
        } catch (e) {}
    }
}
function saveSession() {
    localStorage.setItem('ai_chat_session_v5', JSON.stringify({ apiHistory, displayHistory }));
}

function renderHistory() {
    chatEl.innerHTML = '';
    displayHistory.forEach(item => {
        if (item.role === 'user') {
            const ub = document.createElement('div'); ub.className = 'msg user'; ub.textContent = item.content; chatEl.appendChild(ub);
        } else if (item.role === 'assistant') {
            if (item.reasoning) {
                const rb = document.createElement('div'); rb.className = 'msg reasoning-box collapsed'; 
                rb.innerHTML = `<div class="reasoning-header"><span class="reasoning-arrow">▼</span><span class="reasoning-header-text">思考过程 (点击展开)</span></div><div class="reasoning-body-wrapper"><div class="reasoning-content-inner"><div class="reasoning-content">${escapeHtml(item.reasoning)}</div></div></div>`;
                rb.querySelector('.reasoning-header').addEventListener('click', () => rb.classList.toggle('collapsed'));
                chatEl.appendChild(rb);
            }
            if (item.content) {
                const ab = document.createElement('div'); ab.className = 'msg assistant markdown'; ab.innerHTML = renderMarkdown(item.content); processCodeBlocks(ab); chatEl.appendChild(ab);
            }
            if (item.buildStatus) {
                const card = document.createElement('div'); card.className = 'msg assistant';
                if (item.buildStatus === 'building') {
                    card.innerHTML = `<div style="display:flex; align-items:center; gap:8px; color:#007aff; font-weight:500;"><span style="display:inline-block; width:16px; height:16px; border:2px solid #007aff; border-top-color:transparent; border-radius:50%; animation:spin 1s linear infinite;"></span>云端编译中...</div><style>@keyframes spin { to { transform: rotate(360deg); } }</style>`;
                    chatEl.appendChild(card);
                    if (!globalTimers[item.buildTaskId]) pollBuildStatus(item.buildTaskId, item, card);
                } else if (item.buildStatus === 'success') {
                    const file = item.files && item.files.length > 0 ? item.files[0] : null;
                    if (file) { card.innerHTML = `<div class="file-cards-container"><div class="file-card-inline"><span class="file-icon">📦</span><span class="file-name">编译成功，点击下载</span><a class="file-download" href="${file.url}">下载</a></div></div>`; chatEl.appendChild(card); }
                } else if (item.buildStatus === 'failed') { card.innerHTML = `<span style="color:#c62828;">❌ 编译失败</span>`; chatEl.appendChild(card); }
            }
        }
    });
    scrollBottom(true);
}

newChatBtn.addEventListener('click', () => {
    if (busy) { if(!confirm("确定要新开对话吗？")) return; }
    Object.values(globalTimers).forEach(clearInterval); globalTimers = {};
    localStorage.removeItem('ai_chat_session_v5');
    apiHistory = []; displayHistory = []; chatEl.innerHTML = ''; inputEl.focus();
});

function pollBuildStatus(taskId, msgObj, cardEl) {
    globalTimers[taskId] = setInterval(() => {
        fetch('/check_build?task_id=' + taskId)
            .then(res => res.json())
            .then(data => {
                if (data.status === 'success') {
                    clearInterval(globalTimers[taskId]); delete globalTimers[taskId];
                    msgObj.buildStatus = 'success'; msgObj.files = [{ name: 'Jar', url: '/download?task_id=' + taskId }]; saveSession();
                    cardEl.innerHTML = `<div class="file-cards-container"><div class="file-card-inline"><span class="file-icon">📦</span><span class="file-name">编译成功</span><a class="file-download" href="/download?task_id=${taskId}">下载</a></div></div>`;
                    scrollBottom(true);
                } else if (data.status === 'error') {
                    clearInterval(globalTimers[taskId]); delete globalTimers[taskId];
                    msgObj.buildStatus = 'failed'; saveSession();
                    cardEl.innerHTML = `<span style="color:red; display:block; margin-bottom:8px;">❌ 编译失败，AI 自动修复中...</span><pre style="font-size:12px; color:#999; background:#f5f5f5; padding:8px; border-radius:6px; overflow-x:auto; max-height:150px;">${escapeHtml(data.log)}</pre>`;
                    if (busy) return;
                    let fixPrompt = "之前的代码在云端编译失败了，报错日志如下：\\n\\n```\\n" + (data.log || "未知错误") + "\\n```\\n\\n请仔细分析错误原因（比如是不是忘记 import 导包了），修改代码后，重新调用 `package_mod_project` 打包。";
                    inputEl.value = fixPrompt; send();
                }
            }).catch(() => {});
    }, 10000);
}

async function send() {
    const text = inputEl.value.trim();
    if (!text || busy) return;
    inputEl.value = ''; inputEl.style.height = 'auto'; busy = true; sendBtn.disabled = true; sendBtn.classList.add('busy'); sendBtn.textContent = 'AI回答中';

    const ub = document.createElement('div'); ub.className = 'msg user'; ub.textContent = text; chatEl.appendChild(ub);
    apiHistory.push({ role: 'user', content: text }); displayHistory.push({ role: 'user', content: text });
    const currentMsgObj = { role: 'assistant', reasoning: '', content: '', files: [], buildStatus: null };
    displayHistory.push(currentMsgObj); saveSession(); scrollBottom(true);

    let answerDiv = null, currentText = "", reasoningBox = null, reasoningContent = null, userToggledReasoning = false, firstDeltaReceived = false, buildCardEl = null;

    try {
        const res = await fetch('/chat', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ messages: apiHistory, configIndex: parseInt(modelSelect.value, 10) || 0 })
        });
        const reader = res.body.getReader(); const decoder = new TextDecoder(); let sseBuf = '';
        while (true) {
            const { done, value } = await reader.read(); if (done) break;
            sseBuf += decoder.decode(value, { stream: true });
            let idx;
            while ((idx = sseBuf.indexOf('\\n\\n')) !== -1) {
                const rawEvent = sseBuf.slice(0, idx); sseBuf = sseBuf.slice(idx + 2);
                if (!rawEvent.trim()) continue;
                let type = 'message', dataLines = [];
                for (const line of rawEvent.split('\\n')) {
                    if (line.startsWith('event:')) type = line.slice(6).trim();
                    else if (line.startsWith('data:')) dataLines.push(line.slice(5));
                }
                if (!dataLines.length) continue;
                const data = JSON.parse(dataLines.join('\\n'));
                
                if (type === 'reasoning') {
                    if (!reasoningBox) {
                        reasoningBox = document.createElement('div'); reasoningBox.className = 'msg reasoning-box';
                        reasoningBox.innerHTML = `<div class="reasoning-header"><span class="reasoning-arrow">▼</span><span class="reasoning-header-text">正在思考...</span></div><div class="reasoning-body-wrapper"><div class="reasoning-content-inner"><div class="reasoning-content"></div></div></div>`;
                        reasoningBox.querySelector('.reasoning-header').addEventListener('click', () => { userToggledReasoning = true; reasoningBox.classList.toggle('collapsed'); });
                        chatEl.appendChild(reasoningBox); reasoningContent = reasoningBox.querySelector('.reasoning-content');
                    }
                    reasoningContent.appendChild(document.createTextNode(data.text)); currentMsgObj.reasoning += data.text;
                    reasoningContent.scrollTop = reasoningContent.scrollHeight; scrollBottom(true); saveSession();
                }
                else if (type === 'delta') {
                    if (!firstDeltaReceived) { firstDeltaReceived = true; if(reasoningBox && !userToggledReasoning) reasoningBox.classList.add('collapsed'); }
                    if (!answerDiv) { answerDiv = document.createElement('div'); answerDiv.className = 'msg assistant markdown'; chatEl.appendChild(answerDiv); }
                    currentText += data.text; currentMsgObj.content = currentText;
                    answerDiv.innerHTML = renderMarkdown(currentText.replace(/^\\s+/, '')); processCodeBlocks(answerDiv); scrollBottom(false); saveSession();
                }
                else if (type === 'status') {
                    if (!reasoningBox) {
                        reasoningBox = document.createElement('div'); reasoningBox.className = 'msg reasoning-box';
                        reasoningBox.innerHTML = `<div class="reasoning-header"><span class="reasoning-arrow">▼</span><span class="reasoning-header-text">正在处理...</span></div><div class="reasoning-body-wrapper"><div class="reasoning-content-inner"><div class="reasoning-content"></div></div></div>`;
                        reasoningBox.querySelector('.reasoning-header').addEventListener('click', () => { userToggledReasoning = true; reasoningBox.classList.toggle('collapsed'); });
                        chatEl.appendChild(reasoningBox); reasoningContent = reasoningBox.querySelector('.reasoning-content');
                    }
                    const stepEl = document.createElement('div'); stepEl.className = 'reasoning-step'; stepEl.innerHTML = '⚙️ ' + escapeHtml(data.text); reasoningContent.appendChild(stepEl);
                    reasoningContent.scrollTop = reasoningContent.scrollHeight; scrollBottom(true); saveSession();
                }
                else if (type === 'build_task') {
                    if (reasoningBox) reasoningBox.classList.add('collapsed');
                    currentMsgObj.buildTaskId = data.task_id; currentMsgObj.buildStatus = 'building'; saveSession();
                    buildCardEl = document.createElement('div'); buildCardEl.className = 'msg assistant';
                    buildCardEl.innerHTML = `<div style="display:flex; align-items:center; gap:8px; color:#007aff; font-weight:500;"><span style="display:inline-block; width:16px; height:16px; border:2px solid #007aff; border-top-color:transparent; border-radius:50%; animation:spin 1s linear infinite;"></span>云端编译中...</div><style>@keyframes spin { to { transform: rotate(360deg); } }</style>`;
                    chatEl.appendChild(buildCardEl); scrollBottom(true);
                    pollBuildStatus(data.task_id, currentMsgObj, buildCardEl);
                }
                else if (type === 'error') {
                    const eb = document.createElement('div'); eb.className = 'msg error'; eb.textContent = data.message; chatEl.appendChild(eb); scrollBottom(true);
                }
            }
        }
    } catch (e) {
        const eb = document.createElement('div'); eb.className = 'msg error'; eb.textContent = '网络错误：' + e.message; chatEl.appendChild(eb);
    } finally {
        busy = false; sendBtn.disabled = false; sendBtn.classList.remove('busy'); sendBtn.textContent = '发送'; inputEl.focus(); scrollBottom(false);
    }
}

sendBtn.addEventListener('click', send);
inputEl.addEventListener('keydown', (e) => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send(); } });
loadSession();
</script>
</body>
</html>
`;
