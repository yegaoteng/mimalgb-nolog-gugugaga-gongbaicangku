import HTML_CONTENT from './index.html';

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === '/github_callback' && request.method === 'POST') {
      try {
        const formData = await request.formData();
        const taskId = formData.get('task_id');
        const status = formData.get('status');
        
        if (status === 'success') {
          const jarFile = formData.get('jar_file'); 
          if (jarFile) {
            const arrayBuffer = await jarFile.arrayBuffer();
            await env.BUILD_KV.put(taskId + '_jar', arrayBuffer, { expirationTtl: 3600 });
          }
          await env.BUILD_KV.put(taskId + '_status', JSON.stringify({ status: 'success' }), { expirationTtl: 3600 });
        } else {
          const errorLog = formData.get('error_log') || '未知错误';
          await env.BUILD_KV.put(taskId + '_status', JSON.stringify({ status: 'failed', log: errorLog }), { expirationTtl: 3600 });
        }
        return new Response("ok");
      } catch (e) {
        return new Response("Error: " + e.message, { status: 500 });
      }
    }

    if (url.pathname === '/check_build' && request.method === 'GET') {
      const taskId = url.searchParams.get('task_id');
      const statusStr = await env.BUILD_KV.get(taskId + '_status');
      if (!statusStr) return new Response(JSON.stringify({ status: 'building' }), { headers: { 'Content-Type': 'application/json' } });
      const statusData = JSON.parse(statusStr);
      if (statusData.status === 'success') {
        return new Response(JSON.stringify({ status: 'success', file: taskId + '.jar' }), { headers: { 'Content-Type': 'application/json' } });
      } else {
        return new Response(JSON.stringify({ status: 'error', log: statusData.log }), { headers: { 'Content-Type': 'application/json' } });
      }
    }

    if (url.pathname === '/download' && request.method === 'GET') {
      const taskId = url.searchParams.get('task_id');
      const jarBuffer = await env.BUILD_KV.get(taskId + '_jar', { type: 'arrayBuffer' });
      if (!jarBuffer) return new Response("文件不存在", { status: 404 });
      return new Response(jarBuffer, {
        headers: { 'Content-Type': 'application/java-archive', 'Content-Disposition': 'attachment; filename="' + taskId + '.jar"' }
      });
    }

    if (url.pathname === '/chat' && request.method === 'POST') {
      const body = await request.json();
      return handleChat(body, env, request);
    }

    return new Response(HTML_CONTENT, { headers: { 'Content-Type': 'text/html; charset=utf-8' } });
  }
};

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
    content: '你是一个专业的 Minecraft 模组开发助手。\n你可以使用工具 package_mod_project 将代码打包并提交云端编译。\n【极度重要】：1. 直接一次性把所有代码放入工具，千万不要分批或用纯文本输出大段代码！ 2. 如果遇到构建失败，请根据日志修复代码中的 import 报错等。'
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
          project_files: { type: 'array', items: { type: 'object', properties: { path: { type: 'string' }, content: { type: 'string' } }, required: ['path', 'content'] } },
          mod_name: { type: 'string' }
        },
        required: ['project_files', 'mod_name']
      }
    }
  }];

  const aiReq = { model: cfg.model, messages: cleanMessages, stream: true, tools: tools, tool_choice: 'auto' };

  const aiRes = await fetch(cfg.endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + cfg.token },
    body: JSON.stringify(aiReq)
  });

  if (!aiRes.ok) {
    return new Response('event: error\ndata: ' + JSON.stringify({message: 'API Error: ' + aiRes.status}) + '\n\n', { headers: { 'Content-Type': 'text/event-stream' } });
  }

  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();
  const encoder = new TextEncoder();

  processStream(aiRes.body, writer, encoder, env, request).catch(console.error);

  return new Response(readable, { headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive' } });
}

async function processStream(stream, writer, encoder, env, request) {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let content = '';
  let toolCalls = {};
  let isError = false;
  
  const writeEvent = (event, data) => {
    try {
      writer.write(encoder.encode('event: ' + event + '\ndata: ' + JSON.stringify(data) + '\n\n'));
    } catch(e) {}
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
            project_json: JSON.stringify(args.project_files), 
            callback_url: callbackUrl
          }
        };

        const ghRes = await fetch('https://api.github.com/repos/' + env.GITHUB_REPO + '/dispatches', {
          method: 'POST',
          headers: {
            'Authorization': 'Bearer ' + env.GITHUB_TOKEN,
            'Accept': 'application/vnd.github+json',
            'User-Agent': 'Cloudflare-Worker'
          },
          body: JSON.stringify(payload)
        });

        if (ghRes.status === 204) {
          writeEvent('build_task', { task_id: taskId });
        } else {
          const errData = await ghRes.text();
          writeEvent('error', { message: 'GitHub 提交失败: ' + ghRes.status + ' - ' + errData });
        }
      } catch (err) {
        writeEvent('error', { message: '系统错误: ' + err.message });
      }
    }
  } catch (e) {
    isError = true;
    console.error(e);
    // 关键修复：流读取异常时，发送错误事件给前端
    writeEvent('error', { message: 'AI 响应中断，可能是生成内容过长导致超时。请重试或精简需求。' });
  } finally {
    if (!isError) writeEvent('done', {});
    writer.close();
  }
}
