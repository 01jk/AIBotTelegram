const http = require("http");
const owner = 4294967311;
const tokenPath = "/bot123:test-token/";

function send(res, status, body) {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(body));
}

function message(updateId, sender, text) {
  return {
    update_id: updateId,
    message: {
      from: { id: sender, is_bot: false },
      chat: { id: sender, type: "private" },
      text,
    },
  };
}

function callback(updateId, sender, data) {
  return {
    update_id: updateId,
    callback_query: {
      id: String(updateId),
      from: { id: sender, is_bot: false },
      message: {
        message_id: 100, date: 1794100000,
        from: { id: 123, is_bot: true },
        chat: { id: owner, type: "private" },
      },
      data,
    },
  };
}

http.createServer(async (req, res) => {
  try {
    let body = req.body;
    if (!body || typeof body !== "object") {
      let raw = "";
      for await (const chunk of req) raw += chunk;
      body = JSON.parse(raw || "{}");
    }
    const method = req.url.startsWith(tokenPath) ? req.url.slice(tokenPath.length) : "";
    if (req.method === "POST" && method === "getMe") {
      return send(res, 200, { ok: true, result: { id: 123, username: "testbot", is_bot: true } });
    }
    if (req.method === "POST" && method === "getUpdates") {
      if (!body.allowed_updates?.includes("callback_query")) {
        return send(res, 400, { ok: false, error_code: 400 });
      }
      if (body.offset === 5) return send(res, 409, { ok: false, error_code: 409 });
      if (!(await pm.state.get("telegram-control-polled"))) {
        await pm.state.set("telegram-control-polled", true);
        return send(res, 429, { ok: false, error_code: 429, parameters: { retry_after: 1 } });
      }
      return send(res, 200, {
        ok: true,
        result: [callback(1, 7, "ui:mode auto"), message(2, owner, "/start"),
          callback(3, owner, "ui:pause"), message(4, owner, "/status")],
      });
    }
    if (req.method === "POST" && method === "answerCallbackQuery" && body.callback_query_id === "3") {
      return send(res, 200, { ok: true, result: true });
    }
    if (req.method === "POST" && method === "sendMessage" && body.chat_id === owner) {
      if (body.text === "Управление ботом" && !body.reply_markup?.inline_keyboard?.flat()
          .some((button) => button.callback_data === "ui:pause")) {
        return send(res, 400, { ok: false, error_code: 400 });
      }
      return send(res, 200, { ok: true, result: { message_id: 100 } });
    }
    return send(res, 501, { ok: false, error_code: 501 });
  } catch {
    return send(res, 400, { ok: false, error_code: 400 });
  }
}).listen(process.env.PORT || 4500);
