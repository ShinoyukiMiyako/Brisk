# CPA 响应夹具（脱敏）

本目录是 CPA 真实响应的脱敏副本，供 brisk-proto、brisk-gateway 的测试逐字节回放。原始样本在 `docs/samples/cpa/`，只保存在本地，不进 git。

## 来源

- 上游：CLIProxyAPI v7.3.8，2026-09-19 从 Windows 开发机用 `curl -N` 抓取。`.headers` 是响应头原文（CRLF），正文是解码 chunked 之后的响应体。
- 提示词统一为 `Reply with exactly: pong`。
- 默认测试模型为 `grok-4.6(xhigh)`；响应中的模型名回显为 `grok-4.6-build`，与请求不同，测试不能靠回显匹配。

| 夹具 | 原始文件 | 用途 |
|---|---|---|
| `chat-stream-grok46-xhigh.sse`、`.headers` | `grok46-xhigh-chat-stream` | 默认测试模型的 Chat 流；末块同时带 `finish_reason` 与 usage（213/71，reasoning 70） |
| `chat-stream-grok46-suffix.sse`、`.headers` | `grok46-suffix-chat-stream` | 请求模型名为 `grok-4.6(xhigh)`；usage 213/105，cached 128，reasoning 104 |
| `chat-stream-grok43.sse` | `grok-chat-stream` | reasoning_content 较长；usage 197/216，reasoning 215 |
| `chat-stream-gpt55.sse`、`.headers` | `openai-chat-stream` | Codex 后端，usage 与 finish 同块；usage 307/5 |
| `chat-nonstream-grok46-xhigh.json`、`.headers` | `grok46-xhigh-chat-nonstream` | 非流式；usage 213/128，cached 128，reasoning 127 |
| `chat-nonstream-gpt55.json` | `openai-chat-nonstream` | 非流式；usage 307/5 |
| `error-bad-key.json`、`.headers` | `error-bad-key` | 401，`error` 为字符串形态 |
| `error-bogus-effort.json`、`.headers` | `grok46-bogus-effort` | 400，`error` 为对象形态，原样透传 |
| `responses-stream-grok46-xhigh.sse` | `grok46-xhigh-responses-stream` | 只用于分帧：`event:` 行、事件内的注释行（`: xai-usage`）、末尾多余的 `\n` |
| `anthropic-stream-grok46-suffix.sse` | `grok46-suffix-anthropic-stream` | 只用于分帧 |
| `gemini-sse-grok46-suffix.sse` | `grok46-suffix-gemini-sse` | 只用于分帧 |

测试引用夹具一律写 `include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures/cpa/<文件名>"))`，不依赖当前工作目录。`.gitattributes` 对 `fixtures/**` 设置了 `-text`，git 不改写这里的行结束符。

## 脱敏规则

由 `scripts/fixtures/sanitize-cpa.py <src-dir> <dst-dir>` 生成，只用 Python 标准库，结果确定，可重复执行。所有替换都是等长的原位替换，行结束符、分帧和字节偏移与原件一致。

1. 键为 `id`、`item_id`、`responseId`、`response_id` 的 JSON 字符串值：保留第一个 `_` 及之前的前缀（`resp_`、`msg_`、`rs_`）和连字符位置，其余字符替换为左补零的十进制序号。序号按前缀之后的部分分配，同一文件内相同的原值得到相同的占位值；同一响应的 `rs_<uuid>`、`msg_<uuid>` 与裸 `<uuid>` 共用一个序号，保留了原件中它们之间的对应关系。
2. `prompt_cache_key` 替换为全零 UUID；`safety_identifier` 保留 `user-` 前缀，其余替换为 `0`；`system_fingerprint` 保留 `fp_` 前缀，其余替换为 `0`。
3. 响应头 `X-Cpa-Trace-Id` 保留时间戳段，另两段替换为 `0`（原值中有凭据索引的哈希）；`Date` 保留。
4. 其余内容保留：提示词、推理文本、token 数、`created`、`cost_in_usd_ticks`，以及 Responses 流的 `encrypted_content` 和 Anthropic 流的 thinking `signature`（二者是上游对推理内容的密文与签名，不是凭据）。

## 检查

```sh
python scripts/fixtures/sanitize-cpa.py --check fixtures/cpa
python scripts/fixtures/sanitize-cpa.py --check fixtures/cpa --secret-env BRISK_CPA_KEY
```

`--check` 对目录中的每个夹具检查上述规则和凭据痕迹，要求目录中恰好是下表列出的文件，并核对下表的 SHA-256。`--secret-env NAME` 可重复，另外检查任何文件（包括本文件）都不含该环境变量的值，失败时只报文件名。CPA 的 api-keys 是运维自定义的任意字符串，前缀检查查不出来，提交夹具前由集成负责人在本机带这个参数执行一遍。

CI 不需要 Python：`crates/brisk-proto/tests/fixtures_sanitised.rs` 用 `include_bytes!` 读入全部夹具，做同样的规则与凭据痕迹检查。

## 文件与 SHA-256

| 文件 | 原始文件 | 字节数 | SHA-256 |
|---|---|---|---|
| `chat-stream-grok46-xhigh.sse` | `grok46-xhigh-chat-stream.body` | 4283 | `558888a320e8fc53a4983a42aa46c4116e05d7daf8f676362a7c33c4553a02b7` |
| `chat-stream-grok46-xhigh.headers` | `grok46-xhigh-chat-stream.headers` | 613 | `c778ebbe1ba78c32d11b2c255f74a910c3e38ef0a4a670c000182a15d77f0efb` |
| `chat-stream-grok46-suffix.sse` | `grok46-suffix-chat-stream.body` | 4568 | `57102cb12f293aaba7ab1fa1ccbbab78d01a9a8d97675d3bfe111ba992f4c4d6` |
| `chat-stream-grok46-suffix.headers` | `grok46-suffix-chat-stream.headers` | 613 | `cd8ba37165728f98c32da0fe8a0d34497296fb718d82c2ae6ba855b095093510` |
| `chat-stream-grok43.sse` | `grok-chat-stream.body` | 3928 | `6b6b0a2bca9acee40d3b9b525081413575b0b5d00ae6cfb86f5707e6ce46fca4` |
| `chat-stream-gpt55.sse` | `openai-chat-stream.body` | 772 | `eca31c0b253c46bdd2649d8c20668637ce89808fada602934ea034af932c4722` |
| `chat-stream-gpt55.headers` | `openai-chat-stream.headers` | 613 | `499b9f5dba28775605372cbc38a2b3c18afa247e20f2d6e65293ff9e3bf31e35` |
| `chat-nonstream-grok46-xhigh.json` | `grok46-xhigh-chat-nonstream.body` | 525 | `9c8d9499fdb1d03593b5596523ab352ff44b7cfc0072b15889ad9e974d62304b` |
| `chat-nonstream-grok46-xhigh.headers` | `grok46-xhigh-chat-nonstream.headers` | 556 | `6d1d0c1e7cd5d9d19f59d70fbece5777aed68e4d9aafcce59cd235f12c912a34` |
| `chat-nonstream-gpt55.json` | `openai-chat-nonstream.body` | 539 | `d4e4713c03bb8a83d0f785d5765216641fb40f7e64fd7c8882440a2fe8b6cb1f` |
| `error-bad-key.json` | `error-bad-key.body` | 27 | `1a47b153fd1fb4e74d638b0fe320bfbd0e6c216f8e5520164010ef99b7971a6b` |
| `error-bad-key.headers` | `error-bad-key.headers` | 522 | `792cf72ccb1a1a5956796a5356c0844b712c706efe181e3522a944467fc46e70` |
| `error-bogus-effort.json` | `grok46-bogus-effort.body` | 130 | `ce91e29d4102d984bd006bac90ead5189145732878801f59b509eeeca6916906` |
| `error-bogus-effort.headers` | `grok46-bogus-effort.headers` | 565 | `37df7eb19dc6abc4d687e8b6643e80c5d8dfdee12520cab78ba3189e56f0428e` |
| `responses-stream-grok46-xhigh.sse` | `grok46-xhigh-responses-stream.body` | 10910 | `27349f9ad9585396b67eda0285d83e03f0353cd80f13fb5d591b50ccb14d1f31` |
| `anthropic-stream-grok46-suffix.sse` | `grok46-suffix-anthropic-stream.body` | 3725 | `b6f4d059ef822245479a805cabcc5a3dc354144f5a1b142c1f7a9e1245e7f8a9` |
| `gemini-sse-grok46-suffix.sse` | `grok46-suffix-gemini-sse.body` | 4705 | `a6f0ca9c00dd16e90a35acb793beaa9966738f12733519f93f5d5bf2ed670362` |

## `--secret-env` 检查记录

| 日期 | 执行者 | 命令 | 结果 |
|---|---|---|---|
| 2026-09-24 | 集成负责人（本机 Windows，夹具提交 a4a5363） | `python scripts/fixtures/sanitize-cpa.py --check fixtures/cpa --secret-env BRISK_CPA_KEY` | 通过：17 个夹具与本文件都不含该值 |

`BRISK_CPA_KEY` 取自基准机上 CPA v7.3.8 的 `api-keys`（配置里只有 1 个 key），执行前先用它请求 `GET /v1/models` 得到 200、在它前面加一个字符得到 401，确认取到的是生效的 key，而不是一次空检查。
