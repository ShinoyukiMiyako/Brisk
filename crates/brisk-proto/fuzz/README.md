# brisk-proto 的 fuzz 目标

契约 05 第 4.8 节规定的三个 cargo-fuzz 目标。本目录的 `Cargo.toml` 自带 `[workspace]`，不属于主 workspace：libFuzzer 需要 nightly 的 sanitizer 参数，主 workspace 不能沾上。

| 目标 | 输入 | 断言 |
|---|---|---|
| `head` | 任意字节作为请求体 | `ChatHead::parse` 与 `serde_json::Value` 差分：结果类别一致，解码值一致，每个 span 切出的字节解析后等于对应的值；存在与声明字段折叠等价的键时必须是 `AmbiguousKey`；Value 拒绝的输入 ChatHead 也必须拒绝 |
| `splice` | 任意 `inject` 标志、可选替换模型名、任意请求体 | 请求体被 `ChatHead` 接受时做 `plan_chat`：输出与编辑表逐字节一致，能被 Value 解析且等于「换掉 model、注入 `include_usage: true`」后的输入，`len()` 精确，段数不超过 5，非流式请求注入返回 `InjectWithoutStream` |
| `sse` | 任意字节加任意切块位置 | 任意切块与整流参考分帧器的结果相同（事件原文、data、`event` 字段、注释事件、起止位置、每块的 `last_boundary`、`finish()`），不 panic；超过 `MAX_EVENT_BYTES` 的输入不收录 |

差分 oracle 与 SSE 参考分帧器不在本目录：目标用 `#[path]` 直接编译 `../tests/common/oracle.rs` 与 `../tests/common/sse_ref.rs`，与属性测试 `jsonhead_props`、`splice_props`、`sse_props` 共用同一份实现，二者不会各自漂移。

`clippy.toml` 故意为空：否则 clippy 会读到上一级 brisk-proto 的配置，其中 R1 清单禁止的 `std::fs::File::create` 正是 `fuzz_target!` 宏展开出来的。

## 运行（Linux 或 WSL2）

需要 nightly 与 `cargo install cargo-fuzz`。在 `crates/brisk-proto` 目录下：

```sh
cargo +nightly fuzz run head   fuzz/corpus/head   fuzz/seeds/head -- -dict=$PWD/fuzz/json.dict -max_total_time=1800
cargo +nightly fuzz run splice fuzz/corpus/splice fuzz/seeds/head -- -dict=$PWD/fuzz/json.dict -max_total_time=1800
cargo +nightly fuzz run sse    fuzz/corpus/sse    ../../fixtures/cpa -- -dict=$PWD/fuzz/sse.dict -max_total_time=1800
```

第一个目录是 libFuzzer 写入新输入的语料库（`corpus/`、`artifacts/` 已被本目录的 `.gitignore` 排除）；其后的目录只读，作为种子：`seeds/head/` 是几个手写请求体（其中有转义键、折叠键与大量空白），`sse` 直接以 CPA 夹具为种子。`splice` 与 `sse` 的输入经 `arbitrary` 解码，种子字节会被重新解释，仍能提供有用的结构。

验收（契约 05 第 4.8 节）：三个目标各跑 30 分钟，没有崩溃，没有差分不一致。fuzz 只检查正确性，不产出性能数字，WSL2 即可，不需要基准机。

## Windows

Windows 开发机上只做编译检查，不运行：

```sh
cd crates/brisk-proto/fuzz
cargo check
cargo clippy --all-targets -- -D warnings
```

stable 工具链在 MSVC 目标上无法链接带 sancov 插桩的 libFuzzer（缺少 `__start___sancov_cntrs` 等段起止符号），这与 cargo-fuzz 在 Windows 上需要 nightly 与 ASan 的限制一致。目标里的全部断言都在属性测试中以同一份 oracle 在 Windows 上运行。
