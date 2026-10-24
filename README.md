# `llamm`

[![Crates.io](https://img.shields.io/crates/v/llamm.svg?label=llamm)](https://crates.io/crates/llamm)
[![license: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](#license)
[![Rust CI](https://github.com/HellButcher/llamm/actions/workflows/rust.yml/badge.svg)](https://github.com/HellButcher/llamm/actions/workflows/rust.yml)

<!-- Short Introduction -->
`LlaMM` 

## Installation

```sh
cargo install --git https://github.com/HellButcher/llamm.git
```

Then just run `llamm` on the command line.

It will connect to `http://localhost:9090` (it needs a running `llama.cpp` instance there)
and will display the available models with their status.

- Use `j` and `k` to move the selection.
- Type `l` to load or `u` to unload the selected model

## License

[license]: #license

This project is licensed under either of

* MIT license ([LICENSE-MIT] or <http://opensource.org/licenses/MIT>)
* Apache License, Version 2.0, ([LICENSE-APACHE] or <http://www.apache.org/licenses/LICENSE-2.0>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

[LICENSE-MIT]: ./LICENSE-MIT
[LICENSE-APACHE]: ./LICENSE-APACHE
