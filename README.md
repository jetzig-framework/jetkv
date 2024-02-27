# JetKV

_JetKV_ is a key-value store written in _Zig_, intended for use in development web servers.

In simple terms, _JetKV_ serves as a very rudimentary alternative to _Redis_ and _memcache_ that runs in-process without the need to run a separate service/daemon. Developers can use this library in local development to replicate some of the features of more robust production-ready systems without having to manage a separate process.

_JetKV_ can be used for:

* Background job queuing.
* Persistent data storage.
* Cache.

For production systems, battle-tested tools like _Redis_, _memcache_, _RabbitMQ_, etc. are recommended: _JetKV_ makes very little effort to be particularly performant or memory efficient.

## Checklist

* :white_check_mark: In-memory storage.
* :white_check_mark: String value storage.
* :white_check_mark: Array value storage.
* :white_check_mark: Array pop/queue implementation.
* :x: On-disk storage.
* :x: Key expiry.
* :x: Shared memory.

## License

[MIT](LICENSE)
