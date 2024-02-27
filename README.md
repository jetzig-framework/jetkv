# JetKV

_JetKV_ is a key-value store written in _Zig_, intended for use in development web servers.

In simple terms, _JetKV_ serves as a very rudimentary alternative to _Redis_ and _memcache_ that runs in-process without the need to run a separate service/daemon. Developers can use this library in local development to replicate some of the features of more robust production-ready systems without having to manage a separate process.

_JetKV_ can be used for:

* Background job queuing.
* Persistent data storage.
* Cache.

For production systems, battle-tested tools like _Redis_, _memcache_, _RabbitMQ_, etc. are recommended.

## Checklist

* :x: In-memory storage.
* :x: On-disk storage.
* :x: String value storage.
* :x: Array value storage.
* :x: Key expiry.
* :x: Array pop/queue implementation.
* :x: Shared memory.

## License

[MIT](LICENSE)
