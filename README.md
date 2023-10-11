**:warning: This is an incomplete work-in-progress, bugs are expected: Do not use it if you can't afford to loose data.**

Notafs is a library allowing the creation of copy-on-write filesystems running on Mirage block devices. At its core, it provides a glorified memory allocator to allocate and release pages of the disk in a safe way. Some nice properties are derived from there:

- **Fail-safe:** Imperative updates are transparently written to new pages of the disk, rather than mutating the existing sectors in place. This guarantees that previous versions of the filesystem are always recoverable if anything goes wrong -- typically caused by a harsh power-off, with disk writes not completing and/or creating corrupted sectors. During boot, the latest filesystem is checksummed to validate its coherency -- if not, a previous valid version is used.
- **Transactional semantics:** Since updates generate a shallow copy of the new filesystem (like a purely functional datastructure), one can reveal a large set of changes at once by comitting the freshly created copy. This permits updating multiple files and doing complex mutations involving several operations in a way that appears atomic (either all changes are visible at the same time, or none are).
- **Relatively efficient:** A number of optimizations are running behind the scene, including dynamic sector pointer sizes (which uses less space on smaller disks), batching reads and writes of consecutive sectors, and flushing writes to disk early to avoid blowing up memory when one is performing a large update... but more is coming!

As a work-in-progress demo, `notafs` includes a partial implementation of the [`mirage-kv`](https://ocaml.org/p/mirage-kv) interface. It supports large file contents represented by append-optimized ropes, but it is limited to a small numbers of filenames (!). While this is unsatisfying for general usage, this minimal filesystem is enough to run the [`irmin-pack` backend of Irmin](https://mirage.github.io/irmin/irmin-pack/), since it only requires a dozen of very large files. By running Irmin, one gets for free a filesystem-on-steroid for MirageOS: it supports an arbitrary number of filenames, is optimized for large and small file contents, has git-like history and branching and merging, ... and even provides a garbage collector to avoid running out of disk space (by deleting older commits). Since the Irmin filesystem is versionned by merkle hashes of its contents, one can imagine deploying [reproducible unikernels](https://robur.coop/Projects/Reproducible_builds) on reproducible filesystem states!

The tests include a visualization of Irmin running on a Mirage block device, performing commits and garbage collection. Each 1kb sector is represented by a 32x32 pixel square (colored pages contain live data, grey ones are free). Epilepsy warning: this recording of the test below contains flashing lights.

https://github.com/art-w/notafs/assets/4807590/82a33898-5938-42ba-8376-f2b89748878b

To run the unikernel demos, you'll need to pin the `notafs` library, copy the `unikernel-kv` folder out of the project (to avoid recursive issues with `opam-monorepo`), compile for your prefered target and create a disk to use:

```shell
# Pin the library
$ cd notafs
notafs/ $ opam pin . -ny --with-version=dev

# Copy the demos to another folder
notafs/ $ cp -r unikernel-kv ../unikernel-kv
notafs/ $ cd ../unikernel-kv

# Build the unikernel for your prefered target
unikernel-kv/ $ mirage config -t hvt
unikernel-kv/ $ make depends
unikernel-kv/ $ make build

# Create a disk
$ dd if=/dev/zero of=/tmp/storage count=400

# And run!
unikernel-kv/ $ solo5-hvt --block:storage=/tmp/storage -- ./dist/block_test.hvt
```

The Irmin demo relies on OCaml 5 support for algebraic effects: While the `irmin-pack` backend abstracts its IO syscalls behind a direct-style API, interacting with Mirage block devices is done through Lwt. To bridge this gap, the small `lwt_direct` library hides Lwt operation behind a direct-style interface usable by `irmin-pack` (it plays a similar purpose to `lwt_eio` without a unix dependency). Perhaps surprisingly, this indirection trick doesn't work well when Irmin itself uses Lwt internally -- so the Eio fork of `irmin-pack` is used instead with the `eio.mock` handler. Note that [OCaml 5 support in solo5 is still experimental](https://github.com/mirage/ocaml-solo5/pull/124):

```shell
$ opam switch 5.0.0

# Enable experimental support for OCaml 5 on solo5:
$ opam pin https://github.com/mirage/ocaml-solo5.git#500-cleaned -ny
```
