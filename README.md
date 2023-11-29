**:warning: Experimental! Do not use if you can't afford to loose data! :warning:**

Notafs is a pseudo filesystem for Mirage block devices. It can handle a small number of large files. While the limited number of filenames is unsatisfying for general usage, it can be used to run the [`irmin-pack` backend of Irmin](https://mirage.github.io/irmin/irmin-pack/) which only requires a dozen of very large files. By running Irmin, one gets for free a filesystem-on-steroid for MirageOS: it supports an arbitrary large number of filenames; is optimized for small and large file contents; performs files deduplication; includes a git-like history with branching and merging, ... and it even provides a garbage collector to avoid running out of disk space (by deleting older commits). Since the Irmin filesystem is versioned by merkle hashes, one can imagine deploying [reproducible unikernels](https://robur.coop/Projects/Reproducible_builds) on reproducible filesystem states!

At its core, Notafs provides a memory allocator for disk pages allowing copy-on-write everywhere in a safe way. Some nice properties are derived from there:

- **Fail-safe:** All updates are written to new pages of the disk, rather than mutating existing sectors in place. This guarantees that previous versions of the filesystem are always recoverable -- typically in case of a harsh power-off, with disk writes not completing and/or creating corrupted sectors. During boot, the latest filesystem is checksummed to validate its coherency -- if not, a previous valid version of the files is used.
- **Transactional semantics:** Since updates generate a shallow copy of the new filesystem (like a purely functional datastructure), one can reveal a large set of changes at once by persisting the freshly created copy. This permits updating multiple files and doing complex mutations involving several operations in a way that appears atomic (either all changes are visible, or none are).
- **Relatively efficient:** A number of optimizations are running behind the scene, including dynamic sector pointer sizes (to use less space on smaller disks), batching writes of consecutive sectors, and flushing writes to disk early to avoid blowing up memory when performing a large update.

As a work-in-progress demo, `notafs` includes a partial implementation of the [`mirage-kv`](https://ocaml.org/p/mirage-kv) interface. It supports large file contents represented by append-optimized B-tree ropes, but it is not optimized for a large number of filenames (no hard limit though). Benchmarks on `solo5-hvt` with a 2gb disk with 4kb sectors (512b for fat/docteur), and 1gb of available ram, with different filesystems failing sooner or later depending on their use of memory:

![benchmark `set/get/get_partial` on large files](https://art-w.github.io/notafs/bench.png)

## Unikernel demos

To run the unikernel demos, you'll need to pin the `notafs` library, copy the `unikernel-kv` folder out of the project (to avoid recursive issues with `opam-monorepo`), compile it for your prefered Mirage target and create a disk to use:

```shell
# Pin the library
$ cd notafs
notafs/ $ opam pin add notafs . --with-version=dev

# Copy the mirage-kv demo to another folder
notafs/ $ cp -r unikernel-kv ../unikernel-kv
notafs/ $ cd ../unikernel-kv

# Build the unikernel for your prefered target
unikernel-kv/ $ mirage config -t hvt
unikernel-kv/ $ make depends
unikernel-kv/ $ make build

# Create an empty disk
$ truncate -s 400K /tmp/storage

# And run!
unikernel-kv/ $ solo5-hvt --block:storage=/tmp/storage -- ./dist/block_test.hvt
```

The integration with Irmin relies on OCaml 5 support for algebraic effects: While the `irmin-pack` backend abstracts its IO syscalls behind a direct-style API, interacting with Mirage block devices is done through Lwt. To bridge this gap, the small `lwt_direct` library hides Lwt operation behind a direct-style interface usable by `irmin-pack` (it plays a similar purpose to [`lwt_eio`](https://github.com/ocaml-multicore/lwt_eio) without a unix dependency). Perhaps surprisingly, this indirection trick doesn't work well when Irmin itself uses Lwt internally -- so the experimental Eio fork of `irmin-pack` is used instead (with the `eio.mock` handler). Note that [OCaml 5 support in solo5 is also experimental](https://github.com/mirage/ocaml-solo5/pull/124):

```shell
$ opam switch 5.0.0

# Enable experimental support for OCaml 5 on solo5:
$ opam pin 'https://github.com/mirage/ocaml-solo5.git#500-cleaned'

notafs/ $ opam pin . --with-version=dev
```

The Irmin unikernel demo on solo5 can be run by following the same steps as for the `mirage-kv` one above. You may have to set `solo5` version to `0.8.0` in `mirage/block_test-hvt.opam` before running `make depends`.

The tests include a visualization of Irmin running on a Mirage block device, performing commits and garbage collection. Each 1kb sector is represented by a 32x32 pixel square (colored pages contain live data, grey crossed ones are free). Epilepsy warning: the slowed recording of the test below contains flashing lights.

```
# Run the test with a graphic visualization of blocks operations:
notafs/ $ truncate -s 200K /tmp/notafs-irmin
notafs/ $ dune exec -- ./tests/test_irmin.exe   # --help for options
```

https://github.com/art-w/notafs/assets/4807590/915f6967-d26e-47fe-b53c-467762c24f05

## Notafs-CLI

A command-line tool is available to facilitate the creation of a disk usable with the `mirage-kv` interface:

```shell
# Install the executable
notafs/ $ opam pin add notafs-cli . --with-version=dev

# Create an empty disk
$ truncate -s 400K /tmp/storage
```

Before we can use it, the freshly created disk needs to be formatted. The flag `-p` represents the size of the sectors (512, 1024, 4096). It is optional and set to 512 by default.

```shell
# Format a disk
$ notafs-cli format -d/tmp/storage -p4096

# Display general informations about a formatted disk:
$ notafs-cli info /tmp/storage
```

To copy local files into the disk and extract them afterward: (paths on the disk have to be prefixed with the character `@` when using the `copy` operation)

```shell
# Copy a local file `foo` into the disk as `dir/foo`:
$ notafs-cli copy -d/tmp/storage foo @dir/foo

# Duplicate a disk file `dir/foo` as `bar`:
$ notafs-cli copy -d/tmp/storage @dir/foo @bar

# Extract a disk file `bar` and name it `goo`:
$ notafs-cli copy -d/tmp/storage @bar goo

# Dump a file `bar` from the disk into the standard output:
$ notafs-cli cat -d/tmp/storage bar
```

To list the contents and metadatas of files:

```shell
# Get the size of a file `foo`:
notafs/ $ notafs-cli stats -d/tmp/storage foo

# List all the files/subdirectories of the folder `dir`:
notafs/ $ notafs-cli list -d/tmp/storage dir

# Recursively list all the files/directories of the folder `dir`:
notafs/ $ notafs-cli tree -d/tmp/storage dir
```

And to perform maintenance operations:

```shell
# Create an empty file named 'foo'
$ notafs-cli touch -d/tmp/storage foo

# Rename file 'foo' to 'bar':
$ notafs-cli rename -d/tmp/storage foo bar

# Remove the 'bar' file:
$ notafs-cli remove -d/tmp/storage bar
```
