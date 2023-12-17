(** File system for Mirage block devices *)

(** {1:checksum Checksum interface & implementations} *)

(** Signature for checksum modules. *)
module type CHECKSUM = sig
  include Checksum.S
end

module Adler32 : CHECKSUM
(** [Adler32] implementation, built around {!Checkseum}'s implementation. *)

module No_checksum : CHECKSUM
(** Fake implementation, does nothing.
    Use it only where integrity checks does not matter (mostly specific tests). *)

(** {1:stores Notafs stores} *)

(** The [Notafs] library provides two entry points, depending on your needs:
    - {!KV}, a simple key-value implementation, which follows the {!Mirage_kv} signatures.
      It should be the best entry point for anyone wishing to use [Notafs] alongside MirageOS.
    - {!FS}, a lower-level implementation, with a more usual file system approach. *)

(** {2:store-kv KV, hierarchical Key-Value store} *)

(** Simple key-value implementation, based on {!Mirage_kv.RW}.

    It also provides a [format] & [connect] functions for disks creation & opening.
    This functor requires several arguments:
    - [Clock] is a {!Mirage_clock.PCLOCK}. It will be used for the files and folders timestamps.
    - [Check] is a {!CHECKSUM}. It will be used for the integrity verifications.
    - [Block] is a {!Mirage_block.S}. It is the block device the filesystem will be built upon. *)
module KV (Clock : Mirage_clock.PCLOCK) (Check : CHECKSUM) (Block : Mirage_block.S) : sig
  type t
  (** Representation of the kv filesystem. *)

  type error =
    [ `Read of Block.error (** Mirage_block default errors *)
    | `Write of Block.write_error (** Mirage_block default write errors *)
    | Mirage_kv.error
    | Mirage_kv.write_error
    | `Invalid_checksum of Int64.t
      (** Integrity checksum failed for the returned sector id, indicates that the sector was corrupted/probably not written *)
    | `All_generations_corrupted
      (** None of the generations could be retrieved, disk is beyond repair *)
    | `Disk_not_formatted (** The disk is not formatted or the header was corrupted *)
    | `Wrong_page_size of int
      (** The disk header gives a different page size than what was expected by the disk *)
    | `Wrong_disk_size of Int64.t
      (** The disk header gives a different disk size than what was expected by the disk *)
    | `Wrong_checksum_algorithm of string * int
      (** The disk header gives a different algorithm name/size than what was expected by the checksum parameter *)
    | `Unsupported_operation of string (** Operation not implemented yet *)
    ]
  (** The type for errors. *)

  type write_error = error
  (** The type for write errors (See {!error}). *)

  val format : Block.t -> (t, write_error) Lwt_result.t
  (** [format block] will format the [block] device into a usable filesystem [t]. *)

  val connect : Block.t -> (t, error) Lwt_result.t
  (** [connect block] connects to the filesystem [t] stored in the [block] device. *)

  val free_space : t -> int64
  (** [free_space t] returns the number of unused sectors of the filesystem [t]. *)

  include
    Mirage_kv.RW
      with type t := t
       and type error := error
       and type write_error := write_error
end

(** {2:store-fs FS, filesystem approach} *)

(** Filesystem implementation with a lower-level approach.

    It was written specifically to allow {!Index} and {!Irmin_pack} to run on Mirage block devices.
    This functor requires several arguments:
    - [Clock] is a {!Mirage_clock.PCLOCK}. It will be used for the files and folders timestamps.
    - [Check] is a {!CHECKSUM}. It will be used for the integrity verifications.
    - [Block] is a {!Mirage_block.S}. It is the block device the filesystem will be built upon. *)
module FS (Clock : Mirage_clock.PCLOCK) (Check : CHECKSUM) (Block : Mirage_block.S) : sig
  type error =
    [ `All_generations_corrupted
      (** None of the generations could be retrieved, disk is beyond repair *)
    | `Disk_is_full (** The disk does not have enough available space left *)
    | `Disk_not_formatted (** The disk is not formatted or the header was corrupted *)
    | `Invalid_checksum of Int64.t
      (** Integrity checksum failed for the returned sector id, indicates that the sector was corrupted/probably not written *)
    | `Read of Block.error (** Mirage_block default errors *)
    | `Write of Block.write_error (** Mirage_block default write errors *)
    | `Wrong_checksum_algorithm of string * int
      (** The disk header gives a different algorithm name/size than what was expected by the checksum functor *)
    | `Wrong_disk_size of Int64.t
      (** The disk header gives a different disk size than what was expected by the disk *)
    | `Wrong_page_size of int
      (** The disk header gives a different page size than what was expected by the disk *)
    ]
  (** The type for errors. *)

  val pp_error : Format.formatter -> error -> unit
  (** [pp_error] is a pretty printer for the type [error]. *)

  exception Fs of error
  (** Exception for filesystem errors. All of the functions below may raise this exception. *)

  type t
  (** Representation of the filesystem. *)

  val format : Block.t -> t Lwt.t
  (** [format block] will format the [block] device. *)

  val connect : Block.t -> t Lwt.t
  (** [connect block] connects to the {{!t} [filesystem]} stored in the [block] device. *)

  val flush : t -> unit Lwt.t
  (** [flush t] forces the flush of all the pending writes to the disk.
      This must be called after a series of update operations to guarantee
      that the changes are committed to disk. *)

  type file
  (** Representation of the filesystem file handles. *)

  (** {3 Read-only functions} *)

  val filename : file -> string
  (** [filename file] returns the name of the given {!file}. *)

  val size : file -> int Lwt.t
  (** [size file] returns the size of the given {!file}. *)

  val exists : t -> string -> [> `Dictionary | `Value ] option
  (** [exists t path] returns:
      - [None] if [path] is not associated to anything in the filesystem {!t}.
      - [Some `Dictionary] if [path] is a folder/dictionary.
      - [Some `Value] if [path] is a file. *)

  val find : t -> string -> file option
  (** [find t path] returns the [file] associated to [path] in the filesystem {!t}. Result is:
      - [None] if [path] is missing or points to a dictionary.
      - [Some file] if [path] is associated to a [file]. *)

  (** {3 Read-write functions} *)

  val touch : t -> string -> string -> file Lwt.t
  (** [touch t path str] creates a {!file} containing [str], at [path] in the filesystem {!t},
      and returns its handle. *)

  val append_substring : file -> string -> off:int -> len:int -> unit Lwt.t
  (** [append_substring file s ~off ~len] appends to [file] the substring of [s],
      starting at the offset [off] and with length [len]. *)

  val blit_from_string : file -> off:int -> len:int -> string -> unit Lwt.t
  (** [blit_from_string file ~off ~len s] replaces [len] bytes of [file] at the offset [off] with
      the content of [s]. If it would overflow the length of [file], then the remaining bytes will
      be appended at the end of the file. *)

  val blit_to_bytes : file -> off:int -> len:int -> bytes -> int Lwt.t
  (** [blit_to_bytes file ~off ~len b] copies [len] characters starting at the offset [off] of [file]
      into [bytes]. Returns the number of bytes read. *)

  val remove : t -> string -> unit Lwt.t
  (** [remove disk path] removes the [path] from the filesystem {!t}.
      If [path] was associated with a file, the file will be removed. If [path] was
      associated to a folder/dictionary, it will recursively remove all of its files. *)

  val rename : t -> src:string -> dst:string -> unit Lwt.t
  (** [rename disk ~src ~dst] renames a dictionary/file found under the path [src], moving it to the path [dst].
      If [dst] was previously a folder, all of its files will be removed.
      Any active [file] handle to [src] will be implicitly renamed. *)
end

(** {2:general_fun Configuration infos} *)

type config =
  { disk_size : Int64.t (** Number of pages of the disk *)
  ; page_size : int (** Size of the disk's pages in bytes *)
  ; checksum_algorithm : string (** Name of the checksum used by the disk *)
  ; checksum_byte_size : int (** Byte size of the checksum used by the disk *)
  }
(** Representation of a Notafs filesystem configuration. *)

val get_config
  :  (module Mirage_block.S with type t = 'block)
  -> 'block
  -> (config, [> `Disk_not_formatted ]) result Lwt.t
(** [get_config block] reads the configuration of the filesystem stored
    on the [block] device, if it was previously formatted.
    Otherwise, returns the error [`Disk_not_formatted].

    After formatting a disk, it is not possible to resize the block device,
    change its page size, or switch to a different checksum algorithm. *)
