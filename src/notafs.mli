(** File system for Mirage block devices *)

(** {1:checksum Checksum interface & implementations} *)

(** Signature for checksum modules.

    Personnal implementations needs to follow it. *)
module type CHECKSUM = sig
  include Checksum.S
end

(** {b Default implementations.} *)

module Adler32 : CHECKSUM
(** [Adler32] implementation, built around [Checkseum]'s implementation. *)

module No_checksum : CHECKSUM
(** Fake implementation, does nothing.
    Use it only were integrity checks does not matter (mostly specific tests). *)

(** {1:stores Notafs stores} *)

(** The [Notafs] library provides several implementations, with the goal to be
    more adaptative to the needs of the users.

    To this day, there are 2 implementations:
    - {!KV}, a simple key-value implementations, which follows the [Mirage_kv] signatures.
      It should be the best entry point for anyone wishing to use [Notafs] alongside [mirage-os].
    - {!FS}, a more complex implementation, with more options drifting away from a
      key-value store for a more usual file system approach. *)

(** {2:store-kv KV, Key-Value store} *)

(** Simple key-value implementation, based on [Mirage_KV.RO] & [Mirage_KV.RW].

    It also provides a format & connect functions for disks creation & opening.
    It is a functor and therefore requires several arguments:

    - [Clock] is a [Mirage_clock.PCLOCK]. It will be used for the files and folders timestamps.
    - [Check] is a {!CHECKSUM}. It will be used for the integrity verifications.
    - [Block] is a [Mirage_block.S]. It is the device block the disk will be built upon. *)
module KV (Clock : Mirage_clock.PCLOCK) (Check : CHECKSUM) (Block : Mirage_block.S) : sig
  type t
  (** Representation of the kv disk. *)

  type error =
    [ `Read of Block.error (** Mirage_block default errors *)
    | `Write of Block.write_error (** Mirage_block default write errors *)
    | Mirage_kv.error
    | Mirage_kv.write_error
    | `Invalid_checksum of Int64.t
      (** Integrity checksum failed for the returned sector id, indicates that the sector was corrupted/probably not written *)
    | `All_generations_corrupted
      (** None of the generations could be retrieved, disk is probably beyond repairs *)
    | `Disk_not_formatted (** The disk is not formatted or the header was corrupted *)
    | `Wrong_page_size of int
      (** The disk header gives a different page size from what was expected by the block functor *)
    | `Wrong_disk_size of Int64.t
      (** The disk header gives a different disk size from what was expected by the block functor *)
    | `Wrong_checksum_algorithm of string * int
      (** The disk header gives a different algorithm name/size from what was expected by the checksum functor *)
    | `Unsupported_operation of string (** Operation not implemented yet *)
    ]
  (** The type for errors. *)

  type write_error = error
  (** The type for write errors (See {!error}). *)

  (** See [Mirage_kv.RW]'s documentation for more informations. *)
  include
    Mirage_kv.RW
      with type t := t
       and type error := error
       and type write_error := write_error

  val format : Block.t -> (t, write_error) Lwt_result.t
  (** [format block] will format [block] into a usable disk and connects to it.

      Returns [Ok disk] on success, [Error err] upon meeting error [err] *)

  val connect : Block.t -> (t, error) Lwt_result.t
  (** [connect block] connects to the disk [t] stored in [block].

      Returns [Ok disk] on success, [Error err] upon meeting error [err] *)

  val free_space : t -> int64
  (** [free_space t] returns the space available on the disk [t]. *)
end

(** {2:store-fs FS, file system approach} *)

(** File-system implementation with a more low-level approach.

    It was also adapted to
    easily allow the usage of the [irmin] project on top of it.
    It is a functor and therefore requires several arguments:
    - [Clock] is a [Mirage_clock.PCLOCK]. It will be used for the files and folders timestamps.
    - [Check] is a {!CHECKSUM}. It will be used for the integrity verifications.
    - [Block] is a [Mirage_block.S]. It is the device block the disk will be built upon. *)
module FS (Clock : Mirage_clock.PCLOCK) (Check : CHECKSUM) (Block : Mirage_block.S) : sig
  type error =
    [ `All_generations_corrupted
      (** None of the generations could be retrieved, disk is probably beyond repairs *)
    | `Disk_is_full (** The disk does not have enough available space left *)
    | `Disk_not_formatted (** The disk is not formatted or the header was corrupted *)
    | `Invalid_checksum of Int64.t
      (** Integrity checksum failed for the returned sector id, indicates that the sector was corrupted/probably not written *)
    | `Read of Block.error (** Mirage_block default errors *)
    | `Write of Block.write_error (** Mirage_block default write errors *)
    | `Wrong_checksum_algorithm of string * int
      (** The disk header gives a different algorithm name/size from what was expected by the checksum functor *)
    | `Wrong_disk_size of Int64.t
      (** The disk header gives a different disk size from what was expected by the block functor *)
    | `Wrong_page_size of int
      (** The disk header gives a different page size from what was expected by the block functor *)
    ]
  (** The type for errors. *)

  val pp_error : Format.formatter -> error -> unit
  (** [pp_error] is a pretty printer for the type [error] *)

  exception Fs of error
  (** Exception for file-system errors *)

  type t
  (** Representation of the fs disk. *)

  val format : Block.t -> t Lwt.t
  (** [format block] will format [block] into a usable disk and connects to it.

      Returns the formatted {{!t} [disk]} on success, raises the exception {{!Fs} [Fs err]} upon meeting the error [err]. *)

  val connect : Block.t -> t Lwt.t
  (** [connect block] connects to the {{!t} [disk]} stored in [block].

      Returns the {{!t} [disk]} on success, raises the exception {{!Fs} [Fs err]} upon meeting the error [err]. *)

  val flush : t -> unit Lwt.t
  (** [flush disk] forces the flush of all the sectors of [disk].

      Returns [unit] on success, raises the exception {{!Fs} [Fs err]} upon meeting the error [err]. *)

  type file
  (** Representation of the fs files. *)

  (** {3 Read-only functions} *)

  val filename : file -> string
  (** [filename file] returns the name of the given {{!file} [file]}. *)

  val size : file -> int Lwt.t
  (** [size file] returns the size of the given {{!file} [file]}. *)

  val exists : t -> string -> [> `Dictionary | `Value ] option
  (** [exists disk path] returns:
      - [None] if [path] is not associated to anything in {{!t} [disk]}.
      - [Some `Dictionary] if [path] is associated to a folder/dictionary in {{!t} [disk]}.
      - [Some `Value] if [path] is associated to a file in {{!t} [disk]}. *)

  val find : t -> string -> file option
  (** [find disk path] returns the [file] associated to [path] if any. Result is:
      - [None] if [path] is not associated to anything or to a dictionary in {{!t} [disk]}.
      - [Some file] if [path] is associated to a [file] in {{!t} [disk]}.

      Raises [File_expected] if [path] is associated to a folder/dictionary in {{!t} [disk]} *)

  val touch : t -> string -> string -> file Lwt.t
  (** [touch disk path s] creates a {{!file} [file]} associated to [path] with the content [s]
      and returns it. *)

  (** {3 Read-write functions} *)

  val append_substring : file -> string -> off:int -> len:int -> unit Lwt.t
  (** [append_substring file s ~off ~len] appends to [file] the substring [s'] created from [s],
      starting at the offset [off] and with length [len]. *)

  val blit_from_string : file -> off:int -> len:int -> string -> unit Lwt.t
  (** [blit_from_string file ~off ~len s] replaces [len] bytes of [file] at the offset [off] with
      the content of [s]. If it would overflow the length of [file], then the remaining bytes will
      be appened at the if of the file. *)

  val blit_to_bytes : file -> off:int -> len:int -> bytes -> int Lwt.t
  (** [blit_to_bytes file ~off ~len b] copies [len] bytes starting at the offset [off] from [file]
      into a bytes object and returns it. *)

  val remove : t -> string -> unit Lwt.t
  (** [remove disk path] removes all associations with the path [path] in {{!t} [disk]}.
      If [path] was associated to a file, the file will be freed and removed. If it was
      associated to a folder/dictionary, it will be removed and will recursively free all
      the files in contains. *)

  val rename : t -> src:string -> dst:string -> unit Lwt.t
  (** [rename disk ~src ~dst] renames a dictionary/file found under the path [src], moving it to the path [dst].
      - if [src] was associated with a file, it will be automaticaly updated, and all pre-fetched [file] objects
        will now affect the file under it's new path.
      - if [src] was associated with a folder, this will also be done recursively on all of it's sub-files and
        sub directories.
        Also:
      - if [dst] was associated with a file, it will be freed & all existing [file] objects associated
        with it will be invalidated.
      - if [dst] was associated with a folder, this will also be done recursively on all of it's sub-files and
        sub directories. *)
end

(** {2:general_fun Configuration infos} *)

type config =
  { disk_size : Int64.t (** size of the disk in pages *)
  ; page_size : int (** size of the disk's pages in bytes *)
  ; checksum_algorithm : string (** name of the checksum used by the disk *)
  ; checksum_byte_size : int (** byte size of the checksum used by the disk *)
  }
(** Representation of a notafs's disk configuration. *)

val get_config
  :  (module Mirage_block.S with type t = 'a)
  -> 'a
  -> (config, [> `Disk_not_formatted ]) result Lwt.t
(** [get_config block] reads the configuration of the disk contained in [block] if it was
    previously formatted. Otherwise, returns and error [`Disk_not_formatted]. *)
