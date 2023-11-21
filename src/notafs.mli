(* checksum *)
module type CHECKSUM = sig
  include Checksum.S
end

module No_checksum : CHECKSUM
module Adler32 : CHECKSUM

(* KV *)
module KV (Clock : Mirage_clock.PCLOCK) (Check : CHECKSUM) (Block : Mirage_block.S) : sig
  type t

  type error =
    [ `Read of Block.error
    | `Write of Block.write_error
    | Mirage_kv.error
    | Mirage_kv.write_error
    | `Invalid_checksum of Int64.t
    | `All_generations_corrupted
    | `Disk_not_formatted
    | `Wrong_page_size of int
    | `Wrong_disk_size of Int64.t
    | `Wrong_checksum_algorithm of string * int
    | `Unsupported_operation of string
    ]

  type write_error = error

  include
    Mirage_kv.RW
      with type t := t
       and type error := error
       and type write_error := write_error

  val format : Block.t -> (t, write_error) Lwt_result.t
  val connect : Block.t -> (t, error) Lwt_result.t
  val free_space : t -> int64
end

(* FS *)
module FS (Clock : Mirage_clock.PCLOCK) (Check : CHECKSUM) (Block : Mirage_block.S) : sig
  type error =
    [ `All_generations_corrupted
    | `Disk_is_full
    | `Disk_not_formatted
    | `Invalid_checksum of Int64.t
    | `Read of Block.error
    | `Write of Block.write_error
    | `Wrong_checksum_algorithm of string * int
    | `Wrong_disk_size of Int64.t
    | `Wrong_page_size of int
    ]

  val pp_error : Format.formatter -> error -> unit

  exception Fs of error

  type t

  val format : Block.t -> t Lwt.t
  val connect : Block.t -> t Lwt.t
  val flush : t -> unit Lwt.t

  type file

  (* read-only operations *)
  val filename : file -> string
  val size : file -> int Lwt.t
  val exists : t -> string -> [> `Dictionary | `Value ] option
  val find : t -> string -> file option

  (* write operations *)
  val touch : t -> string -> string -> file Lwt.t
  val append_substring : file -> string -> off:int -> len:int -> unit Lwt.t
  val blit_from_string : file -> off:int -> len:int -> string -> unit Lwt.t
  val blit_to_bytes : file -> off:int -> len:int -> bytes -> int Lwt.t
  val remove : t -> string -> unit Lwt.t
  val rename : t -> src:string -> dst:string -> unit Lwt.t
end

val get_config
  :  (module Mirage_block.S with type t = 'a)
  -> 'a
  -> (Header.config, [> `Disk_not_formatted ]) result Lwt.t
