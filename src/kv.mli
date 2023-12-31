module Make
    (Clock : Mirage_clock.PCLOCK)
    (Check : Checksum.S)
    (Block : Mirage_block.S) : sig
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

  (* many are unnecessary, clean me up *)
  val flush : t -> (unit, write_error) Lwt_result.t
  val clear : t -> (unit, write_error) Lwt_result.t
  val disk_space : t -> int64
  val free_space : t -> int64
  val page_size : t -> int
end
