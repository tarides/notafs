module type DISK = sig
  include Mirage_block.S

  val discard : t -> Int64.t -> unit
  val flush : t -> unit
end

module type A_DISK = sig
  type read_error = private [> Mirage_block.error ]
  type write_error = private [> Mirage_block.write_error ]

  type error =
    [ `Read of read_error
    | `Write of write_error
    ]

  val page_size : int
  val nb_sectors : int64
  val read : Int64.t -> Cstruct.t -> (unit, [> `Read of read_error ]) Lwt_result.t
  val write : Int64.t -> Cstruct.t -> (unit, [> `Write of write_error ]) Lwt_result.t
  val discard : Int64.t -> unit
  val acquire_discarded : unit -> Int64.t list
  val flush : unit -> unit
end

let of_impl (type t) (module B : DISK with type t = t) (disk : t) : (module A_DISK) Lwt.t =
  let open Lwt.Syntax in
  let+ info = B.get_info disk in
  (module struct
    type read_error = B.error
    type write_error = B.write_error

    type error =
      [ `Read of read_error
      | `Write of write_error
      ]

    let page_size = info.sector_size
    let nb_sectors = info.size_sectors

    let read page_id cstruct =
      Lwt.map (Result.map_error (fun e -> `Read e)) @@ B.read disk page_id [ cstruct ]

    let write page_id cstruct =
      Lwt.map (Result.map_error (fun e -> `Write e)) @@ B.write disk page_id [ cstruct ]

    let discarded = ref []

    let discard page_id =
      discarded := page_id :: !discarded ;
      B.discard disk page_id

    let acquire_discarded () =
      let lst = !discarded in
      discarded := [] ;
      List.rev lst

    let flush () = B.flush disk
  end : A_DISK)
