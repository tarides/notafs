type config =
  { disk_size : Int64.t
  ; page_size : int
  ; checksum_algorithm : string
  ; checksum_byte_size : int
  }

module type CONFIG = sig
  type error
  type 'a io := ('a, error) Lwt_result.t

  val load_config : unit -> config io
end

module Make (B : Context.A_DISK) : sig
  module Schema : module type of Schema.Make (B)
  module Sector = Schema.Sector

  type error = B.error
  type t = Sector.t
  type 'a io := ('a, error) Lwt_result.t

  val magic : int64
  val get_magic : t -> int64 io
  val get_disk_size : t -> int64 io
  val get_page_size : t -> int io
  val get_roots : t -> int io
  val get_format_uid : t -> int64 io
  val create : disk_size:int64 -> page_size:int -> Sector.t io
  val load : unit -> Sector.t io
  val load_config : unit -> config io
end = struct
  module Schema = Schema.Make (B)
  module Sector = Schema.Sector
  open Lwt_result.Syntax

  type error = B.error
  type t = Sector.t

  type schema =
    { magic : int64 Schema.field
    ; disk_size : int64 Schema.field
    ; page_size : int Schema.field
    ; roots : int Schema.field
    ; checksum_byte_size : int Schema.field
    ; checksum_algorithm : int64 Schema.field
    ; format_uid : int64 Schema.field
    }

  let { magic
      ; disk_size
      ; page_size
      ; roots
      ; checksum_byte_size
      ; checksum_algorithm
      ; format_uid
      }
    =
    Schema.define
    @@
    let open Schema.Syntax in
    let+ magic = Schema.uint64
    and+ disk_size = Schema.uint64
    and+ page_size = Schema.uint32
    and+ roots = Schema.uint32
    and+ checksum_byte_size = Schema.uint8
    and+ checksum_algorithm = Schema.uint64
    and+ format_uid = Schema.uint64 in
    { magic
    ; disk_size
    ; page_size
    ; roots
    ; checksum_byte_size
    ; checksum_algorithm
    ; format_uid
    }

  include struct
    open Schema.Infix

    let set_magic t v = t.@(magic) <- v
    let get_magic t = t.@(magic)
    let set_disk_size t v = t.@(disk_size) <- v
    let get_disk_size t = t.@(disk_size)
    let set_page_size t v = t.@(page_size) <- v
    let get_page_size t = t.@(page_size)
    let set_roots t v = t.@(roots) <- v
    let get_roots t = t.@(roots)
    let set_format_uid t v = t.@(format_uid) <- v
    let get_format_uid t = t.@(format_uid)
  end

  let int64_of_string s =
    let open Int64 in
    let ( + ) = logor in
    let get i =
      try shift_left (of_int (Char.code s.[i])) (i * 8) with
      | _ -> 0L
    in
    get 0 + get 1 + get 2 + get 3 + get 4 + get 5 + get 6 + get 7

  let string_of_int64 x =
    let open Int64 in
    let s = Bytes.create 8 in
    for i = 0 to 7 do
      Bytes.set s i @@ Char.chr (to_int (logand 0xFFL (shift_right x (i * 8))))
    done ;
    Bytes.unsafe_to_string s

  let magic = 0x534641544F4EL (* NOTAFS *)
  let () = assert (magic = int64_of_string "NOTAFS")

  let random_format_uid () =
    let cstruct = Cstruct.create B.page_size in
    let+ () = B.read (B.Id.of_int 0) cstruct in
    let acc = ref 0L in
    for i = 0 to (Cstruct.length cstruct / 8) - 1 do
      let x = Cstruct.HE.get_uint64 cstruct (i * 8) in
      acc := Int64.logxor !acc x
    done ;
    !acc

  let create ~disk_size ~page_size =
    let open Schema.Infix in
    let* format_uid = random_format_uid () in
    let* s = Sector.create ~at:(Sector.root_loc @@ B.Id.of_int 0) () in
    let* () = set_magic s magic in
    let* () = set_disk_size s disk_size in
    let* () = set_page_size s page_size in
    let* () = set_roots s 4 in
    let* () = set_format_uid s format_uid in
    let* () = s.@(checksum_byte_size) <- B.Check.byte_size in
    let+ () = s.@(checksum_algorithm) <- int64_of_string B.Check.name in
    s

  let load_config () =
    let open Schema.Infix in
    let* s = Sector.load_root ~check:false (B.Id.of_int 0) in
    let* magic' = get_magic s in
    let* () =
      if magic' <> magic
      then Lwt_result.fail `Disk_not_formatted
      else Lwt_result.return ()
    in
    let* disk_size = get_disk_size s in
    let* page_size = get_page_size s in
    let* checksum_byte_size = s.@(checksum_byte_size) in
    let+ checksum_algorithm = s.@(checksum_algorithm) in
    let config =
      { disk_size
      ; page_size
      ; checksum_byte_size
      ; checksum_algorithm = string_of_int64 checksum_algorithm
      }
    in
    config, s

  let load () =
    let* config, s = load_config () in
    let* () =
      if config.page_size <> B.page_size
      then Lwt_result.fail (`Wrong_page_size config.page_size)
      else Lwt_result.return ()
    in
    let* () =
      if config.disk_size <> B.nb_sectors
      then Lwt_result.fail (`Wrong_disk_size config.disk_size)
      else Lwt_result.return ()
    in
    let* () =
      if config.checksum_byte_size <> B.Check.byte_size
         || config.checksum_algorithm <> B.Check.name
      then
        Lwt_result.fail
          (`Wrong_checksum_algorithm
            (config.checksum_algorithm, config.checksum_byte_size))
      else Lwt_result.return ()
    in
    let+ () = Sector.verify_checksum s in
    s

  let load_config () =
    let+ config, _ = load_config () in
    config
end
