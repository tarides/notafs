module Make (B : Context.A_DISK) : sig
  module Schema : module type of Schema.Make (B)
  module Sector = Schema.Sector

  type t = Sector.t
  type 'a io := ('a, B.error) Lwt_result.t

  val magic: int64
  val get_magic : t -> int64 io
  val get_disk_size : t -> int64 io
  val get_page_size : t -> int io
  val get_roots : t -> int io
  val create : disk_size:int64 -> page_size:int -> Sector.t io
end = struct
  module Schema = Schema.Make (B)
  module Sector = Schema.Sector
  open Lwt_result.Syntax

  type t = Sector.t

  type schema =
    { magic : int64 Schema.field
    ; disk_size : int64 Schema.field
    ; page_size : int Schema.field
    ; roots : int Schema.field
    }

  let { magic; disk_size; page_size; roots } =
    Schema.define
    @@
    let open Schema.Syntax in
    let+ magic = Schema.uint64
    and+ disk_size = Schema.uint64
    and+ page_size = Schema.uint32
    and+ roots = Schema.uint32 in
    { magic; disk_size; page_size; roots }

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
  end

  let magic = 0x534641544F4EL (* NOTAFS *)

  let create ~disk_size ~page_size =
    let* s = Sector.create ~at:(Sector.root_loc @@ B.Id.of_int 0) () in
    let* () = set_magic s magic in
    let* () = set_disk_size s disk_size in
    let* () = set_page_size s page_size in
    let+ () = set_roots s 4 in
    s
end
