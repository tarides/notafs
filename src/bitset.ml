module Make (B : Context.A_DISK) = struct
  module Sector = Sector.Make (B)
  module Schema = Schema.Make (B)

  type t = Sector.t

  open Lwt_result.Syntax

  let get t i =
    let pos = i / 8 in
    let* value = Sector.get_uint8 t pos in
    let offset = i mod 8 in
    let flag = value land (1 lsl offset) in
    Lwt_result.return (flag = 0)

  let free t i =
    let pos = i / 8 in
    let* value = Sector.get_uint8 t pos in
    let offset = i mod 8 in
    let flag = value land (1 lsl offset) in
    assert (flag > 0) ;
    let update = value lxor (1 lsl offset) in
    Sector.set_uint8 t pos update

  (* TODO: Optimize free_range to use less set_uint calls *)
  let rec free_range t (id, len) =
    if len = 0
    then Lwt_result.return ()
    else (
      let int_id = Int64.to_int (B.Id.to_int64 id) in
      let* () = free t int_id in
      free_range t (B.Id.succ id, len - 1))

  let use t i =
    let pos = i / 8 in
    let* value = Sector.get_uint8 t pos in
    let offset = i mod 8 in
    let flag = value land (1 lsl offset) in
    assert (flag = 0) ;
    let update = value lor (1 lsl offset) in
    Sector.set_uint8 t pos update

  (* TODO: Optimize use_range to use less set_uint calls *)
  let rec use_range t (id, len) =
    if len = 0
    then Lwt_result.return ()
    else (
      let int_id = Int64.to_int (B.Id.to_int64 id) in
      let* () = use t int_id in
      use_range t (B.Id.succ id, len - 1))

  let create () =
    let* t = Sector.create () in
    let sz = B.page_size in
    let rec init = function
      | i when i >= sz -> Lwt_result.return ()
      | i ->
        let* () = Sector.set_uint8 t i 0 in
        init (i + 1)
    in
    let rec init_reserved = function
      | i when i < 0 -> Lwt_result.return ()
      | i ->
        let* () = use t i in
        init_reserved (i - 1)
    in
    let* () = init 0 in
    let+ () = init_reserved 12 in
    t
end
