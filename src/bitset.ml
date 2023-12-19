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

  let free_leaf t i =
    let pos = i / 8 in
    let* value = Sector.get_uint8 t pos in
    let offset = i mod 8 in
    let flag = value land (1 lsl offset) in
    assert (flag > 0) ;
    let update = value lxor (1 lsl offset) in
    Sector.set_uint8 t pos update

  let free t i =
    (* Format.printf "Freeing %d@." i; *)
    let page_size = B.page_size in
    let rec reach_leaf t i ind =
      let pos = i / page_size in
      if pos < page_size - 1
      then
        let* child = Sector.get_child t pos in
        free_leaf child ind
      else
        let* child = Sector.get_child t pos in
        reach_leaf child pos ind
    in
    reach_leaf t i (i mod page_size)

  (* TODO: Optimize free_range to use less set_uint calls *)
  let rec free_range t (id, len) =
    if len = 0
    then Lwt_result.return ()
    else (
      let int_id = Int64.to_int (B.Id.to_int64 id) in
      let* () = free t int_id in
      free_range t (B.Id.succ id, len - 1))

  let use_leaf t i =
    let pos = i / 8 in
    let* value = Sector.get_uint8 t pos in
    let offset = i mod 8 in
    let flag = value land (1 lsl offset) in
    assert (flag = 0) ;
    let update = value lor (1 lsl offset) in
    Sector.set_uint8 t pos update

  let use t i =
    (* Format.printf "Using %d@." i; *)
    let page_size = B.page_size in
    let rec reach_leaf t i ind =
      let pos = i / page_size in
      if pos < page_size - 1
      then
        let* child = Sector.get_child t pos in
        use_leaf child ind
      else
        let* child = Sector.get_child t pos in
        reach_leaf child pos ind
    in
    reach_leaf t i (i mod page_size)

  (* TODO: Optimize use_range to use less set_uint calls *)
  let rec use_range t (id, len) =
    if len = 0
    then Lwt_result.return ()
    else (
      let int_id = Int64.to_int (B.Id.to_int64 id) in
      let* () = use t int_id in
      use_range t (B.Id.succ id, len - 1))

  let create_leaf () =
    let* t = Sector.create () in
    let sz = B.page_size in
    let rec init = function
      | i when i >= sz -> Lwt_result.return ()
      | i ->
        let* () = Sector.set_uint8 t i 0 in
        init (i + 1)
    in
    let+ () = init 0 in
    t

  let rec create_parent size length =
    let* t = Sector.create () in
    let rec loop pos =
      Format.printf "Looping over position %d@." pos; 
      match pos with 
      | pos when pos < 0 -> Lwt_result.return ()
      | pos when pos >= length - 1 ->
        let* parent = create_parent pos length in
        let* () = Sector.set_child t pos parent in
        loop (length - 2)
      | pos ->
        let* leaf = create_leaf () in
        let* () = Sector.set_child t pos leaf in
        loop (pos - 1)
    in
    let pos = size / length in
    let+ () = loop pos in
    t

  let create () =
    let nb_sectors = Int64.to_int B.nb_sectors in
    let page_size = B.page_size in
    Format.printf "size: %d, number: %d@." page_size nb_sectors;
    let* root = create_parent nb_sectors page_size in
    Format.printf "Root done@.";
    let rec init_res = function
      | num when num < 0 -> Lwt_result.return ()
      | num ->
        let* () = use root num in
        init_res (num - 1)
    in
    let+ () = init_res 12 in
    Format.printf "Create done@.";
    root
end
