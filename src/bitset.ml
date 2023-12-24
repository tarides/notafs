module Make (B : Context.A_DISK) = struct
  module Sector = Sector.Make (B)
  module Schema = Schema.Make (B)

  type t = Sector.t

  open Lwt_result.Syntax

  let get_page_size () = B.page_size

  let get_nb_leaves () =
    let nb_sectors = Int64.to_int B.nb_sectors in
    let page_size = get_page_size () in
    let bit_size = page_size * 8 in
    (nb_sectors + bit_size - 1) / bit_size

  let get_group_size nb_children nb_leaves =
    let rec get_group_size group_size =
      if group_size * nb_children >= nb_leaves
      then group_size
      else get_group_size (group_size * nb_children)
    in
    get_group_size 1

  let get_ptr_size () =
    let pointer_size = Sector.ptr_size in
    let id_size = Sector.id_size in
    ((pointer_size + id_size) / 8) + 8

  let get_nb_children page_size =
    let incr = get_ptr_size () in
    page_size / incr

  let get t i =
    let pos = i / 8 in
    let* value = Sector.get_uint8 t pos in
    let offset = i mod 8 in
    let flag = value land (1 lsl offset) in
    Lwt_result.return (flag = 0)

  let get_leaf t i =
    let page_size = get_page_size () in
    let bit_size = page_size * 8 in
    let leaf_ind = i / bit_size in
    let nb_leaves = get_nb_leaves () in
    let nb_children = get_nb_children page_size in
    let incr = get_ptr_size () in
    let rec reach_leaf t leaf_ind nb_leaves =
      let group_size = get_group_size nb_children nb_leaves in
      let child_ind = leaf_ind / group_size in
      let* child = Sector.get_child t (incr * child_ind) in
      let new_leaves =
        if child_ind = (nb_leaves - 1) / group_size
        then ((nb_leaves - 1) mod group_size) + 1
        else group_size
      in
      if group_size = 1
      then Lwt_result.return child
      else reach_leaf child (leaf_ind mod group_size) new_leaves
    in
    reach_leaf t leaf_ind nb_leaves

  let free_leaf t i =
    let pos = i / 8 in
    let* value = Sector.get_uint8 t pos in
    let offset = i mod 8 in
    let flag = value land (1 lsl offset) in
    assert (flag > 0) ;
    let update = value lxor (1 lsl offset) in
    Sector.set_uint8 t pos update

  let free t i =
    let page_size = get_page_size () in
    let bit_size = page_size * 8 in
    let* leaf = get_leaf t i in
    free_leaf leaf (i mod bit_size)

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
    let page_size = get_page_size () in
    let bit_size = page_size * 8 in
    let* leaf = get_leaf t i in
    use_leaf leaf (i mod bit_size)

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
    let sz = get_page_size () in
    let rec init = function
      | i when i >= sz -> Lwt_result.return ()
      | i ->
        let* () = Sector.set_uint8 t i 0 in
        init (i + 1)
    in
    let+ () = init 0 in
    t

  let rec create_parent nb_leaves page_size =
    let* parent = create_leaf () in
    let incr = get_ptr_size () in
    let nb_children = get_nb_children page_size in
    let group_size = get_group_size nb_children nb_leaves in
    if group_size = 1
    then (
      let rec init_leaves cur_index = function
        | -1 -> Lwt_result.return ()
        | nb_leaf ->
          let* leaf = create_leaf () in
          let* () = Sector.set_child parent cur_index leaf in
          init_leaves (cur_index + incr) (nb_leaf - 1)
      in
      let+ () = init_leaves 0 (nb_leaves - 1) in
      parent)
    else (
      let rec init_parent index = function
        | 0 -> Lwt_result.return ()
        | nb_leaves ->
          let group = min nb_leaves group_size in
          let* child = create_parent group page_size in
          let* () = Sector.set_child parent index child in
          init_parent (index + incr) (nb_leaves - group)
      in
      let+ () = init_parent 0 nb_leaves in
      parent)

  let create () =
    let page_size = get_page_size () in
    let nb_leaves = get_nb_leaves () in
    let* root = create_parent nb_leaves page_size in
    let rec init_res = function
      | num when num < 0 -> Lwt_result.return ()
      | num ->
        let* () = use root num in
        init_res (num - 1)
    in
    let+ () = init_res 12 in
    root
end
