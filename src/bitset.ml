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

  let get_ptr_size () = Sector.ptr_size

  let get_nb_children page_size =
    let incr = get_ptr_size () in
    page_size / incr

  let get value offset = value land (1 lsl offset)

  let get_flag t i =
    let pos = i / 8 in
    let* value = Sector.get_uint8 t pos in
    let offset = i mod 8 in
    let flag = get value offset in
    Lwt_result.return flag

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
      let new_leaves =
        if child_ind = (nb_leaves - 1) / group_size
        then ((nb_leaves - 1) mod group_size) + 1
        else group_size
      in
      let* child = Sector.get_child t (incr * child_ind) in
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

  let free_range_leaf t (ind, len) =
    assert (ind / 8 = (ind + len - 1) / 8) ;
    let rec set_used cur_ind value =
      if cur_ind = ind + len
      then value
      else (
        let offset = cur_ind mod 8 in
        let flag = value land (1 lsl offset) in
        assert (flag > 0) ;
        let update = value lxor (1 lsl offset) in
        set_used (cur_ind + 1) update)
    in
    let pos = ind / 8 in
    let* value = Sector.get_uint8 t pos in
    let update = set_used ind value in
    Sector.set_uint8 t pos update

  let free_range t (id, len) =
    let page_size = get_page_size () in
    let bit_size = page_size * 8 in
    let rec split leaf (ind, len) =
      match len with
      | 0 -> Lwt_result.return ()
      | len ->
        let cur_len = min len (8 - (ind mod 8)) in
        let next_ind = ind + cur_len in
        let next_len = len - cur_len in
        let* next_leaf =
          if ind / bit_size <> next_ind / bit_size
          then get_leaf t next_ind
          else Lwt_result.return leaf
        in
        let* () = free_range_leaf leaf (ind mod bit_size, cur_len) in
        split next_leaf (next_ind, next_len)
    in
    let ind = Int64.to_int @@ B.Id.to_int64 id in
    let* leaf = get_leaf t ind in
    split leaf (ind, len)

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

  let use_range_leaf t (ind, len) =
    assert (ind / 8 = (ind + len - 1) / 8) ;
    let rec set_used cur_ind value =
      if cur_ind = ind + len
      then value
      else (
        let offset = cur_ind mod 8 in
        let flag = value land (1 lsl offset) in
        assert (flag = 0) ;
        let update = value lor (1 lsl offset) in
        set_used (cur_ind + 1) update)
    in
    let pos = ind / 8 in
    let* value = Sector.get_uint8 t pos in
    let update = set_used ind value in
    Sector.set_uint8 t pos update

  let use_range t (id, len) =
    let page_size = get_page_size () in
    let bit_size = page_size * 8 in
    let rec split leaf (ind, len) =
      match len with
      | 0 -> Lwt_result.return ()
      | len ->
        let cur_len = min len (8 - (ind mod 8)) in
        let next_ind = ind + cur_len in
        let next_len = len - cur_len in
        let* next_leaf =
          if ind / bit_size <> next_ind / bit_size
          then get_leaf t next_ind
          else Lwt_result.return leaf
        in
        let* () = use_range_leaf leaf (ind mod bit_size, cur_len) in
        split next_leaf (next_ind, next_len)
    in
    let ind = Int64.to_int @@ B.Id.to_int64 id in
    let* leaf = get_leaf t ind in
    split leaf (ind, len)

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

  let pop_front t bitset_start quantity =
    let page_size = get_page_size () in
    let bit_size = page_size * 8 in
    let nb_sectors = Int64.to_int B.nb_sectors in
    let start_ind = Int64.to_int @@ B.Id.to_int64 bitset_start in
    let start_ind = start_ind - (start_ind mod 8) in
    let rec do_pop_front ind lst leaf =
      assert (List.length lst < quantity) ;
      let pos = ind mod bit_size / 8 in
      let* value = Sector.get_uint8 leaf pos in
      let needed = quantity - List.length lst in
      let rec get_id cur_ind needed lst =
        if cur_ind >= nb_sectors || cur_ind = ind + 8 || needed = 0
        then Lwt_result.return lst
        else (
          let flag = get value (cur_ind mod 8) in
          if flag = 0
          then
            let* () = use_leaf leaf (cur_ind mod bit_size) in
            get_id (cur_ind + 1) (needed - 1) (cur_ind :: lst)
          else get_id (cur_ind + 1) needed lst)
      in
      let* lst = get_id ind needed lst in
      if List.length lst = quantity
      then Lwt_result.return (List.nth lst 0, lst)
      else if ind < start_ind && ind + 8 >= start_ind
      then Lwt_result.fail `Disk_is_full
      else (
        let new_ind = if ind + 8 >= nb_sectors then 0 else ind + 8 in
        let* leaf =
          if ind / bit_size <> new_ind / bit_size
          then get_leaf t new_ind
          else Lwt_result.return leaf
        in
        do_pop_front new_ind lst leaf)
    in
    let* start_leaf = get_leaf t start_ind in
    let* new_bitset_start, lst = do_pop_front start_ind [] start_leaf in
    let new_bitset_start = B.Id.of_int new_bitset_start in
    let lst = List.rev lst in
    let rec get_range_list cur = function
      | id :: res ->
        (match cur with
         | (top, range) :: rest_cur ->
           if top + range = id
           then get_range_list ((top, range + 1) :: rest_cur) res
           else get_range_list ((id, 1) :: cur) res
         | [] -> get_range_list [ id, 1 ] res)
      | [] -> cur
    in
    let lst = get_range_list [] lst in
    let lst = List.map (fun (id, range) -> B.Id.of_int id, range) lst in
    Lwt_result.return (lst, new_bitset_start)
end
