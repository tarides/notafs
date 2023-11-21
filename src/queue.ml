module Make (B : Context.A_DISK) = struct
  module Sector = Sector.Make (B)
  module Schema = Schema.Make (B)

  type t = Sector.t

  type schema =
    { height : int Schema.field
    ; children : Schema.child Schema.dyn_array (* if height > 0 *)
    ; free_sectors : Schema.id Schema.dyn_array (* if height = 0 *)
    }

  let ({ height; children; free_sectors } as schema) =
    Schema.define
    @@
    let open Schema.Syntax in
    let* height = Schema.uint8 in
    let| children = Schema.array Schema.child
    and| free_sectors = Schema.array Schema.id in
    { height; children; free_sectors }

  include struct
    open Schema
    open Schema.Infix

    let set_height t v = t.@(height) <- v
    let height t = t.@(height)
    let nb_children t = t.@(children.length)
    let set_nb_children t v = t.@(children.length) <- v
    let set_child t i v = t.@(nth children i) <- v
    let get_child t i = t.@(nth children i)
    let nb_free_sectors t = t.@(free_sectors.length)
    let set_nb_free_sectors t v = t.@(free_sectors.length) <- v
    let get_free_sector t i = t.@(nth free_sectors i)
    let set_free_sector t i v = t.@(nth free_sectors i) <- v
  end

  open Lwt_result.Syntax

  let create () =
    let* t = Sector.create () in
    let* () = set_height t 0 in
    let+ () = set_nb_children t 0 in
    t

  type push_back =
    | Ok_push
    | Overflow of Sector.id list

  let rec do_push_back t (children : Sector.id list) =
    assert (children <> []) ;
    let* h = height t in
    if h = 0
    then begin
      let* n = nb_free_sectors t in
      let max_length = free_sectors.max_length in
      let rec go i = function
        | [] ->
          let+ () = set_nb_free_sectors t i in
          Ok_push
        | children when i >= max_length ->
          let+ () = set_nb_free_sectors t i in
          Overflow children
        | (child_ptr : Sector.id) :: children ->
          let* () = set_free_sector t i child_ptr in
          go (i + 1) children
      in
      go n children
    end
    else begin
      let* n = nb_children t in
      let max_length = schema.children.max_length in
      assert (n > 0) ;
      assert (n <= max_length) ;
      let rec go i children =
        if i >= max_length
        then Lwt_result.return (Overflow children)
        else begin
          let* last = create () in
          let* () = set_nb_children t (i + 1) in
          let* () = set_child t i last in
          let* res = do_push_back last children in
          match res with
          | Ok_push -> Lwt_result.return Ok_push
          | Overflow children -> go (i + 1) children
        end
      in
      let i = n - 1 in
      let* last = get_child t i in
      let* res = do_push_back last children in
      match res with
      | Ok_push -> Lwt_result.return Ok_push
      | Overflow children -> go (i + 1) children
    end

  let rec push_back_list t children =
    let* res = do_push_back t children in
    match res with
    | Ok_push -> Lwt_result.return t
    | Overflow children ->
      let* root = create () in
      let* t_height = height t in
      let* () = set_height root (t_height + 1) in
      let* () = set_nb_children root 1 in
      let* () = set_child root 0 t in
      push_back_list root children

  let size ptr =
    let rec size queue =
      let* height = height queue in
      if height = 0
      then nb_free_sectors queue
      else
        let* nb_children = nb_children queue in
        let rec go i acc =
          if i > nb_children - 1
          then Lwt_result.return acc
          else
            let* queue = get_child queue i in
            let* s = size queue in
            go (i + 1) (acc + s)
        in
        go 0 0
    in
    size ptr

  let rec push_discarded ~quantity t =
    match B.acquire_discarded () with
    | [] -> Lwt_result.return (t, quantity)
    | lst ->
      let* t = push_back_list t lst in
      push_discarded ~quantity:(quantity + List.length lst) t

  let push_discarded t = push_discarded ~quantity:0 t

  type pop_front =
    | Ok_pop
    | Underflow of int

  let shift_left t nb =
    let off = schema.free_sectors.Schema.location in
    let len = nb * schema.free_sectors.Schema.size_of_thing in
    let* () = Sector.erase_region t ~off ~len in
    let* t_nb_free_sectors = nb_free_sectors t in
    assert (t_nb_free_sectors - nb > 0) ;
    set_nb_free_sectors t (t_nb_free_sectors - nb)

  let shift_left_children t nb =
    let off = schema.children.Schema.location in
    let len = nb * schema.children.Schema.size_of_thing in
    let* () = Sector.erase_region t ~off ~len in
    let* t_nb_children = nb_children t in
    assert (t_nb_children - nb > 0) ;
    set_nb_children t (t_nb_children - nb)

  let rec do_pop_front t nb acc =
    assert (nb > 0) ;
    let* h = height t in
    if h = 0
    then begin
      let* len = nb_free_sectors t in
      let stop = min len nb in
      let rec go i acc =
        if i >= stop
        then Lwt_result.return acc
        else
          let* child_ptr = get_free_sector t i in
          go (i + 1) (child_ptr :: acc)
      in
      let* acc = go 0 acc in
      if len <= nb
      then begin
        let+ () = set_nb_free_sectors t 0 in
        acc, Underflow (nb - len)
      end
      else begin
        let+ () = shift_left t nb in
        acc, Ok_pop
      end
    end
    else begin
      let* len = nb_children t in
      let rec go i nb acc =
        assert (nb >= 0) ;
        if i >= len
        then begin
          let* () = set_height t 0 in
          let+ () = set_nb_children t 0 in
          acc, Underflow nb
        end
        else if nb = 0
        then begin
          assert (i > 0) ;
          let+ () = shift_left_children t i in
          acc, Ok_pop
        end
        else
          let* first = get_child t i in
          let* acc, res = do_pop_front first nb acc in
          match res with
          | Ok_pop ->
            let+ () = if i > 0 then shift_left_children t i else Lwt_result.return () in
            acc, Ok_pop
          | Underflow rest ->
            Sector.free first ;
            go (i + 1) rest acc
      in
      go 0 nb acc
    end

  let pop_front t nb =
    let* acc, res = do_pop_front t nb [] in
    let* t, nb_discarded = push_discarded t in
    match res with
    | Ok_pop -> Lwt_result.return (t, acc, Int64.of_int (nb - nb_discarded))
    | Underflow _ -> Lwt_result.fail `Disk_is_full

  type q =
    { free_start : Sector.id
    ; free_queue : t
    ; free_sectors : Int64.t
    }

  let push_back { free_start; free_queue; free_sectors } lst =
    let* free_queue = push_back_list free_queue lst in
    let+ free_queue, nb = push_discarded free_queue in
    { free_start
    ; free_queue
    ; free_sectors = Int64.add free_sectors (Int64.of_int (nb + List.length lst))
    }

  let push_discarded { free_start; free_queue; free_sectors } =
    let+ free_queue, nb = push_discarded free_queue in
    { free_start; free_queue; free_sectors = Int64.add free_sectors (Int64.of_int nb) }

  let pop_front { free_start; free_queue; free_sectors } quantity =
    let easy_alloc =
      min quantity Int64.(to_int (sub B.nb_sectors (B.Id.to_int64 free_start)))
    in
    assert (easy_alloc >= 0) ;
    let rest_alloc = quantity - easy_alloc in
    let head = List.init easy_alloc (fun i -> B.Id.add free_start i) in
    let+ free_queue, tail, quantity =
      if rest_alloc <= 0
      then Lwt_result.return (free_queue, [], 0L)
      else pop_front free_queue rest_alloc
    in
    let quantity = Int64.add quantity (Int64.of_int easy_alloc) in
    let q =
      { free_start = B.Id.add free_start easy_alloc
      ; free_queue
      ; free_sectors = Int64.sub free_sectors quantity
      }
    in
    q, head @ tail

  let count_new { free_queue = q; _ } = Sector.count_new q

  let finalize { free_start = f; free_queue = q; free_sectors } ids =
    let+ ts, rest = Sector.finalize q ids in
    assert (rest = []) ;
    { free_start = f; free_queue = q; free_sectors }, ts

  let allocate ~free_queue sector =
    let* count = Sector.count_new sector in
    if count = 0
    then Lwt_result.return (free_queue, [])
    else
      let* free_queue, allocated = pop_front free_queue count in
      let+ to_flush, ids = Sector.finalize sector allocated in
      assert (ids = []) ;
      free_queue, to_flush

  let self_allocate ~free_queue =
    let rec alloc_queue allocated count free_queue =
      assert (count > 0) ;
      let* free_queue, new_allocated = pop_front free_queue count in
      assert (List.length new_allocated = count) ;
      let allocated = List.rev_append new_allocated allocated in
      assert (B.acquire_discarded () = []) ;
      let* new_count = count_new free_queue in
      let allocated_count = List.length allocated in
      if allocated_count = new_count
      then finalize free_queue allocated
      else if allocated_count < new_count
      then alloc_queue allocated (new_count - allocated_count) free_queue
      else begin
        let rec give_back ~free_queue allocated_count = function
          | [] -> assert false
          | id :: allocated ->
            let* free_queue = push_back free_queue [ id ] in
            let allocated_count = allocated_count - 1 in
            let* new_count = count_new free_queue in
            if allocated_count = new_count
            then finalize free_queue allocated
            else if allocated_count > new_count
            then give_back ~free_queue allocated_count allocated
            else alloc_queue allocated allocated_count free_queue
        in
        give_back ~free_queue allocated_count allocated
      end
    in
    assert (B.acquire_discarded () = []) ;
    let* count = count_new free_queue in
    if count > 0
    then alloc_queue [] count free_queue
    else Lwt_result.return (free_queue, [])

  let load (free_start, ptr, free_sectors) =
    let+ free_queue = if Sector.is_null_ptr ptr then create () else Sector.load ptr in
    { free_start; free_queue; free_sectors }

  let verify_checksum { free_queue = ptr; _ } =
    let rec verify_queue queue =
      let* () = Sector.verify_checksum queue in
      let* height = height queue in
      if height > 0
      then
        let* nb_children = nb_children queue in
        let rec check_child i =
          if i > nb_children - 1
          then Lwt_result.return ()
          else
            let* queue = get_child queue i in
            let* () = verify_queue queue in
            check_child (i + 1)
        in
        check_child 0
      else Lwt_result.return ()
    in
    verify_queue ptr

  let size { free_queue; _ } = size free_queue

  let rec reachable_size queue =
    let* height = height queue in
    if height = 0
    then Lwt_result.return 1
    else
      let* nb_children = nb_children queue in
      let rec go i acc =
        if i > nb_children - 1
        then Lwt_result.return acc
        else
          let* queue = get_child queue i in
          let* s = reachable_size queue in
          go (i + 1) (acc + s)
      in
      go 0 1

  let reachable_size { free_queue; _ } = reachable_size free_queue
end
