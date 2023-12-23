module Make (B : Context.A_DISK) = struct
  module Sector = Sector.Make (B)
  module Schema = Schema.Make (B)
  module Bitset = Bitset.Make (B)

  type t = Sector.t
  type range = Sector.id * int

  type schema =
    { height : int Schema.field
    ; children : Schema.child Schema.dyn_array (* if height > 0 *)
    ; free_sectors : range Schema.field Schema.dyn_array (* if height = 0 *)
    }

  let ({ height; children; free_sectors } as schema) =
    Schema.define
    @@
    let open Schema.Syntax in
    let* height = Schema.uint8 in
    let| children = Schema.array Schema.child
    and| free_sectors = Schema.(array (field_pair id uint32)) in
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
    | Overflow of range list

  let rec do_push_back t (children : range list) =
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
        | (child_ptr : range) :: children ->
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

  let rec push_back_list t (children : range list) =
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

  let rec push_discarded ~quantity t bitset =
    let rec free = function
      | a :: b ->
        let* () = Bitset.free_range bitset a in
        free b
      | [] -> Lwt_result.return ()
    in
    match B.acquire_discarded () with
    | [] -> Lwt_result.return (t, quantity)
    | lst ->
      let* t = push_back_list t lst in
      let* () = free lst in
      push_discarded ~quantity:(quantity + List.length lst) t bitset

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
      let* list_len = nb_free_sectors t in
      let rec go i nb acc =
        if i = list_len
        then Lwt_result.return (nb, i, acc)
        else
          let* id, len = get_free_sector t i in
          match nb with
          | nb when nb = len -> Lwt_result.return (0, i + 1, (id, len) :: acc)
          | nb when nb > len -> go (i + 1) (nb - len) ((id, len) :: acc)
          | _ ->
            let* () = set_free_sector t i (B.Id.add id nb, len - nb) in
            Lwt_result.return (0, i, (id, nb) :: acc)
      in
      let* nb_rest, i, acc = go 0 nb acc in
      if nb_rest > 0
      then begin
        let+ () = set_nb_free_sectors t 0 in
        acc, Underflow nb_rest
      end
      else begin
        let+ () = shift_left t i in
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

  let pop_front t bitset nb =
    let* acc, res = do_pop_front t nb [] in
    let* t, nb_discarded = push_discarded t bitset in
    match res with
    | Ok_pop -> Lwt_result.return (t, acc, Int64.of_int (nb - nb_discarded))
    | Underflow _ -> Lwt_result.fail `Disk_is_full

  type q =
    { free_start : Sector.id
    ; free_queue : t
    ; bitset : Bitset.t
    ; free_sectors : Int64.t
    }

  let push_back { free_start; free_queue; bitset; free_sectors } lst =
    let rec free = function
      | a :: b ->
        let* () = Bitset.free_range bitset a in
        free b
      | [] -> Lwt_result.return ()
    in
    let* () = free lst in
    let* free_queue = push_back_list free_queue lst in
    let+ free_queue, nb = push_discarded free_queue bitset in
    { free_start
    ; free_queue
    ; bitset
    ; free_sectors = Int64.add free_sectors (Int64.of_int (nb + List.length lst))
    }

  let push_discarded { free_start; free_queue; bitset; free_sectors } =
    let+ free_queue, nb = push_discarded free_queue bitset in
    { free_start
    ; free_queue
    ; bitset
    ; free_sectors = Int64.add free_sectors (Int64.of_int nb)
    }

  let pop_front { free_start; free_queue; bitset; free_sectors } quantity =
    let easy_alloc =
      min quantity Int64.(to_int (sub B.nb_sectors (B.Id.to_int64 free_start)))
    in
    assert (easy_alloc >= 0) ;
    let rest_alloc = quantity - easy_alloc in
    let head = [ free_start, easy_alloc ] in
    let+ free_queue, tail, quantity =
      if rest_alloc <= 0
      then Lwt_result.return (free_queue, [], 0L)
      else pop_front free_queue bitset rest_alloc
    in
    let quantity = Int64.add quantity (Int64.of_int easy_alloc) in
    let q =
      { free_start = B.Id.add free_start easy_alloc
      ; free_queue
      ; bitset
      ; free_sectors = Int64.sub free_sectors quantity
      }
    in
    q, head @ tail

  let pop_front q quantity =
    let* q, lst = pop_front q quantity in
    let rec use = function
      | a :: b ->
        let* () = Bitset.use_range q.bitset a in
        use b
      | [] -> Lwt_result.return ()
    in
    let* () = use lst in
    let+ q = push_discarded q in 
    q, lst

  let count_new { free_queue = q; bitset = b; _ } =
    let* bitset_size = Sector.count_new b in
    let+ queue_size = Sector.count_new q in
    bitset_size + queue_size

  let finalize { free_start = f; free_queue = q; bitset; free_sectors } ids =
    let* tsqueue, rest = Sector.finalize q ids in
    let+ tsbitset, rest = Sector.finalize bitset rest in
    (* List.iter (fun (id, _) -> Format.printf "Bitset at %d@." (Int64.to_int @@ B.Id.to_int64 id)) tsbitset; *)
    assert (rest = []) ;
    { free_start = f; free_queue = q; bitset; free_sectors }, tsqueue @ tsbitset

  let allocate ~free_queue sector =
    let* count = Sector.count_new sector in
    if count = 0
    then Lwt_result.return (free_queue, [])
    else
      let* free_queue, allocated = pop_front free_queue count in
      let+ to_flush, ids = Sector.finalize sector (B.Diet.list_of_ranges allocated) in
      assert (ids = []) ;
      free_queue, to_flush

  let self_allocate ~free_queue =
    let rec alloc_queue allocated count free_queue =
      assert (count > 0) ;
      let* free_queue, new_allocated = pop_front free_queue count in
      let new_allocated = B.Diet.list_of_ranges new_allocated in
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
          | (id : Sector.id) :: (allocated : Sector.id list) ->
            let* free_queue = push_back free_queue [ id, 1 ] in
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

  let load (free_start, queue_ptr, bitset_ptr, free_sectors) =
    let* free_queue =
      if Sector.is_null_ptr queue_ptr then create () else Sector.load queue_ptr
    in
    let+ bitset =
      if Sector.is_null_ptr bitset_ptr then Bitset.create () else Sector.load bitset_ptr
    in
    { free_start; free_queue; bitset; free_sectors }

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
