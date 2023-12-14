module Leaf (B : Context.A_DISK) : sig
  module Schema : module type of Schema.Make (B)
  module Sector = Schema.Sector
  module Queue : module type of Queue.Make (B)

  type t = Sector.t
  type 'a io := ('a, B.error) Lwt_result.t
  type q := Sector.id * Sector.ptr * Sector.ptr * int64

  val get_free_queue : t -> q io
  val get_payload : t -> Sector.ptr io
  val get_format_uid : t -> int64 io
  val generation : t -> int64 io

  val create
    :  format_uid:int64
    -> gen:int64
    -> at:B.Id.t
    -> q
    -> Sector.ptr
    -> Sector.t io
end = struct
  module Schema = Schema.Make (B)
  module Sector = Schema.Sector
  module Queue = Queue.Make (B)
  open Lwt_result.Syntax

  type t = Sector.t

  type schema =
    { format_uid : int64 Schema.field
    ; generation : int64 Schema.field
    ; free_start : Sector.id Schema.field
    ; free_queue : Schema.ptr
    ; bitset : Schema.ptr
    ; free_sectors : int64 Schema.field
    ; payload : Schema.ptr
    }

  let { format_uid; generation; free_start; free_queue; free_sectors; bitset; payload } =
    Schema.define
    @@
    let open Schema.Syntax in
    let+ format_uid = Schema.uint64
    and+ generation = Schema.uint64
    and+ free_start = Schema.id
    and+ free_queue = Schema.ptr
    and+ bitset = Schema.ptr
    and+ free_sectors = Schema.uint64
    and+ payload = Schema.ptr in
    { format_uid; generation; free_start; free_queue; free_sectors; bitset; payload }

  include struct
    open Schema.Infix

    let set_format_uid t v = t.@(format_uid) <- v
    let format_uid t = t.@(format_uid)
    let set_generation t v = t.@(generation) <- v
    let generation t = t.@(generation)
    let set_free_start t v = t.@(free_start) <- v
    let get_free_start t = t.@(free_start)
    let set_free_queue t v = t.@(free_queue) <- v
    let free_queue t = t.@(free_queue)
    let set_free_bitset t v = t.@(bitset) <- v
    let free_bitset t = t.@(bitset)
    let set_free_sectors t v = t.@(free_sectors) <- v
    let free_sectors t = t.@(free_sectors)
    let get_payload t = t.@(payload)
    let set_payload t v = t.@(payload) <- v
  end

  let get_free_queue t =
    let* queue = free_queue t in
    let* free_start = get_free_start t in
    let* bitset = free_bitset t in
    let+ free_sectors = free_sectors t in
    free_start, queue, bitset, free_sectors

  let get_format_uid t = format_uid t

  let create ~format_uid ~gen ~at (free_start, free_queue, bitset, free_sectors) payload =
    let* s = Sector.create ~at:(Sector.root_loc at) () in
    let* () = set_format_uid s format_uid in
    let* () = set_generation s gen in
    let* () = set_free_start s free_start in
    let* () = set_free_queue s free_queue in
    let* () = set_free_bitset s bitset in
    let* () = set_free_sectors s free_sectors in
    let+ () = set_payload s payload in
    s
end

module Make (B : Context.A_DISK) = struct
  module Leaf = Leaf (B)
  module Schema = Leaf.Schema
  module Sector = Leaf.Sector
  module Header = Header.Make (B)
  module Queue = Leaf.Queue
  open Lwt_result.Syntax

  let rec regroup (first, last, cs, acc) = function
    | [] -> List.rev ((first, List.rev cs) :: acc)
    | (id, c) :: rest ->
      if B.Id.(equal (succ last) id)
      then regroup (first, id, c :: cs, acc) rest
      else regroup (id, id, [ c ], (first, List.rev cs) :: acc) rest

  let regroup = function
    | [] -> []
    | (id, c) :: rest -> regroup (id, id, [ c ], []) rest

  let regroup lst =
    regroup @@ List.sort (fun (a_id, _) (b_id, _) -> B.Id.compare a_id b_id) lst

  let rec flush = function
    | [] -> Lwt_result.return ()
    | (id, cs) :: css ->
      let id_ = Int64.to_int @@ B.Id.to_int64 id in
      assert (id_ <> 0 && id_ <> 1) ;
      let* () = B.write id cs in
      flush css

  let flush lst =
    let lst = regroup lst in
    flush lst

  type t =
    { nb_roots : int
    ; format_uid : int64
    ; mutable generation : Int64.t
    ; mutable current : Leaf.t
    ; mutable parent_at : int
    ; mutable parent : Sector.t
    ; mutable current_idx : int
    }

  type schema =
    { first_generation : int64 Schema.field
    ; generations : Schema.id Schema.dyn_array
    }

  let { first_generation; generations } =
    Schema.define
    @@
    let open Schema.Syntax in
    let+ first_generation = Schema.uint64
    and+ generations = Schema.array Schema.id in
    { first_generation; generations }

  let rec find_latest_generation ~check = function
    | [] -> Lwt_result.return None
    | g :: gs ->
      Lwt.bind (check g) (function
        | Ok v -> Lwt_result.return (Some v)
        | Error _ -> find_latest_generation ~check gs)

  let nb = 8

  let rec split_at n acc = function
    | rest when n = 0 -> List.rev acc, rest
    | x :: xs -> split_at (n - 1) (x :: acc) xs
    | [] -> List.rev acc, []

  let rec load_gens ~format_uid nb_roots nb s i expected_gen acc =
    let open Schema.Infix in
    if i >= nb
    then Lwt_result.return acc
    else
      let* at = s.@(Schema.nth generations i) in
      let* acc =
        Lwt.map
          (function
           | Ok (g, generation) ->
             let g =
               { nb_roots
               ; generation = g
               ; current = generation
               ; parent_at = Int64.to_int @@ B.Id.to_int64 @@ Sector.force_id s
               ; parent = s
               ; current_idx = i
               ; format_uid
               }
             in
             Ok (g :: acc)
           | Error _ -> Ok acc)
          (let* generation = Sector.load_root at in
           let* g = Leaf.generation generation in
           let* () =
             if expected_gen <> g
             then Lwt_result.fail `Disk_not_formatted
             else Lwt_result.return ()
           in
           let* fuid = Leaf.get_format_uid generation in
           let+ () =
             if fuid <> format_uid
             then Lwt_result.fail `Disk_not_formatted
             else Lwt_result.return ()
           in
           g, generation)
      in
      load_gens ~format_uid nb_roots nb s (i + 1) (Int64.succ expected_gen) acc

  let rec load_roots nb_roots i acc =
    if i >= nb_roots
    then Lwt_result.return acc
    else
      let* s = Sector.load_root (B.Id.of_int (i + B.header_size)) in
      let open Schema.Infix in
      let* first_gen = s.@(first_generation) in
      load_roots nb_roots (i + 1) ((first_gen, s) :: acc)

  let load ~check () =
    let* header = Header.load () in
    let* nb_roots = Header.get_roots header in
    let* format_uid = Header.get_format_uid header in
    let* roots = load_roots nb_roots 0 [] in
    let roots = List.sort (fun (a, _) (b, _) -> Int64.compare b a) roots in
    let rec find_latest = function
      | [] -> failwith "Root.load: no valid generation"
      | (first_gen, parent) :: rest ->
        let open Schema.Infix in
        let* nb = parent.@(generations.length) in
        let* generations = load_gens ~format_uid nb_roots nb parent 0 first_gen [] in
        let* found = find_latest_generation ~check generations in
        begin
          match found with
          | None -> find_latest rest
          | Some r -> Lwt_result.return r
        end
    in
    find_latest roots

  let create_header ~disk_size ~page_size =
    let+ h = Header.create ~disk_size ~page_size in
    h

  let rec create_roots nb_roots i acc =
    if i >= nb_roots
    then Lwt_result.return acc
    else
      let* s =
        Sector.create ~at:(Sector.root_loc @@ B.Id.of_int (i + B.header_size)) ()
      in
      let open Schema.Infix in
      let* () = s.@(first_generation) <- Int64.zero in
      let* () = s.@(generations.length) <- 0 in
      create_roots nb_roots (i + 1) (s :: acc)

  let format () =
    let* header = create_header ~page_size:B.page_size ~disk_size:B.nb_sectors in
    let* nb_roots = Header.get_roots header in
    let* format_uid = Header.get_format_uid header in
    let used = nb_roots + nb + B.header_size in
    let free_start = B.Id.of_int used in
    let free_sectors = Int64.sub B.nb_sectors (Int64.of_int used) in
    let* roots = create_roots nb_roots 0 [] in
    let s0 = List.hd roots in
    let open Schema.Infix in
    let* () = s0.@(first_generation) <- Int64.one in
    let* () = s0.@(generations.length) <- nb in
    let rec go i =
      if i >= nb
      then Lwt_result.return ()
      else
        let* () =
          s0.@(Schema.nth generations i) <- B.Id.of_int (i + nb_roots + B.header_size)
        in
        go (i + 1)
    in
    let* () = go 0 in
    let* at = s0.@(Schema.nth generations 0) in
    let* first =
      Leaf.create
        ~format_uid
        ~gen:Int64.one
        ~at
        (free_start, Sector.null_ptr, Sector.null_ptr, free_sectors)
        Sector.null_ptr
    in
    let rec write_all = function
      | [] -> Lwt_result.return ()
      | r :: rs ->
        let* () = Sector.write_root r in
        write_all rs
    in
    let* () = Sector.write_root header in
    let+ () = write_all (first :: roots) in
    { nb_roots
    ; generation = Int64.one
    ; current = first
    ; parent_at = 0
    ; parent = s0
    ; current_idx = 0
    ; format_uid
    }

  let current_idx t = Int64.rem t.generation (Int64.of_int nb)

  let update t ~queue ~payload =
    t.generation <- Int64.succ t.generation ;
    t.current_idx <- t.current_idx + 1 ;
    let open Schema.Infix in
    let* max_gens = t.parent.@(generations.length) in
    let* queue =
      if t.current_idx < max_gens
      then Lwt_result.return queue
      else begin
        t.parent_at <- (t.parent_at + 1) mod t.nb_roots ;
        let* s1 =
          Sector.create
            ~at:(Sector.root_loc @@ B.Id.of_int (t.parent_at + B.header_size))
            ()
        in
        t.parent <- s1 ;
        let* queue, free_gens = Queue.pop_front queue nb in
        let* () = s1.@(generations.length) <- nb in
        let* () = s1.@(first_generation) <- t.generation in
        let rec go i = function
          | [] ->
            assert (i = nb) ;
            Lwt_result.return ()
          | g :: gens ->
            let* () = s1.@(Schema.nth generations i) <- g in
            go (i + 1) gens
        in
        let* () = go 0 free_gens in
        let+ () = Sector.write_root s1 in
        t.current_idx <- 0 ;
        t.parent <- s1 ;
        queue
      end
    in
    let previous_generation = Sector.force_id t.current in
    let* queue =
      let* queue = Queue.push_back queue [ previous_generation ] in
      let* queue, to_flush_queue = Queue.self_allocate ~free_queue:queue in
      let+ () = flush to_flush_queue in
      queue
    in
    let { Queue.free_start = new_free_start
        ; free_queue = new_free_root
        ; bitset = new_bitset
        ; free_sectors = new_free_sectors
        }
      =
      queue
    in
    let* at = t.parent.@(Schema.nth generations t.current_idx) in
    let* current =
      Leaf.create
        ~format_uid:t.format_uid
        ~gen:t.generation
        ~at
        (new_free_start, Sector.to_ptr new_free_root, Sector.to_ptr new_bitset, new_free_sectors)
        (Sector.to_ptr payload)
    in
    t.current <- current ;
    let+ () = Sector.write_root current in
    queue

  let get_free_queue t = Leaf.get_free_queue t.current
  let get_payload t = Leaf.get_payload t.current
  let reachable_size t = t.nb_roots + nb - t.current_idx
end
